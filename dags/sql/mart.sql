-- =====================================================================
-- Слой mart: звёздная схема для Power BI.
-- Перестраивается целиком при каждом запуске, в одной транзакции.
-- Суррогатные ключи (product_key, location_key) пересчитываются при
-- каждой перестройке — это нормально для полной перестройки, но не
-- подойдёт для инкрементальной загрузки.
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS mart;

-- Сначала факт (он ссылается на справочники), потом справочники
DROP TABLE IF EXISTS mart.fact_sales;
DROP TABLE IF EXISTS mart.dim_customer;
DROP TABLE IF EXISTS mart.dim_product;
DROP TABLE IF EXISTS mart.dim_location;
DROP TABLE IF EXISTS mart.dim_date;

-- ---------- dim_customer: ключ customer_id (конфликтов в данных 0) ----------
CREATE TABLE mart.dim_customer AS
SELECT DISTINCT
    customer_id,
    customer_name,
    segment
FROM staging.superstore;

ALTER TABLE mart.dim_customer ADD PRIMARY KEY (customer_id);    -- упадёт, если у ID два имени

-- ---------- dim_product: суррогатный ключ ----------
-- Один product_id в источнике может означать два разных товара,
-- поэтому товар = пара (product_id, product_name)
CREATE TABLE mart.dim_product AS
SELECT
    ROW_NUMBER() OVER (ORDER BY product_id, product_name)::INTEGER AS product_key,
    product_id,
    product_name,
    category,
    sub_category
FROM (
    SELECT DISTINCT product_id, product_name, category, sub_category
    FROM staging.superstore
) p;

ALTER TABLE mart.dim_product
    ADD PRIMARY KEY (product_key),
    ADD UNIQUE (product_id, product_name);                       -- упадёт, если у пары две подкатегории

-- ---------- dim_location: суррогатный ключ + региональный менеджер ----------
CREATE TABLE mart.dim_location AS
SELECT
    ROW_NUMBER() OVER (ORDER BY g.country, g.region, g.state, g.city, g.postal_code)::INTEGER AS location_key,
    g.country,
    g.region,
    g.state,
    g.city,
    g.postal_code,
    pp.regional_manager
FROM (
    SELECT DISTINCT country, region, state, city, postal_code
    FROM staging.superstore
) g
LEFT JOIN staging.people pp ON pp.region = g.region;

ALTER TABLE mart.dim_location ADD PRIMARY KEY (location_key);

-- ---------- dim_date: календарь на полные годы ----------
-- Покрывает и даты заказов, и даты отгрузки (отгрузки заходят в следующий год)
CREATE TABLE mart.dim_date AS
SELECT
    d::DATE                                 AS calendar_date,
    EXTRACT(YEAR    FROM d)::INTEGER        AS year,
    EXTRACT(QUARTER FROM d)::INTEGER        AS quarter,
    EXTRACT(MONTH   FROM d)::INTEGER        AS month,
    TO_CHAR(d, 'FMMonth')                   AS month_name,
    TO_CHAR(d, 'YYYY-MM')                   AS year_month,
    EXTRACT(ISODOW  FROM d)::INTEGER        AS day_of_week,   -- 1 = понедельник
    EXTRACT(ISODOW  FROM d) IN (6, 7)       AS is_weekend
FROM generate_series(
    (SELECT DATE_TRUNC('year', MIN(order_date)) FROM staging.superstore),
    (SELECT DATE_TRUNC('year', MAX(ship_date)) + INTERVAL '1 year - 1 day' FROM staging.superstore),
    INTERVAL '1 day'
) AS d;

ALTER TABLE mart.dim_date ADD PRIMARY KEY (calendar_date);

-- ---------- fact_sales: одна строка = одна строка заказа (row_id) ----------
CREATE TABLE mart.fact_sales AS
SELECT
    s.row_id,
    s.order_id,
    s.order_date,
    s.ship_date,
    s.ship_mode,
    s.customer_id,
    p.product_key,
    l.location_key,
    s.sales,
    s.quantity,
    s.discount,
    s.profit,
    (r.order_id IS NOT NULL)                AS is_returned
FROM staging.superstore s
LEFT JOIN mart.dim_product p
       ON p.product_id   = s.product_id
      AND p.product_name = s.product_name
LEFT JOIN mart.dim_location l
       ON l.country     IS NOT DISTINCT FROM s.country
      AND l.region      IS NOT DISTINCT FROM s.region
      AND l.state       IS NOT DISTINCT FROM s.state
      AND l.city        IS NOT DISTINCT FROM s.city
      AND l.postal_code IS NOT DISTINCT FROM s.postal_code
LEFT JOIN staging.returns r
       ON r.order_id = s.order_id;

-- Ключи и связи: если какой-то JOIN не нашёл пару, здесь будет ошибка
ALTER TABLE mart.fact_sales
    ADD PRIMARY KEY (row_id),
    ALTER COLUMN product_key  SET NOT NULL,
    ALTER COLUMN location_key SET NOT NULL,
    ADD FOREIGN KEY (customer_id)  REFERENCES mart.dim_customer (customer_id),
    ADD FOREIGN KEY (product_key)  REFERENCES mart.dim_product  (product_key),
    ADD FOREIGN KEY (location_key) REFERENCES mart.dim_location (location_key),
    ADD FOREIGN KEY (order_date)   REFERENCES mart.dim_date     (calendar_date),
    ADD FOREIGN KEY (ship_date)    REFERENCES mart.dim_date     (calendar_date);

-- ---------- Сверка: в факте столько же строк, сколько в staging ----------
DO $$
BEGIN
    IF (SELECT COUNT(*) FROM mart.fact_sales) <> (SELECT COUNT(*) FROM staging.superstore) THEN
        RAISE EXCEPTION 'fact_sales row count does not match staging.superstore';
    END IF;
END $$;

-- ---------- Итог для лога ----------
SELECT
    (SELECT COUNT(*) FROM mart.fact_sales)                        AS fact_sales,
    (SELECT COUNT(*) FROM mart.fact_sales WHERE is_returned)      AS returned_rows,
    (SELECT COUNT(*) FROM mart.dim_customer)                      AS dim_customer,
    (SELECT COUNT(*) FROM mart.dim_product)                       AS dim_product,
    (SELECT COUNT(*) FROM mart.dim_location)                      AS dim_location,
    (SELECT COUNT(*) FROM mart.dim_date)                          AS dim_date;
