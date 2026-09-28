-- =====================================================================
-- Слой staging: приведение типов и очистка.
-- Перестраивается целиком при каждом запуске, в одной транзакции.
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS staging;

-- ---------- Заказы ----------
DROP TABLE IF EXISTS staging.superstore;
CREATE TABLE staging.superstore AS
SELECT
    row_id::INTEGER                             AS row_id,
    TRIM(order_id)                              AS order_id,
    TO_DATE(order_date, 'MM/DD/YYYY')           AS order_date,   -- источник: М/Д/ГГГГ
    TO_DATE(ship_date,  'MM/DD/YYYY')           AS ship_date,
    TRIM(ship_mode)                             AS ship_mode,
    TRIM(customer_id)                           AS customer_id,
    TRIM(customer_name)                         AS customer_name,
    TRIM(segment)                               AS segment,
    TRIM(country)                               AS country,
    TRIM(city)                                  AS city,
    TRIM(state)                                 AS state,
    TRIM(postal_code)                           AS postal_code,  -- код, а не число: остаётся TEXT
    TRIM(region)                                AS region,
    TRIM(product_id)                            AS product_id,
    TRIM(category)                              AS category,
    TRIM(sub_category)                          AS sub_category,
    TRIM(REPLACE(product_name, chr(160), ' '))  AS product_name, -- неразрывный пробел -> обычный
    sales::NUMERIC                              AS sales,
    quantity::INTEGER                           AS quantity,
    discount::NUMERIC                           AS discount,
    profit::NUMERIC                             AS profit,
    source_file,
    loaded_at
FROM raw.superstore;

ALTER TABLE staging.superstore ADD PRIMARY KEY (row_id);        -- упадёт, если row_id повторится

-- ---------- Региональные менеджеры ----------
DROP TABLE IF EXISTS staging.people;
CREATE TABLE staging.people AS
SELECT
    TRIM(region)  AS region,
    TRIM(person)  AS regional_manager
FROM raw.people;

ALTER TABLE staging.people ADD PRIMARY KEY (region);            -- один менеджер на регион

-- ---------- Возвраты ----------
DROP TABLE IF EXISTS staging.returns;
CREATE TABLE staging.returns AS
SELECT DISTINCT TRIM(order_id) AS order_id
FROM raw.returns
WHERE TRIM(returned) = 'Yes';

ALTER TABLE staging.returns ADD PRIMARY KEY (order_id);

-- ---------- Итог для лога ----------
SELECT
    (SELECT COUNT(*) FROM staging.superstore) AS superstore_rows,
    (SELECT COUNT(*) FROM staging.people)     AS people_rows,
    (SELECT COUNT(*) FROM staging.returns)    AS returns_rows;
