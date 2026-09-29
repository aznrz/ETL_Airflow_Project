-- mart.fact_sales: одна строка = одна строка заказа (row_id)
-- Нужны: staging.superstore, staging.returns и все четыре справочника
CREATE SCHEMA IF NOT EXISTS mart;

DROP TABLE IF EXISTS mart.fact_sales;
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
    (r.order_id IS NOT NULL) AS is_returned
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

-- Ключи и связи со справочниками
ALTER TABLE mart.fact_sales
    ADD PRIMARY KEY (row_id),
    ALTER COLUMN product_key  SET NOT NULL,
    ALTER COLUMN location_key SET NOT NULL,
    ADD FOREIGN KEY (customer_id)  REFERENCES mart.dim_customer (customer_id),
    ADD FOREIGN KEY (product_key)  REFERENCES mart.dim_product  (product_key),
    ADD FOREIGN KEY (location_key) REFERENCES mart.dim_location (location_key),
    ADD FOREIGN KEY (order_date)   REFERENCES mart.dim_date     (calendar_date),
    ADD FOREIGN KEY (ship_date)    REFERENCES mart.dim_date     (calendar_date);

-- Сверка с staging
DO $$
BEGIN
    IF (SELECT COUNT(*) FROM mart.fact_sales) <> (SELECT COUNT(*) FROM staging.superstore) THEN
        RAISE EXCEPTION 'fact_sales row count does not match staging.superstore';
    END IF;
END $$;

SELECT
    COUNT(*)                            AS fact_rows,
    COUNT(*) FILTER (WHERE is_returned) AS returned_rows,
    SUM(sales)                          AS total_sales,
    SUM(profit)                         AS total_profit
FROM mart.fact_sales;