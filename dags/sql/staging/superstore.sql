-- staging.superstore: заказы с правильными типами и очисткой
CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.superstore;
CREATE TABLE staging.superstore AS
SELECT
    row_id::INTEGER                             AS row_id,
    TRIM(order_id)                              AS order_id,
    TO_DATE(order_date, 'MM/DD/YYYY')           AS order_date,
    TO_DATE(ship_date,  'MM/DD/YYYY')           AS ship_date,
    TRIM(ship_mode)                             AS ship_mode,
    TRIM(customer_id)                           AS customer_id,
    TRIM(customer_name)                         AS customer_name,
    TRIM(segment)                               AS segment,
    TRIM(country)                               AS country,
    TRIM(city)                                  AS city,
    TRIM(state)                                 AS state,
    TRIM(postal_code)                           AS postal_code,
    TRIM(region)                                AS region,
    TRIM(product_id)                            AS product_id,
    TRIM(category)                              AS category,
    TRIM(sub_category)                          AS sub_category,
    TRIM(REPLACE(product_name, chr(160), ' '))  AS product_name,
    sales::NUMERIC                              AS sales,
    quantity::INTEGER                           AS quantity,
    discount::NUMERIC                           AS discount,
    profit::NUMERIC                             AS profit,
    source_file,
    loaded_at
FROM raw.superstore;

ALTER TABLE staging.superstore ADD PRIMARY KEY (row_id);

SELECT COUNT(*) AS superstore_rows FROM staging.superstore;