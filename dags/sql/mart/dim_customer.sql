-- mart.dim_customer: справочник клиентов, ключ customer_id
CREATE SCHEMA IF NOT EXISTS mart;

DROP TABLE IF EXISTS mart.dim_customer CASCADE;
CREATE TABLE mart.dim_customer AS
SELECT DISTINCT
    customer_id,
    customer_name,
    segment
FROM staging.superstore;

ALTER TABLE mart.dim_customer ADD PRIMARY KEY (customer_id);

SELECT COUNT(*) AS dim_customer_rows FROM mart.dim_customer;