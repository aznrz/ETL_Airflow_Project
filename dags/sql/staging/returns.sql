-- staging.returns: возвращённые заказы, по одному разу
CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.returns;
CREATE TABLE staging.returns AS
SELECT DISTINCT TRIM(order_id) AS order_id
FROM raw.returns
WHERE TRIM(returned) = 'Yes';

ALTER TABLE staging.returns ADD PRIMARY KEY (order_id);

SELECT COUNT(*) AS returns_rows FROM staging.returns;