-- mart.dim_date: календарь на полные годы
-- Покрывает и даты заказов, и даты отгрузки
CREATE SCHEMA IF NOT EXISTS mart;

DROP TABLE IF EXISTS mart.dim_date CASCADE;
CREATE TABLE mart.dim_date AS
SELECT
    d::DATE                                 AS calendar_date,
    EXTRACT(YEAR    FROM d)::INTEGER        AS year,
    EXTRACT(QUARTER FROM d)::INTEGER        AS quarter,
    EXTRACT(MONTH   FROM d)::INTEGER        AS month,
    TO_CHAR(d, 'FMMonth')                   AS month_name,
    TO_CHAR(d, 'YYYY-MM')                   AS year_month,
    EXTRACT(ISODOW  FROM d)::INTEGER        AS day_of_week,
    EXTRACT(ISODOW  FROM d) IN (6, 7)       AS is_weekend
FROM generate_series(
    (SELECT DATE_TRUNC('year', MIN(order_date)) FROM staging.superstore),
    (SELECT DATE_TRUNC('year', MAX(ship_date)) + INTERVAL '1 year - 1 day' FROM staging.superstore),
    INTERVAL '1 day'
) AS d;

ALTER TABLE mart.dim_date ADD PRIMARY KEY (calendar_date);

SELECT COUNT(*) AS dim_date_rows FROM mart.dim_date;