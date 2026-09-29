-- mart.dim_location: география + региональный менеджер
-- Нужны две staging-таблицы: superstore (адреса) и people (менеджеры)
CREATE SCHEMA IF NOT EXISTS mart;

DROP TABLE IF EXISTS mart.dim_location CASCADE;
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

SELECT
    COUNT(*)                                          AS dim_location_rows,
    COUNT(*) FILTER (WHERE regional_manager IS NULL)  AS without_manager
FROM mart.dim_location;