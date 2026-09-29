-- staging.people: региональные менеджеры
CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.people;
CREATE TABLE staging.people AS
SELECT
    TRIM(region)  AS region,
    TRIM(person)  AS regional_manager
FROM raw.people;

ALTER TABLE staging.people ADD PRIMARY KEY (region);

SELECT COUNT(*) AS people_rows FROM staging.people;