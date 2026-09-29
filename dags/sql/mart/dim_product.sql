-- mart.dim_product: товары с суррогатным ключом
-- Один product_id в источнике может означать два разных товара,
-- поэтому товар = пара (product_id, product_name)
CREATE SCHEMA IF NOT EXISTS mart;

DROP TABLE IF EXISTS mart.dim_product CASCADE;
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
    ADD UNIQUE (product_id, product_name);

SELECT COUNT(*) AS dim_product_rows FROM mart.dim_product;