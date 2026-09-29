-- =====================================================================
-- Проверки качества данных в слое raw.
-- Файл готовит схему dq, затем последним SELECT возвращает результаты.
-- Каждая проверка возвращает failed_count: сколько записей нарушают правило.
-- 0 = проверка пройдена.
-- ERROR  — pipeline останавливается, staging и mart не перестраиваются.
-- WARNING — pipeline продолжается, проблема фиксируется в dq.check_results.
-- =====================================================================

-- ---------- Подготовка: схема, таблица результатов, безопасный разбор даты ----------
CREATE SCHEMA IF NOT EXISTS dq;

CREATE TABLE IF NOT EXISTS dq.check_results (
    run_id        TEXT,
    checked_at    TIMESTAMP,
    check_no      INTEGER,
    check_name    TEXT,
    severity      TEXT,
    failed_count  BIGINT,
    status        TEXT          -- PASS / WARN / FAIL
);

-- TO_DATE падает на невозможных датах (2/30/2016, 13/45/2016).
-- Эта функция вместо ошибки возвращает NULL, чтобы проверка могла их посчитать.
CREATE OR REPLACE FUNCTION dq.try_mdy_date(value TEXT)
RETURNS DATE
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF value IS NULL OR value !~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN
        RETURN NULL;
    END IF;
    RETURN TO_DATE(value, 'MM/DD/YYYY');
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END $$;

-- ---------- Заказы: raw.superstore ----------
SELECT 1 AS check_no, 'orders_not_empty' AS check_name, 'ERROR' AS severity,
       CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS failed_count
FROM raw.superstore

UNION ALL
-- пустые и повторяющиеся row_id
SELECT 2, 'row_id_not_null_unique', 'ERROR',
       COUNT(*) - COUNT(DISTINCT NULLIF(TRIM(row_id), ''))
FROM raw.superstore

UNION ALL
-- формат М/Д/ГГГГ и реальная дата (без 30 февраля и 13-го месяца)
SELECT 3, 'dates_valid_mdy', 'ERROR',
       COUNT(*) FILTER (
           WHERE dq.try_mdy_date(order_date) IS NULL
              OR dq.try_mdy_date(ship_date)  IS NULL)
FROM raw.superstore

UNION ALL
SELECT 4, 'ship_not_before_order', 'ERROR',
       COUNT(*) FILTER (WHERE dq.try_mdy_date(ship_date) < dq.try_mdy_date(order_date))
FROM raw.superstore

UNION ALL
SELECT 5, 'numbers_format', 'ERROR',
       COUNT(*) FILTER (
           WHERE COALESCE(sales,    '') !~ '^-?\d+(\.\d+)?$'
              OR COALESCE(quantity, '') !~ '^\d+$'
              OR COALESCE(discount, '') !~ '^\d+(\.\d+)?$'
              OR COALESCE(profit,   '') !~ '^-?\d+(\.\d+)?$')
FROM raw.superstore

UNION ALL
SELECT 6, 'quantity_positive', 'ERROR',
       COUNT(*) FILTER (WHERE CASE WHEN quantity ~ '^\d+$' THEN quantity::INTEGER <= 0 END)
FROM raw.superstore

UNION ALL
SELECT 7, 'discount_between_0_and_1', 'ERROR',
       COUNT(*) FILTER (WHERE CASE WHEN discount ~ '^\d+(\.\d+)?$' THEN discount::NUMERIC > 1 END)
FROM raw.superstore

UNION ALL
SELECT 8, 'sales_positive', 'WARNING',
       COUNT(*) FILTER (WHERE CASE WHEN sales ~ '^-?\d+(\.\d+)?$' THEN sales::NUMERIC <= 0 END)
FROM raw.superstore

UNION ALL
-- у одного customer_id должно быть одно имя и один сегмент (нужно для dim_customer)
SELECT 9, 'customer_single_name_segment', 'ERROR', COUNT(*)
FROM (
    SELECT TRIM(customer_id)
    FROM raw.superstore
    GROUP BY 1
    HAVING COUNT(DISTINCT TRIM(customer_name)) > 1 OR COUNT(DISTINCT TRIM(segment)) > 1
) x

UNION ALL
-- у пары «товар + название» одна категория и подкатегория (нужно для dim_product)
SELECT 10, 'product_single_subcategory', 'ERROR', COUNT(*)
FROM (
    SELECT TRIM(product_id), TRIM(REPLACE(product_name, chr(160), ' '))
    FROM raw.superstore
    GROUP BY 1, 2
    HAVING COUNT(DISTINCT TRIM(category)) > 1 OR COUNT(DISTINCT TRIM(sub_category)) > 1
) x

UNION ALL
-- известная проблема источника: один product_id у разных товаров (решено product_key)
SELECT 11, 'product_id_multiple_names', 'WARNING', COUNT(*)
FROM (
    SELECT TRIM(product_id)
    FROM raw.superstore
    GROUP BY 1
    HAVING COUNT(DISTINCT TRIM(REPLACE(product_name, chr(160), ' '))) > 1
) x

UNION ALL
-- строки, где совпадает всё, кроме row_id (известный случай: 3406 / 3407)
SELECT 12, 'full_duplicate_rows', 'WARNING', COALESCE(SUM(cnt - 1), 0)
FROM (
    SELECT COUNT(*) AS cnt
    FROM raw.superstore
    GROUP BY order_id, order_date, ship_date, ship_mode, customer_id, customer_name,
             segment, country, city, state, postal_code, region, product_id,
             category, sub_category, product_name, sales, quantity, discount, profit
    HAVING COUNT(*) > 1
) x

-- ---------- Справочники из Excel ----------
UNION ALL
SELECT 13, 'returns_known_orders', 'ERROR', COUNT(*)
FROM raw.returns r
WHERE NOT EXISTS (
    SELECT 1 FROM raw.superstore s WHERE TRIM(s.order_id) = TRIM(r.order_id)
)

UNION ALL
SELECT 14, 'region_has_manager', 'ERROR', COUNT(DISTINCT TRIM(s.region))
FROM raw.superstore s
WHERE NOT EXISTS (
    SELECT 1 FROM raw.people p WHERE TRIM(p.region) = TRIM(s.region)
)

UNION ALL
SELECT 15, 'region_single_manager', 'ERROR', COUNT(*)
FROM (
    SELECT TRIM(region)
    FROM raw.people
    GROUP BY 1
    HAVING COUNT(*) > 1
) x

ORDER BY check_no;
