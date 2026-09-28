from datetime import datetime, timezone
import csv
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ---------- Настройки ----------
SOURCE_FILE = "/opt/airflow/input/superstore.csv"        # исходный файл (cp1252)
STAGE_FILE = "/opt/airflow/output/superstore_raw.csv"    # промежуточный файл (UTF-8)
SOURCE_ENCODING = "cp1252"
POSTGRES_CONN_ID = "etl_postgres"                        # Connection из Admin → Connections

# Ожидаемый заголовок файла: если источник поменяется, DAG упадёт на read_csv
EXPECTED_COLUMNS = [
    "Row ID", "Order ID", "Order Date", "Ship Date", "Ship Mode",
    "Customer ID", "Customer Name", "Segment", "Country", "City",
    "State", "Postal Code", "Region", "Product ID", "Category",
    "Sub-Category", "Product Name", "Sales", "Quantity", "Discount", "Profit",
]


def to_snake(name):
    # "Order Date" -> "order_date", "Sub-Category" -> "sub_category"
    return name.strip().lower().replace(" ", "_").replace("-", "_")


RAW_COLUMNS = [to_snake(c) for c in EXPECTED_COLUMNS]


def read_csv():
    # Задача 1: проверить файл и заголовок, посчитать строки
    if not os.path.exists(SOURCE_FILE):
        raise FileNotFoundError(f"File not found: {SOURCE_FILE}")

    with open(SOURCE_FILE, encoding=SOURCE_ENCODING, newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
        row_count = sum(1 for _ in reader)

    if header != EXPECTED_COLUMNS:
        raise ValueError(f"Unexpected header: {header}")

    print(f"Header OK, {len(header)} columns, {row_count} rows")
    return row_count  # уходит в XCom


def transform(ti):
    # Задача 2: cp1252 -> UTF-8, snake_case заголовки, технические колонки
    expected_rows = ti.xcom_pull(task_ids="read_csv")
    source_name = os.path.basename(SOURCE_FILE)
    loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    rows = 0
    with open(SOURCE_FILE, encoding=SOURCE_ENCODING, newline="") as src, \
         open(STAGE_FILE, "w", encoding="utf-8", newline="") as dst:
        reader = csv.reader(src)
        writer = csv.writer(dst)

        next(reader)  # пропускаем исходный заголовок
        writer.writerow(RAW_COLUMNS + ["source_file", "loaded_at"])

        for line_no, row in enumerate(reader, start=2):
            if len(row) != len(EXPECTED_COLUMNS):
                raise ValueError(f"Line {line_no}: expected {len(EXPECTED_COLUMNS)} fields, got {len(row)}")
            writer.writerow(row + [source_name, loaded_at])
            rows += 1

    if rows != expected_rows:
        raise ValueError(f"Row count mismatch: read_csv={expected_rows}, transform={rows}")

    print(f"Written {rows} rows to {STAGE_FILE}, loaded_at={loaded_at} UTC")
    return {"source_file": source_name, "rows": rows}  # уходит в XCom


def load_postgres(ti):
    # Задача 3: заменить строки этого файла в raw.superstore
    meta = ti.xcom_pull(task_ids="transform")
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    columns_ddl = ",\n            ".join(f"{c} TEXT" for c in RAW_COLUMNS)
    create_sql = f"""
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE TABLE IF NOT EXISTS raw.superstore (
            {columns_ddl},
            source_file TEXT,
            loaded_at TIMESTAMP
        );
    """
    all_columns = ", ".join(RAW_COLUMNS + ["source_file", "loaded_at"])
    copy_sql = f"COPY raw.superstore ({all_columns}) FROM STDIN WITH (FORMAT csv, HEADER true)"

    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(create_sql)

            cur.execute("DELETE FROM raw.superstore WHERE source_file = %s", (meta["source_file"],))
            print(f"Deleted old rows for {meta['source_file']}: {cur.rowcount}")

            with open(STAGE_FILE, encoding="utf-8") as f:
                cur.copy_expert(copy_sql, f)

            cur.execute("SELECT COUNT(*) FROM raw.superstore WHERE source_file = %s", (meta["source_file"],))
            loaded = cur.fetchone()[0]

        if loaded != meta["rows"]:
            raise ValueError(f"Loaded {loaded} rows, expected {meta['rows']}")

        conn.commit()  # всё или ничего: DELETE и COPY фиксируются вместе
        print(f"Loaded {loaded} rows into raw.superstore")
    except Exception:
        conn.rollback()  # при ошибке старые данные остаются нетронутыми
        raise
    finally:
        conn.close()


with DAG(
    dag_id="superstore_raw_dag",
    start_date=datetime(2026, 9, 1),
    schedule=None,      # пока только ручной запуск
    catchup=False,
    tags=["learning", "superstore"],
) as dag:

    t1 = PythonOperator(task_id="read_csv", python_callable=read_csv)
    t2 = PythonOperator(task_id="transform", python_callable=transform)
    t3 = PythonOperator(task_id="load_postgres", python_callable=load_postgres)

    t1 >> t2 >> t3
