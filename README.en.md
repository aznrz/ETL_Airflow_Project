# ETL Airflow Project — data pipeline on Apache Airflow

[Русский](README.md) | **English**

![Python](https://img.shields.io/badge/Python-3.x-3776AB?logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache_Airflow-2.10-017CEE?logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-Reporting-F2C811)
![Status](https://img.shields.io/badge/Status-In_progress-orange)

A containerized ETL pipeline that loads sales data (Superstore) from two sources — CSV and Excel, orchestrates processing with **Apache Airflow**, stores the results in **PostgreSQL** in raw → staging → mart layers, and serves a star schema to **Power BI** for reporting.

```
CSV   ─┐
       ├─→  Airflow (Python + SQL)  →  PostgreSQL: raw → staging → mart  →  Power BI
Excel ─┘
```

---

## 🛠️ Tech stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 2.10 (LocalExecutor) |
| Processing | Python 3 |
| Storage | PostgreSQL 16 |
| Infrastructure | Docker, Docker Compose |
| Reporting | Power BI |

---

## 🔄 Pipeline `superstore_raw_dag`

```
read_csv   → transform       → load_postgres       ─┐                 ┌─→ stg_superstore ─→ dim_customer, dim_product, dim_date ─┐
                                                    ├─→ check_quality ─┼─→ stg_people ─────→ dim_location ────────────────────────┼─→ fact_sales
read_excel → transform_excel → load_excel_postgres ─┘                 └─→ stg_returns ──────────────────────────────────────────┘
```

15 tasks. The two load branches run in parallel; `check_quality` waits for both. Staging is built only after the checks pass: three tables in parallel, then the dimensions and the fact table.

| Task | What it does |
|---|---|
| `read_csv` | Checks that `superstore.csv` exists and its header matches the expected 21 columns; counts rows. |
| `transform` | Re-encodes cp1252 → UTF-8, converts headers to snake_case, adds `source_file` and `loaded_at` technical columns. |
| `load_postgres` | Replaces this file's rows in `raw.superstore`: `DELETE` and `COPY` run in a single transaction. |
| `read_excel` | Checks the `People` and `Returns` sheets in `superstore_extra.xlsx` and their headers; counts non-empty rows. |
| `transform_excel` | Each sheet → its own UTF-8 CSV: numbers without `.0`, empty rows skipped. |
| `load_excel_postgres` | Loads `raw.people` and `raw.returns` in a single transaction. |
| `check_quality` | [`dags/sql/dq_checks.sql`](dags/sql/dq_checks.sql): 15 checks on the raw layer, results go to `dq.check_results`. Any `FAIL` stops the DAG before staging. |
| `stg_superstore`, `stg_people`, `stg_returns` | [`dags/sql/staging/`](dags/sql/staging/): types (`INTEGER`, `DATE`, `NUMERIC`), `TRIM`, `\xa0` replaced with a space, primary keys. One task per table. |
| `dim_customer`, `dim_product`, `dim_date`, `dim_location` | [`dags/sql/mart/`](dags/sql/mart/): star schema dimensions. |
| `fact_sales` | [`dags/sql/mart/fact_sales.sql`](dags/sql/mart/fact_sales.sql): sales fact table, foreign keys, row count reconciliation with staging. |

- **Row count reconciliation** at every step: the DAG fails if the counts don't match.
- **All or nothing**: raw loads run in a transaction and roll back on error, leaving existing data untouched. Staging and mart are rebuilt in full.
- **Idempotent**: re-running doesn't duplicate data — 9994 rows deleted, 9994 loaded, the table still has 9994 rows.
- **Keys as checks**: `PRIMARY KEY`, `UNIQUE`, `NOT NULL` and `FOREIGN KEY` constraints in staging and mart fail the DAG on duplicates or broken relationships.
- **Quality checks before staging**: if bad data lands in raw, staging and mart keep their previous, correct state.
- **No passwords in code**: the connection comes from the Airflow Connection `etl_postgres`.
- Project data lives in a dedicated `etl_data` database, separate from Airflow's own metadata database.

---

## ✅ Data quality checks: `dq` schema

The `check_quality` task runs 15 rules on the raw layer: empty load, duplicate and empty `row_id`s, date format and validity, ship date not before order date, number format, quantity and discount within limits, consistent customers and products, returns referencing existing orders, exactly one manager per region.

| Severity | What happens |
|---|---|
| `ERROR` | Check fails → status `FAIL`, the task fails, staging and mart are not rebuilt. |
| `WARNING` | Status `WARN`, the pipeline continues. Used for known quirks of the source: 32 `product_id`s with different names, 1 fully duplicated row. |

- **`dq.check_results`** — check history: `run_id`, `check_no`, `check_name`, `severity`, `failed_count`, status `PASS` / `WARN` / `FAIL`. Results are committed before the task fails, so the history is kept for failed runs too.
- **`dq.try_mdy_date`** — parses M/D/YYYY dates and returns `NULL` instead of an error for impossible dates (`2/30/2016`): `TO_DATE` would crash the task, and the check could not count such rows.

```bash
docker compose exec postgres psql -U airflow -d etl_data -c "SELECT check_no, check_name, status, failed_count FROM dq.check_results WHERE checked_at = (SELECT MAX(checked_at) FROM dq.check_results) ORDER BY check_no;"
```

---

## ⭐ Data model: `mart` star schema

```
              dim_customer
                   │
dim_product ── fact_sales ── dim_location
                   │
               dim_date  (order_date, ship_date)
```

| Table | Rows | Key | Contents |
|---|---:|---|---|
| `fact_sales` | 9994 | `row_id` | One row = one order line: sales, quantity, discount, profit, `is_returned` |
| `dim_customer` | 793 | `customer_id` | Customer name, segment |
| `dim_product` | 1894 | `product_key` (surrogate) | `product_id`, name, category, sub-category |
| `dim_location` | 632 | `location_key` (surrogate) | Country, region, state, city, postal code, regional manager |
| `dim_date` | 1826 | `calendar_date` | Year, quarter, month, weekday, weekend flag — 2014–2018 |

Modeling decisions:
- **Grain is the order line (`row_id`).** Power BI computes order-level totals itself, and the link to products is kept.
- **Surrogate `product_key`.** 32 `product_id` values in the source refer to different products, so a product is a `product_id` + name pair: 1862 IDs yield 1894 products.
- **Address lives in `dim_location`, not in the customer:** 780 of 793 customers received orders in more than one city. The regional manager (`People` sheet) is here too, since it is tied to the region.
- **Returns are an `is_returned` flag in the fact.** The source marks the whole order as returned: 296 orders = 800 order lines.
- **One `dim_date` for both dates** (order and ship), a calendar covering full years.

Reconciliation: total sales in `raw.superstore` and in `mart.fact_sales` match — **2,297,200.86**.

---

## 📂 Project structure

| Folder / file | Contents |
|---|---|
| [dags/](dags/) | Airflow DAGs |
| [dags/sql/](dags/sql/) | [`dq_checks.sql`](dags/sql/dq_checks.sql) — quality checks, [`staging/`](dags/sql/staging/) and [`mart/`](dags/sql/mart/) — one file per table |
| [input/](input/) | Source files (`superstore.csv`, `superstore_extra.xlsx`) — not stored in git |
| [output/](output/) | Intermediate UTF-8 CSVs before loading into raw |
| [archive/](archive/) | Archive of processed files |
| [docs/](docs/) | Screenshots |
| [Dockerfile](Dockerfile), [requirements.txt](requirements.txt) | Airflow image with extra libraries (`openpyxl`) |
| [docker-compose.yaml](docker-compose.yaml) | Airflow + PostgreSQL services |

---

## 📊 Data sources

### `superstore.csv` — orders

The public Sample Superstore dataset: retail sales for 2014–2017.

| Property | Value |
|---|---|
| Encoding | cp1252 (Windows-1252), not UTF-8 |
| Delimiter | comma `,` |
| Line endings | CRLF (`\r\n`) |
| Rows | 9994 + header |
| Columns | 21 |
| Date format | M/D/YYYY without leading zeros (`6/9/2014` = June 9) |
| Quirks | 427 non-breaking spaces (`\xa0`) in text fields |

### `superstore_extra.xlsx` — reference data

The Excel version of the same Sample Superstore. Two sheets are used; the `Orders` sheet is not loaded, since orders come from the CSV.

| Sheet | Columns | Rows | Used for |
|---|---|---:|---|
| `People` | `Person`, `Region` | 4 | regional managers → `dim_location` |
| `Returns` | `Returned`, `Order ID` | 296 | returned orders → `is_returned` in the fact |

---

## 📥 Getting the data

The datasets are not stored in the repository — download them yourself and put them in `input/`:

| File | Source | Size |
|---|---|---:|
| `superstore.csv` | [Superstore Dataset on Kaggle](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final) (a free account is required) → **Download** → unzip | 2,287,806 bytes |
| `superstore_extra.xlsx` | Excel version of Sample Superstore with `Orders`, `People` and `Returns` sheets | 1,106,911 bytes |

1. Rename the files exactly as in the table: the DAG looks for these names.
2. Check the size. A different size means a wrong or corrupted file.

> ⚠️ **Do not open the files in Excel or a text editor, and do not save them.** An editor can silently change the encoding, line endings or date format, and the DAG will fail the header check or load corrupted data. If a file was opened, download it again.

---

## 🚀 Getting started

**Prerequisites:** [Docker Desktop](https://www.docker.com/products/docker-desktop/) and both data files in the `input/` folder (see [Getting the data](#-getting-the-data)).

```bash
# 1. Clone the repository
git clone https://github.com/aznrz/ETL_Airflow_Project.git
cd ETL_Airflow_Project

# 2. Build the Airflow image and start Airflow and PostgreSQL
docker compose up -d --build

# 3. Create the project database
docker compose exec postgres psql -U airflow -c "CREATE DATABASE etl_data;"

# 4. Create the etl_postgres connection in Airflow
docker compose exec airflow airflow connections add etl_postgres \
  --conn-type postgres --conn-host postgres --conn-port 5432 \
  --conn-schema etl_data --conn-login airflow --conn-password airflow

# 5. Get the generated admin password
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

Open http://localhost:8080 and log in as `admin` with that password. Unpause `superstore_raw_dag` and trigger it. Check the result (should be 9994):

```bash
docker compose exec postgres psql -U airflow -d etl_data -c "SELECT COUNT(*) FROM mart.fact_sales;"
```

| Action | Command |
|---|---|
| Stop services | `docker compose down` |
| Container status | `docker compose ps` |
| Follow Airflow logs | `docker compose logs -f airflow` |

> ⚠️ Credentials in `docker-compose.yaml` and in the commands above are local development defaults. Do not reuse them in production.

---

## 🖼️ Screenshots

**DAG graph** — two load branches, quality checks, then staging and mart; all 15 tasks completed successfully:

![DAG graph](docs/dag_graph.png)

**`check_quality` task log** — 15 checks: 13 `PASS` and 2 `WARN` for known source quirks, no `FAIL`:

![Task log](docs/task_logs.png)

---

## 🎯 Roadmap

- [x] Dockerized Airflow + PostgreSQL environment
- [x] CSV → PostgreSQL ETL DAG (raw layer) with idempotent loading
- [x] Second load branch: Excel → PostgreSQL (managers and returns)
- [x] Data layers: raw → staging → mart (star schema)
- [x] Data quality checks before staging (`dq` schema)
- [ ] Automatic processing of new files, without loading the same file twice
- [ ] Daily schedule
- [ ] Failure alerts in Telegram
- [ ] Power BI report on top of the mart layer

---

*ETL Airflow Project · last updated: 29.09.2026, 17:25 (Almaty, UTC+5)*
