# ETL Pipeline with Apache Airflow

[Русский](README.md) | **English**

A containerized ETL pipeline that ingests employee data from CSV files, orchestrates processing with **Apache Airflow**, loads the results into **PostgreSQL**, and serves them to **Power BI** for reporting.

```
CSV / Excel  →  Airflow (Python)  →  PostgreSQL  →  Power BI
```

## Tech stack

| Layer          | Tool                                |
|----------------|-------------------------------------|
| Orchestration  | Apache Airflow 2.10 (LocalExecutor) |
| Processing     | Python 3                            |
| Storage        | PostgreSQL 16                       |
| Infrastructure | Docker, Docker Compose              |
| Reporting      | Power BI                            |

## Pipelines

### `copy_file_dag` — file ingestion and archiving

```
check_file  →  copy_to_archive  →  verify_copy
```

| Task              | What it does                                                        |
|-------------------|---------------------------------------------------------------------|
| `check_file`      | Checks that the source file exists in `input/`; fails fast if not.  |
| `copy_to_archive` | Copies the file to `archive/`, keeping its metadata.                |
| `verify_copy`     | Compares source and archived file sizes to confirm the copy is complete. |

### `employees_to_postgres` — load to the warehouse *(in progress)*

```
read_csv  →  transform  →  load_postgres
```

Reads `employees.csv`, cleans and transforms it, then loads it into a dedicated PostgreSQL database, kept separate from Airflow's own metadata database.

## Project structure

```
ETL_Airflow_Project/
├── dags/                  # Airflow DAG definitions
│   └── copy_file_dag.py
├── input/                 # Source files (sample: employees.csv, 1000 rows)
├── archive/               # Archived copies of processed files
├── output/                # Processing results
└── docker-compose.yaml    # Airflow + PostgreSQL services
```

## Sample data

`input/employees.csv` is semicolon-separated and has 1000 rows:

```
id;name;department;salary
1;Azamat;IT;500000
2;Marat;Sales;400000
```

Departments: IT, Sales, Finance, HR, Marketing, Logistics.

## Getting started

**Prerequisites:** Docker Desktop.

```bash
# 1. Clone the repository
git clone https://github.com/aznrz/ETL_Airflow_Project.git
cd ETL_Airflow_Project

# 2. Start Airflow and PostgreSQL
docker compose up -d

# 3. Get the generated admin password
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

Open http://localhost:8080 and log in as `admin` with that password. Unpause `copy_file_dag` and trigger it. The archived file then appears in `archive/`.

| Action              | Command                          |
|---------------------|----------------------------------|
| Stop services       | `docker compose down`            |
| Container status    | `docker compose ps`              |
| Follow Airflow logs | `docker compose logs -f airflow` |

> Credentials in `docker-compose.yaml` are local development defaults. Do not reuse them in production.

<!-- Screenshots: add images to docs/ and uncomment
## Screenshots

![DAG graph](docs/dag_graph.png)
![Task logs](docs/task_logs.png)
-->

## Roadmap

- [x] Dockerized Airflow + PostgreSQL environment
- [x] File ingestion and archiving DAG with validation
- [ ] Error handling and retries
- [ ] Daily schedule
- [ ] CSV → PostgreSQL ETL DAG
- [ ] Power BI report on top of PostgreSQL
- [ ] Export to a network file share
