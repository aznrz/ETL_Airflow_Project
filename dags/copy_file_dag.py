from datetime import datetime
import os
import shutil

from airflow import DAG
from airflow.operators.python import PythonOperator

# Пути ВНУТРИ контейнера (это твои папки input и archive)
INPUT_FILE = "/opt/airflow/input/employees.csv"
ARCHIVE_FILE = "/opt/airflow/archive/employees.csv"


def check_file():
    # Задача 1: проверяем, что файл существует
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"File not found: {INPUT_FILE}")
    print(f"File found: {INPUT_FILE}")


def copy_to_archive():
    # Задача 2: копируем файл в archive
    shutil.copy2(INPUT_FILE, ARCHIVE_FILE)
    print(f"Copied to: {ARCHIVE_FILE}")


def verify_copy():
    # Задача 3: сравниваем размер оригинала и копии
    src_size = os.path.getsize(INPUT_FILE)
    dst_size = os.path.getsize(ARCHIVE_FILE)
    if src_size != dst_size:
        raise ValueError(f"Size mismatch: {src_size} vs {dst_size}")
    print(f"Copy OK, size = {dst_size} bytes")


with DAG(
    dag_id="copy_file_dag",
    start_date=datetime(2026, 9, 1),
    schedule="0 3 * * *",   # каждый день в 03:00 UTC = 08:00 Алматы
    catchup=False,
    tags=["learning"],
) as dag:

    t1 = PythonOperator(task_id="check_file", python_callable=check_file)
    t2 = PythonOperator(task_id="copy_to_archive", python_callable=copy_to_archive)
    t3 = PythonOperator(task_id="verify_copy", python_callable=verify_copy)

    t1 >> t2 >> t3
