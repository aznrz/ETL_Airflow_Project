# ETL Airflow Project — пайплайн данных на Apache Airflow

**Русский** | [English](README.en.md)

![Python](https://img.shields.io/badge/Python-3.x-3776AB?logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache_Airflow-2.10-017CEE?logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-Reporting-F2C811)
![Статус](https://img.shields.io/badge/%D0%A1%D1%82%D0%B0%D1%82%D1%83%D1%81-%D0%92%20%D1%80%D0%B0%D0%B7%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%BA%D0%B5-orange)

ETL-пайплайн в Docker. Он загружает данные о продажах (Superstore) из CSV-файлов, оркестрирует обработку через **Apache Airflow**, складывает результат в **PostgreSQL** по слоям raw → staging → mart и отдаёт витрину в **Power BI** для отчётности.

```
CSV  →  Airflow (Python)  →  PostgreSQL (raw → staging → mart)  →  Power BI
```

---

## 🛠️ Стек

| Слой | Инструмент |
|---|---|
| Оркестрация | Apache Airflow 2.10 (LocalExecutor) |
| Обработка | Python 3 |
| Хранилище | PostgreSQL 16 |
| Инфраструктура | Docker, Docker Compose |
| Отчётность | Power BI |

---

## 🔄 Пайплайны

### 🐘 `superstore_raw_dag` — загрузка `superstore.csv` в слой raw

```
read_csv  →  transform  →  load_postgres
```

| Задача | Что делает |
|---|---|
| `read_csv` | Проверяет, что файл есть и заголовок совпадает с ожидаемыми 21 колонкой, считает строки. |
| `transform` | Перекодирует cp1252 → UTF-8, переводит заголовки в snake_case, добавляет технические колонки `source_file` и `loaded_at`. |
| `load_postgres` | Заменяет строки этого файла в `raw.superstore`: `DELETE` и `COPY` в одной транзакции. |

- **Сверка строк** на каждом шаге: если количество не сходится, DAG падает.
- **Всё или ничего**: при ошибке транзакция откатывается, старые данные остаются нетронутыми.
- **Идемпотентность**: повторный запуск не задваивает данные — удалено 9994, загружено 9994, в таблице по-прежнему 9994 строки.
- **Без паролей в коде**: подключение берётся из Airflow Connection `etl_postgres`.
- Данные проекта лежат в отдельной базе `etl_data`, она не смешивается со служебной базой Airflow.

---

## 📂 Структура проекта

| Папка / файл | Что внутри |
|---|---|
| [dags/](dags/) | Описания DAG для Airflow |
| [input/](input/) | Исходные файлы (`superstore.csv`) |
| [output/](output/) | Промежуточные файлы (перекодированный CSV перед загрузкой) |
| [archive/](archive/) | Архив обработанных файлов |
| [docs/](docs/) | Скриншоты |
| [docker-compose.yaml](docker-compose.yaml) | Сервисы Airflow и PostgreSQL |

---

## 📊 Источник данных: `superstore.csv`

Открытый датасет Sample Superstore — продажи сетевого магазина за 2014–2017 годы.

| Параметр | Значение |
|---|---|
| Кодировка | cp1252 (Windows-1252), не UTF-8 |
| Разделитель | запятая `,` |
| Переносы строк | CRLF (`\r\n`) |
| Строк | 9994 + заголовок |
| Колонок | 21 |
| Формат дат | М/Д/ГГГГ без ведущих нулей (`6/9/2014` = 9 июня) |
| Особенности | 427 неразрывных пробелов (`\xa0`) в текстовых полях |

---

## 📥 Как получить данные

Датасет в репозитории не хранится — скачайте его сами:

1. Откройте [Superstore Dataset на Kaggle](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final) (нужен бесплатный аккаунт) и нажмите **Download**.
2. Распакуйте архив, переименуйте CSV-файл в `superstore.csv` и положите в папку `input/`.
3. Проверьте размер: **2 287 806 байт**. Если размер другой — файл не тот или повреждён.

> ⚠️ **Не открывайте `superstore.csv` в Excel или текстовом редакторе и не сохраняйте его.** Редактор может молча сменить кодировку, переносы строк или формат дат, и DAG упадёт на проверке заголовка или загрузит испорченные данные. Если файл открывали — скачайте заново.

---

## 🚀 Запуск

**Что нужно:** [Docker Desktop](https://www.docker.com/products/docker-desktop/) и файл `superstore.csv` в папке `input/` (см. [Как получить данные](#-как-получить-данные)).

```bash
# 1. Клонировать репозиторий
git clone https://github.com/aznrz/ETL_Airflow_Project.git
cd ETL_Airflow_Project

# 2. Запустить Airflow и PostgreSQL
docker compose up -d

# 3. Создать базу для данных проекта
docker compose exec postgres psql -U airflow -c "CREATE DATABASE etl_data;"

# 4. Создать подключение etl_postgres в Airflow
docker compose exec airflow airflow connections add etl_postgres \
  --conn-type postgres --conn-host postgres --conn-port 5432 \
  --conn-schema etl_data --conn-login airflow --conn-password airflow

# 5. Получить сгенерированный пароль администратора
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

Откройте http://localhost:8080 и войдите под логином `admin` с этим паролем. Включите `superstore_raw_dag` и запустите его вручную. Проверить результат:

```bash
docker compose exec postgres psql -U airflow -d etl_data -c "SELECT COUNT(*) FROM raw.superstore;"
```

| Действие | Команда |
|---|---|
| Остановить сервисы | `docker compose down` |
| Статус контейнеров | `docker compose ps` |
| Логи Airflow | `docker compose logs -f airflow` |

> ⚠️ Логины и пароли в `docker-compose.yaml` и в командах выше — стандартные значения для локальной разработки. В продакшене их использовать нельзя.

---

## 🖼️ Скриншоты

**Граф DAG** — три задачи выполнились успешно:

![Граф DAG](docs/dag_graph.png)

**Лог задачи `load_postgres`** — повторный запуск заменил 9994 строки, а не добавил их:

![Лог задачи](docs/task_logs.png)

---

## 🎯 Дорожная карта

- [x] Окружение Airflow + PostgreSQL в Docker
- [x] ETL-DAG: CSV → PostgreSQL (слой raw), идемпотентная загрузка
- [ ] Слои данных: raw → staging → mart (звёздная схема)
- [ ] Проверки качества данных перед загрузкой
- [ ] Автоматическая обработка новых файлов без повторной загрузки
- [ ] Ежедневное расписание
- [ ] Уведомления об ошибках в Telegram
- [ ] Отчёт Power BI поверх витрины mart

---

*ETL Airflow Project · последнее обновление: 28.09.2026, 21:42 (Алматы, UTC+5)*
