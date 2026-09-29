# ETL Airflow Project — пайплайн данных на Apache Airflow

**Русский** | [English](README.en.md)

![Python](https://img.shields.io/badge/Python-3.x-3776AB?logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache_Airflow-2.10-017CEE?logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-Reporting-F2C811)
![Статус](https://img.shields.io/badge/%D0%A1%D1%82%D0%B0%D1%82%D1%83%D1%81-%D0%92%20%D1%80%D0%B0%D0%B7%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D0%BA%D0%B5-orange)

ETL-пайплайн в Docker. Он загружает данные о продажах (Superstore) из двух источников — CSV и Excel, оркестрирует обработку через **Apache Airflow**, складывает результат в **PostgreSQL** по слоям raw → staging → mart и отдаёт звёздную схему в **Power BI** для отчётности.

```
CSV   ─┐
       ├─→  Airflow (Python + SQL)  →  PostgreSQL: raw → staging → mart  →  Power BI
Excel ─┘
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

## 🔄 Пайплайн `superstore_raw_dag`

```
read_csv   → transform       → load_postgres       ─┐                 ┌─→ stg_superstore ─→ dim_customer, dim_product, dim_date ─┐
                                                    ├─→ check_quality ─┼─→ stg_people ─────→ dim_location ────────────────────────┼─→ fact_sales
read_excel → transform_excel → load_excel_postgres ─┘                 └─→ stg_returns ──────────────────────────────────────────┘
```

15 задач. Две ветки загрузки идут параллельно, `check_quality` ждёт обе. Staging строится только после успешных проверок: три таблицы параллельно, затем измерения и факт.

| Задача | Что делает |
|---|---|
| `read_csv` | Проверяет, что `superstore.csv` есть и заголовок совпадает с ожидаемыми 21 колонкой, считает строки. |
| `transform` | Перекодирует cp1252 → UTF-8, переводит заголовки в snake_case, добавляет технические колонки `source_file` и `loaded_at`. |
| `load_postgres` | Заменяет строки этого файла в `raw.superstore`: `DELETE` и `COPY` в одной транзакции. |
| `read_excel` | Проверяет листы `People` и `Returns` в `superstore_extra.xlsx` и их заголовки, считает непустые строки. |
| `transform_excel` | Каждый лист → свой UTF-8 CSV: числа без `.0`, пустые строки пропускаются. |
| `load_excel_postgres` | Загружает `raw.people` и `raw.returns` в одной транзакции. |
| `check_quality` | [`dags/sql/dq_checks.sql`](dags/sql/dq_checks.sql): 15 проверок слоя raw, результаты — в `dq.check_results`. Любой `FAIL` останавливает DAG до staging. |
| `stg_superstore`, `stg_people`, `stg_returns` | [`dags/sql/staging/`](dags/sql/staging/): типы (`INTEGER`, `DATE`, `NUMERIC`), `TRIM`, замена `\xa0` на пробел, первичные ключи. Одна задача — одна таблица. |
| `dim_customer`, `dim_product`, `dim_date`, `dim_location` | [`dags/sql/mart/`](dags/sql/mart/): измерения звёздной схемы. |
| `fact_sales` | [`dags/sql/mart/fact_sales.sql`](dags/sql/mart/fact_sales.sql): факт продаж, внешние ключи, сверка строк с staging. |

- **Сверка строк** на каждом шаге: если количество не сходится, DAG падает.
- **Всё или ничего**: загрузка raw идёт в транзакции, при ошибке старые данные остаются нетронутыми. Staging и mart перестраиваются целиком.
- **Идемпотентность**: повторный запуск не задваивает данные — удалено 9994, загружено 9994, в таблице по-прежнему 9994 строки.
- **Ключи как проверки**: `PRIMARY KEY`, `UNIQUE`, `NOT NULL` и `FOREIGN KEY` в staging и mart роняют DAG, если в данных появились дубли или потерянные связи.
- **Проверки качества до staging**: если в raw пришли плохие данные, staging и mart остаются в прежнем, корректном состоянии.
- **Без паролей в коде**: подключение берётся из Airflow Connection `etl_postgres`.
- Данные проекта лежат в отдельной базе `etl_data`, она не смешивается со служебной базой Airflow.

---

## ✅ Проверки качества данных: схема `dq`

Задача `check_quality` выполняет 15 правил над слоем raw: пустая выгрузка, дубли и пустые `row_id`, формат и реальность дат, отгрузка не раньше заказа, формат чисел, количество и скидка в допустимых пределах, однозначность клиентов и товаров, возвраты по существующим заказам, ровно один менеджер на регион.

| Уровень | Что происходит |
|---|---|
| `ERROR` | Проверка не прошла → статус `FAIL`, задача падает, staging и mart не перестраиваются. |
| `WARNING` | Статус `WARN`, пайплайн продолжается. Так отмечены известные особенности источника: 32 `product_id` с разными названиями, 1 полный дубль строки. |

- **`dq.check_results`** — история проверок: `run_id`, `check_no`, `check_name`, `severity`, `failed_count`, статус `PASS` / `WARN` / `FAIL`. Результаты сохраняются до того, как задача упадёт, поэтому история видна и для неудачных запусков.
- **`dq.try_mdy_date`** — разбор даты М/Д/ГГГГ, который на невозможной дате (`2/30/2016`) возвращает `NULL` вместо ошибки: `TO_DATE` уронил бы задачу, и проверка не смогла бы посчитать такие строки.

```bash
docker compose exec postgres psql -U airflow -d etl_data -c "SELECT check_no, check_name, status, failed_count FROM dq.check_results WHERE checked_at = (SELECT MAX(checked_at) FROM dq.check_results) ORDER BY check_no;"
```

---

## ⭐ Модель данных: звёздная схема `mart`

```
              dim_customer
                   │
dim_product ── fact_sales ── dim_location
                   │
               dim_date  (order_date, ship_date)
```

| Таблица | Строк | Ключ | Что внутри |
|---|---:|---|---|
| `fact_sales` | 9994 | `row_id` | Одна строка = одна позиция заказа: продажи, количество, скидка, прибыль, `is_returned` |
| `dim_customer` | 793 | `customer_id` | Имя клиента, сегмент |
| `dim_product` | 1894 | `product_key` (суррогатный) | `product_id`, название, категория, подкатегория |
| `dim_location` | 632 | `location_key` (суррогатный) | Страна, регион, штат, город, индекс, региональный менеджер |
| `dim_date` | 1826 | `calendar_date` | Год, квартал, месяц, день недели, выходной — 2014–2018 |

Решения по модели:
- **Зерно — позиция заказа (`row_id`).** Итоги по заказам Power BI считает сам, связь с товарами сохраняется.
- **Суррогатный `product_key`.** 32 `product_id` в источнике относятся к разным товарам, поэтому товар — это пара `product_id` + название: 1862 ID дают 1894 товара.
- **Адрес — в `dim_location`, а не в клиенте:** 780 из 793 клиентов получали заказы в разные города. Региональный менеджер (лист `People`) тоже здесь, он привязан к региону.
- **Возвраты — флаг `is_returned` в факте.** Возврат в источнике оформлен на весь заказ: 296 заказов = 800 позиций.
- **`dim_date` — одна таблица для двух дат** (заказа и отгрузки), календарь на полные годы.

Сверка: сумма продаж в `raw.superstore` и в `mart.fact_sales` совпадает — **2 297 200,86**.

---

## 📂 Структура проекта

| Папка / файл | Что внутри |
|---|---|
| [dags/](dags/) | DAG для Airflow |
| [dags/sql/](dags/sql/) | [`dq_checks.sql`](dags/sql/dq_checks.sql) — проверки качества, [`staging/`](dags/sql/staging/) и [`mart/`](dags/sql/mart/) — по одному файлу на таблицу |
| [input/](input/) | Исходные файлы (`superstore.csv`, `superstore_extra.xlsx`) — в git не хранятся |
| [output/](output/) | Промежуточные UTF-8 CSV перед загрузкой в raw |
| [archive/](archive/) | Архив обработанных файлов |
| [docs/](docs/) | Скриншоты |
| [Dockerfile](Dockerfile), [requirements.txt](requirements.txt) | Образ Airflow с дополнительными библиотеками (`openpyxl`) |
| [docker-compose.yaml](docker-compose.yaml) | Сервисы Airflow и PostgreSQL |

---

## 📊 Источники данных

### `superstore.csv` — заказы

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

### `superstore_extra.xlsx` — справочники

Excel-версия того же Sample Superstore. Из неё берутся два листа, лист `Orders` не грузится: заказы приходят из CSV.

| Лист | Колонки | Строк | Куда |
|---|---|---:|---|
| `People` | `Person`, `Region` | 4 | региональные менеджеры → `dim_location` |
| `Returns` | `Returned`, `Order ID` | 296 | возвращённые заказы → `is_returned` в факте |

---

## 📥 Как получить данные

Датасеты в репозитории не хранятся — скачайте их сами и положите в `input/`:

| Файл | Откуда | Размер |
|---|---|---:|
| `superstore.csv` | [Superstore Dataset на Kaggle](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final) (нужен бесплатный аккаунт) → **Download** → распаковать | 2 287 806 байт |
| `superstore_extra.xlsx` | Excel-версия Sample Superstore с листами `Orders`, `People` и `Returns` | 1 106 911 байт |

1. Переименуйте файлы точно как в таблице: DAG ищет именно эти имена.
2. Проверьте размер. Если он другой — файл не тот или повреждён.

> ⚠️ **Не открывайте файлы в Excel или текстовом редакторе и не сохраняйте их.** Редактор может молча сменить кодировку, переносы строк или формат дат, и DAG упадёт на проверке заголовка или загрузит испорченные данные. Если файл открывали — скачайте заново.

---

## 🚀 Запуск

**Что нужно:** [Docker Desktop](https://www.docker.com/products/docker-desktop/) и оба файла данных в папке `input/` (см. [Как получить данные](#-как-получить-данные)).

```bash
# 1. Клонировать репозиторий
git clone https://github.com/aznrz/ETL_Airflow_Project.git
cd ETL_Airflow_Project

# 2. Собрать образ Airflow и запустить Airflow и PostgreSQL
docker compose up -d --build

# 3. Создать базу для данных проекта
docker compose exec postgres psql -U airflow -c "CREATE DATABASE etl_data;"

# 4. Создать подключение etl_postgres в Airflow
docker compose exec airflow airflow connections add etl_postgres \
  --conn-type postgres --conn-host postgres --conn-port 5432 \
  --conn-schema etl_data --conn-login airflow --conn-password airflow

# 5. Получить сгенерированный пароль администратора
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

Откройте http://localhost:8080 и войдите под логином `admin` с этим паролем. Включите `superstore_raw_dag` и запустите его вручную. Проверить результат (должно быть 9994):

```bash
docker compose exec postgres psql -U airflow -d etl_data -c "SELECT COUNT(*) FROM mart.fact_sales;"
```

| Действие | Команда |
|---|---|
| Остановить сервисы | `docker compose down` |
| Статус контейнеров | `docker compose ps` |
| Логи Airflow | `docker compose logs -f airflow` |

> ⚠️ Логины и пароли в `docker-compose.yaml` и в командах выше — стандартные значения для локальной разработки. В продакшене их использовать нельзя.

---

## 🖼️ Скриншоты

**Граф DAG** — две ветки загрузки, проверки качества, затем staging и mart; все 15 задач выполнились успешно:

![Граф DAG](docs/dag_graph.png)

**Лог задачи `check_quality`** — 15 проверок: 13 `PASS` и 2 `WARN` по известным особенностям источника, ни одного `FAIL`:

![Лог задачи](docs/task_logs.png)

---

## 🎯 Дорожная карта

- [x] Окружение Airflow + PostgreSQL в Docker
- [x] ETL-DAG: CSV → PostgreSQL (слой raw), идемпотентная загрузка
- [x] Вторая ветка загрузки: Excel → PostgreSQL (менеджеры и возвраты)
- [x] Слои данных: raw → staging → mart (звёздная схема)
- [x] Проверки качества данных перед staging (схема `dq`)
- [ ] Автоматическая обработка новых файлов без повторной загрузки
- [ ] Ежедневное расписание
- [ ] Уведомления об ошибках в Telegram
- [ ] Отчёт Power BI поверх витрины mart

---

*ETL Airflow Project · последнее обновление: 29.09.2026, 17:25 (Алматы, UTC+5)*
