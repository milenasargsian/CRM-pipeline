# CRM Data Pipeline

A data engineering project that ingests CRM data into ClickHouse Cloud,
transforms it using dbt, and orchestrates the pipeline with Apache Airflow.

---

## Project Structure
CRM pipeline/

├── data/

│   ├── users.csv

│   ├── stages.csv

│   ├── fields.csv

│   ├── deal_changes.csv

│   ├── activity.csv

│   └── activity_types.csv

├── dags/

│   └── crm_ingestion.py

├── scripts/

│   └── ingest.py

├── dbt_project/

│   ├── logs/

│   │   └── dbt.log

│   ├── models/

│   │   └── staging/

│   │       ├── stg_users.sql

│   │       ├── stg_stages.sql

│   │       ├── stg_fields.sql

│   │       ├── stg_deal_changes.sql

│   │       ├── stg_activity.sql

│   │       ├── stg_activity_types.sql

│   │       └── schema.yml

│   ├── dbt_project.yml

│   └── profiles.yml

├── screenshots/ # screenshots of the results

├── .dockerignore

├── .gitignore

├── docker-compose.yml

├── Dockerfile

└── requirements.txt

---

## Architecture & Design

### Data Warehouse Design

ClickHouse Cloud is used as the data warehouse. The database is organized
into two layers using table prefixes:

crm (database)

├── lnd_*       ← Landing layer: raw data as-is from source CSVs

└── crm_staging ← Staging layer: cleaned and typed views (managed by dbt)

**Landing layer** (`crm.lnd_*`) — managed by `ingest.py`:
| Table | Description | Rows |
|---|---|---|
| `lnd_users` | CRM users / sales reps | 1,787 |
| `lnd_stages` | Deal pipeline stages | 9 |
| `lnd_fields` | CRM field metadata | 4 |
| `lnd_deal_changes` | Historical deal changes | 15,406 |
| `lnd_activity` | CRM activities | 4,579 |
| `lnd_activity_types` | Activity type lookup | 4 |

**Staging layer** (`crm_staging.stg_*`) — managed by dbt:
| Model | Description |
|---|---|
| `stg_users` | Cleaned users with normalized email and parsed timestamps |
| `stg_stages` | Cleaned stage names |
| `stg_fields` | Cleaned field metadata with lowercase keys |
| `stg_deal_changes` | Deal changes with parsed DateTime and null handling |
| `stg_activity` | Activities with Boolean casting and DateTime parsing |
| `stg_activity_types` | Activity types with Boolean active flag |

### Design Decisions

- **Landing layer stores data as-is** — no transformations during ingestion.
  This preserves the raw source data and makes debugging easy.
- **All datetime casting happens in staging** — the source data has mixed
  datetime formats (`2024-04-27 04:51:50.980402` and `2024-05-22T14:03:21`).
  ClickHouse's `parseDateTimeBestEffort()` handles both formats cleanly.
- **Staging models are views** — they are lightweight and always reflect
  the latest landing data without duplicating storage.
- **Idempotent ingestion** — `ingest.py` truncates tables before inserting,
  making it safe to re-run without creating duplicates.

---

## Pipeline Flow
CSV files (source)

↓

ingest.py → crm.lnd_*          (landing layer - raw data)

↓

dbt run   → crm_staging.stg_*  (staging layer - clean data)

↓

dbt test  → 12 tests passing   (data quality validation)

---

**The entire pipeline is orchestrated by Apache Airflow and runs
automatically every day at 06:00 UTC.**

---

Airflow DAG: crm_daily_ingestion

├── Task 1: validate_source_files      → checks all 6 CSVs exist

├── Task 2: ingest_csvs_to_clickhouse  → runs ingest.py

├── Task 3: dbt_run_staging            → runs all staging models

└── Task 4: dbt_test_staging           → runs all dbt tests

---

## Run Instructions

### 1. Run ingestion manually
```bash
python scripts/ingest.py
```

### 2. Run dbt transformations
```bash
cd dbt_project
dbt run --profiles-dir . --project-dir .
dbt test --profiles-dir . --project-dir .
```
### 3. Start Airflow (automated scheduling)
```bash
# Step 1: Initialize Airflow
docker-compose up airflow-init

# Step 2: Start Airflow
docker-compose up airflow-webserver airflow-scheduler
```

Open **http://localhost:8080** in your browser:
- Username: `admin`
- Password: `admin`

The `crm_daily_ingestion` DAG will appear and run automatically
every day at 06:00 UTC. It can also be triggered manually by clicking
the ▶ button.

---

## dbt Tests

The following data quality tests are defined in `schema.yml`:

| Model | Column | Test |
|---|---|---|
| `stg_users` | `user_id` | unique, not_null |
| `stg_users` | `email` | not_null |
| `stg_stages` | `stage_id` | unique, not_null |
| `stg_fields` | `field_id` | unique, not_null |
| `stg_deal_changes` | `deal_id` | not_null |
| `stg_deal_changes` | `change_time` | not_null |
| `stg_activity` | `activity_id` | not_null |
| `stg_activity_types` | `activity_type_id` | unique, not_null |

> **Note:** `activity_id` uniqueness test was intentionally removed after
> investigation revealed 11 duplicate IDs exist in the source data.

---

## Tools & Technologies

| Tool | Version | Purpose |
|---|---|---|
| Python | 3.13 | Ingestion scripting |
| ClickHouse Cloud | 24.x | Data warehouse |
| dbt-clickhouse | 1.10.0 | SQL transformations |
| Apache Airflow | 2.9.0 | Orchestration & scheduling |
| Docker | 29.3.1 | Running Airflow on Windows |
| pandas | 2.x | CSV processing |
