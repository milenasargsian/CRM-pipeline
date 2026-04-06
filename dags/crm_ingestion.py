"""
CRM Ingestion DAG
Orchestrates the daily CRM data pipeline:
  1. Validates source CSV files exist
  2. Loads CSVs into ClickHouse landing tables
  3. Triggers dbt staging transformations

Schedule: daily at 06:00 UTC
"""

import subprocess
import sys
import os
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

PROJECT_ROOT = Path("/opt/airflow")
SCRIPTS_DIR  = PROJECT_ROOT / "scripts"
DBT_DIR      = PROJECT_ROOT / "dbt_project"

default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      60,
    "email_on_failure": False,
}

with DAG(
    dag_id="crm_daily_ingestion",
    description="Load CRM CSVs into ClickHouse and run dbt staging",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["crm", "ingestion", "dbt"],
) as dag:

    # Validate source files
    validate_sources = BashOperator(
        task_id="validate_source_files",
        bash_command="""
        set -e
        DATA_DIR="/opt/airflow/data"
        for f in users.csv stages.csv fields.csv deal_changes.csv activity.csv activity_types.csv; do
            if [ ! -f "$DATA_DIR/$f" ]; then
                echo "ERROR: Missing $f"
                exit 1
            fi
            echo "Found $f — $(wc -l < $DATA_DIR/$f) lines"
        done
        echo "All source files validated ✓"
        """,
    )

    # Run ingestion
    def run_ingestion(**context):
        """Load CSVs into ClickHouse landing tables."""
        result = subprocess.run(
            [sys.executable, "/opt/airflow/scripts/ingest.py"],
            capture_output=True,
            text=True,
            env={
                **os.environ,
                "CH_PASSWORD": os.getenv("CH_PASSWORD", ""),
            },
        )
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            raise RuntimeError(f"Ingestion failed:\n{result.stderr}")

    ingest_to_clickhouse = PythonOperator(
        task_id="ingest_csvs_to_clickhouse",
        python_callable=run_ingestion,
    )

    # dbt staging
    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="""
        set -e
        cd /opt/airflow/dbt_project
        rm -rf /opt/airflow/dbt_project/target
        rm -rf /opt/airflow/dbt_project/dbt_packages
        dbt run --select staging \
            --profiles-dir /opt/airflow/dbt_project \
            --project-dir /opt/airflow/dbt_project
        """,
    )

    # dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test_staging",
        bash_command="""
        set -e
        cd /opt/airflow/dbt_project
        dbt test --select staging \
            --profiles-dir /opt/airflow/dbt_project \
            --project-dir /opt/airflow/dbt_project
        """,
    )