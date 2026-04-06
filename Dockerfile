FROM apache/airflow:2.9.0

# Install required Python packages for CRM pipeline
RUN pip install clickhouse-connect pandas dbt-core dbt-clickhouse

# Create dbt target directory
RUN mkdir -p /opt/airflow/dbt_project/target