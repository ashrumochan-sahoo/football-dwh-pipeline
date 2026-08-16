FROM apache/airflow:2.9.1

USER root
RUN apt-get update && apt-get install -y git && apt-get clean

USER airflow
RUN pip install --no-cache-dir \
    dbt-snowflake==1.12.0
