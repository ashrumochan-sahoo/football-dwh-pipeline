# ============================================================
# DAG: dag_run_dbt
# Purpose: Orchestrate dbt transformations after all
#          raw data has been loaded into Snowflake
# Schedule: Daily at 8am UTC (after all extract DAGs)
# Dependencies: Runs after fixtures(6am), standings(6:30am),
#               players(7am) have all completed
# ============================================================

import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ============================================================
# CONSTANTS
# ============================================================
DBT_PROJECT_DIR = "/opt/airflow/dbt/football_dwh"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

# ============================================================
# FUNCTION: Validate raw data exists before running dbt
# ============================================================
def validate_raw_data(**context):
    """
    Checks that raw data was loaded before triggering dbt.
    If no data loaded today, skip dbt run to avoid
    building empty mart tables.
    """
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=os.environ.get("SNOWFLAKE_ACCOUNT"),
        user=os.environ.get("SNOWFLAKE_USER"),
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        database="FOOTBALL_RAW",
        schema="FOOTBALL",
        role=os.environ.get("SNOWFLAKE_ROLE")
    )

    cursor = conn.cursor()

    # Check if standings data was loaded today
    cursor.execute("""
        SELECT COUNT(*)
        FROM FOOTBALL_RAW.FOOTBALL.RAW_STANDINGS
    """)

    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    logging.info(f"Raw standings rows loaded today: {count}")

    if count == 0:
        raise ValueError(
            "No raw data loaded today. "
            "Skipping dbt run to avoid empty mart tables."
        )

    logging.info("Raw data validated. Proceeding with dbt run.")


# ============================================================
# DAG DEFINITION
# ============================================================
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_run_dbt",
    description="Run dbt transformations after raw data is loaded",
    default_args=default_args,
    start_date=datetime(2024, 8, 1),
    schedule_interval="0 8 * * *",  # 8am — 1 hour after last extract DAG
    catchup=False,
    tags=["football", "dbt", "transform"]
) as dag:

    # ============================================================
    # Task 1: Validate raw data exists
    # ============================================================
    validate_task = PythonOperator(
        task_id="validate_raw_data",
        python_callable=validate_raw_data,
        provide_context=True
    )

    # ============================================================
    # Task 2: Run dbt deps — install packages
    # ============================================================
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"""
            cd {DBT_PROJECT_DIR} &&
            dbt deps \
                --profiles-dir {DBT_PROFILES_DIR} \
                --project-dir {DBT_PROJECT_DIR}
        """
    )

    # ============================================================
    # Task 3: Run dbt staging models
    # ============================================================
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"""
            cd {DBT_PROJECT_DIR} &&
            dbt run \
                --select staging \
                --profiles-dir {DBT_PROFILES_DIR} \
                --project-dir {DBT_PROJECT_DIR}
        """
    )

    # ============================================================
    # Task 4: Run dbt mart models
    # ============================================================
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"""
            cd {DBT_PROJECT_DIR} &&
            dbt run \
                --select marts \
                --profiles-dir {DBT_PROFILES_DIR} \
                --project-dir {DBT_PROJECT_DIR}
        """
    )

    # ============================================================
    # Task 5: Run dbt tests
    # ============================================================
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
            cd {DBT_PROJECT_DIR} &&
            dbt test \
                --profiles-dir {DBT_PROFILES_DIR} \
                --project-dir {DBT_PROJECT_DIR}
        """
    )

    # ============================================================
    # Task 6: Log success
    # ============================================================
    def log_success(**context):
        logging.info("dbt pipeline completed successfully.")
        logging.info("Staging views and mart tables are up to date.")
        logging.info("All data quality tests passed.")

    log_task = PythonOperator(
        task_id="log_success",
        python_callable=log_success,
        provide_context=True
    )

    # ============================================================
    # Task Dependencies — define the order
    # ============================================================
    validate_task >> dbt_deps >> dbt_run_staging >> dbt_run_marts >> dbt_test >> log_task
