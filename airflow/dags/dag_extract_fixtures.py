# ============================================================
# DAG: dag_extract_fixtures
# Purpose: Extract yesterday's football fixtures from API
#          and load raw JSON into Snowflake RAW layer
# Schedule: Daily at 6am UTC
# ============================================================

import os
import json
import logging
from datetime import datetime, timedelta

import requests
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator

# ============================================================
# CONSTANTS
# Read from environment variables set in docker-compose.yml
# which reads from your .env file
# ============================================================
API_KEY = os.environ.get("API_FOOTBALL_KEY")
SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.environ.get("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_ROLE = os.environ.get("SNOWFLAKE_ROLE")
SNOWFLAKE_DATABASE = os.environ.get("SNOWFLAKE_RAW_DATABASE")
SNOWFLAKE_SCHEMA = os.environ.get("SNOWFLAKE_RAW_SCHEMA")

# Leagues we care about
# 39 = English Premier League
# 140 = La Liga
LEAGUE_IDS = [39, 140]

# ============================================================
# FUNCTION 1: Extract fixtures from API
# ============================================================
def extract_fixtures(**context):
    """
    Pulls fixtures for yesterday's date from API-Football.
    Returns list of raw fixture records.
    Uses Airflow's execution_date to determine which date to pull.
    This makes the DAG idempotent — re-running for the same date
    always pulls the same data.
    """
    # Get yesterday's date from Airflow context
    execution_date = context["execution_date"]
    target_date = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")

    logging.info(f"Extracting fixtures for date: {target_date}")

    all_fixtures = []

    for league_id in LEAGUE_IDS:
        url = "https://v3.football.api-sports.io/fixtures"
        headers = {"x-apisports-key": API_KEY}
        params = {
            "date": target_date,
            "league": league_id,
            "season": 2024
        }

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()  # raises error if API call failed

        data = response.json()
        fixtures = data.get("response", [])

        logging.info(f"League {league_id}: found {len(fixtures)} fixtures")

        for fixture in fixtures:
            all_fixtures.append({
                "fixture_id": fixture["fixture"]["id"],
                "league_id": league_id,
                "date": target_date,
                "home_team_id": fixture["teams"]["home"]["id"],
                "home_team_name": fixture["teams"]["home"]["name"],
                "away_team_id": fixture["teams"]["away"]["id"],
                "away_team_name": fixture["teams"]["away"]["name"],
                "home_goals": fixture["goals"]["home"],
                "away_goals": fixture["goals"]["away"],
                "status": fixture["fixture"]["status"]["short"],
                "raw_json": json.dumps(fixture)  # store full raw JSON
            })

    logging.info(f"Total fixtures extracted: {len(all_fixtures)}")

    # Push to XCom so next task can use it
    # XCom = Airflow's way of passing data between tasks
    return all_fixtures


# ============================================================
# FUNCTION 2: Load fixtures into Snowflake RAW
# ============================================================
def load_fixtures_to_snowflake(**context):
    """
    Takes fixtures from previous task via XCom.
    Creates RAW_FIXTURES table if it doesn't exist.
    Inserts records with _loaded_at timestamp for incremental tracking.
    Uses MERGE to avoid duplicates on re-runs.
    """
    # Pull data from previous task via XCom
    ti = context["ti"]
    fixtures = ti.xcom_pull(task_ids="extract_fixtures")

    if not fixtures:
        logging.info("No fixtures to load. Skipping.")
        return

    logging.info(f"Loading {len(fixtures)} fixtures to Snowflake")

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE
    )

    cursor = conn.cursor()

    # Create table if it doesn't exist
    # VARIANT = Snowflake's JSON column type
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS RAW_FIXTURES (
            fixture_id      INTEGER,
            league_id       INTEGER,
            date            DATE,
            home_team_id    INTEGER,
            home_team_name  VARCHAR,
            away_team_id    INTEGER,
            away_team_name  VARCHAR,
            home_goals      INTEGER,
            away_goals      INTEGER,
            status          VARCHAR,
            raw_json        VARIANT,
            _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)

    # MERGE = insert if not exists, update if exists
    # Prevents duplicate rows on DAG re-runs
    merge_sql = """
        MERGE INTO RAW_FIXTURES AS target
        USING (
            SELECT
                %(fixture_id)s      AS fixture_id,
                %(league_id)s       AS league_id,
                %(date)s            AS date,
                %(home_team_id)s    AS home_team_id,
                %(home_team_name)s  AS home_team_name,
                %(away_team_id)s    AS away_team_id,
                %(away_team_name)s  AS away_team_name,
                %(home_goals)s      AS home_goals,
                %(away_goals)s      AS away_goals,
                %(status)s          AS status,
                PARSE_JSON(%(raw_json)s) AS raw_json
        ) AS source
        ON target.fixture_id = source.fixture_id
        WHEN MATCHED THEN UPDATE SET
            home_goals   = source.home_goals,
            away_goals   = source.away_goals,
            status       = source.status,
            raw_json     = source.raw_json,
            _loaded_at   = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            fixture_id, league_id, date,
            home_team_id, home_team_name,
            away_team_id, away_team_name,
            home_goals, away_goals,
            status, raw_json
        ) VALUES (
            source.fixture_id, source.league_id, source.date,
            source.home_team_id, source.home_team_name,
            source.away_team_id, source.away_team_name,
            source.home_goals, source.away_goals,
            source.status, source.raw_json
        )
    """

    for fixture in fixtures:
        cursor.execute(merge_sql, fixture)

    conn.commit()
    cursor.close()
    conn.close()

    logging.info("Fixtures loaded successfully")


# ============================================================
# DAG DEFINITION
# ============================================================
default_args = {
    "owner": "airflow",
    "retries": 2,                           # retry twice on failure
    "retry_delay": timedelta(minutes=5),    # wait 5 mins between retries
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_extract_fixtures",
    description="Extract football fixtures from API and load to Snowflake RAW",
    default_args=default_args,
    start_date=datetime(2024, 8, 1),        # backfill starts from here
    schedule_interval="0 6 * * *",          # run daily at 6am UTC
    catchup=False,                          # don't backfill past runs
    tags=["football", "extract", "raw"]
) as dag:

    # Task 1: Extract from API
    extract_task = PythonOperator(
        task_id="extract_fixtures",
        python_callable=extract_fixtures,
        provide_context=True
    )

    # Task 2: Load to Snowflake
    load_task = PythonOperator(
        task_id="load_fixtures_to_snowflake",
        python_callable=load_fixtures_to_snowflake,
        provide_context=True
    )

    # Define order: extract must finish before load starts
    extract_task >> load_task
