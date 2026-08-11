# ============================================================
# DAG: dag_extract_standings
# Purpose: Extract league standings from API
#          and load raw JSON into Snowflake RAW layer
# Schedule: Daily at 6:30am UTC (30 mins after fixtures DAG)
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
# ============================================================
API_KEY = os.environ.get("API_FOOTBALL_KEY")
SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.environ.get("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_ROLE = os.environ.get("SNOWFLAKE_ROLE")
SNOWFLAKE_DATABASE = os.environ.get("SNOWFLAKE_RAW_DATABASE")
SNOWFLAKE_SCHEMA = os.environ.get("SNOWFLAKE_RAW_SCHEMA")

LEAGUE_IDS = [39, 140]
SEASON = 2024

# ============================================================
# FUNCTION 1: Extract standings from API
# ============================================================
def extract_standings(**context):
    """
    Pulls current standings for each league from API-Football.
    Standings reflect cumulative season data — not date-specific.
    We store a snapshot every day so we can track progression.
    """
    execution_date = context["execution_date"]
    snapshot_date = execution_date.strftime("%Y-%m-%d")

    logging.info(f"Extracting standings snapshot for: {snapshot_date}")

    all_standings = []

    for league_id in LEAGUE_IDS:
        url = "https://v3.football.api-sports.io/standings"
        headers = {"x-apisports-key": API_KEY}
        params = {
            "league": league_id,
            "season": SEASON
        }

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()

        # Standings are nested: response → league → standings → [0] → teams
        try:
            standings = data["response"][0]["league"]["standings"][0]
        except (IndexError, KeyError):
            logging.warning(f"No standings data for league {league_id}")
            continue

        logging.info(f"League {league_id}: {len(standings)} teams in standings")

        for team in standings:
            all_standings.append({
                "snapshot_date": snapshot_date,
                "league_id": league_id,
                "season": SEASON,
                "rank": team["rank"],
                "team_id": team["team"]["id"],
                "team_name": team["team"]["name"],
                "played": team["all"]["played"],
                "win": team["all"]["win"],
                "draw": team["all"]["draw"],
                "lose": team["all"]["lose"],
                "goals_for": team["all"]["goals"]["for"],
                "goals_against": team["all"]["goals"]["against"],
                "goal_diff": team["goalsDiff"],
                "points": team["points"],
                "form": team["form"],
                "raw_json": json.dumps(team)
            })

    logging.info(f"Total standing records extracted: {len(all_standings)}")
    return all_standings


# ============================================================
# FUNCTION 2: Load standings into Snowflake RAW
# ============================================================
def load_standings_to_snowflake(**context):
    """
    Loads standings snapshot into RAW_STANDINGS.
    Uses snapshot_date + team_id + league_id as unique key.
    Allows tracking of how standings change day by day.
    """
    ti = context["ti"]
    standings = ti.xcom_pull(task_ids="extract_standings")

    if not standings:
        logging.info("No standings to load. Skipping.")
        return

    logging.info(f"Loading {len(standings)} standing records to Snowflake")

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

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS RAW_STANDINGS (
            snapshot_date   DATE,
            league_id       INTEGER,
            season          INTEGER,
            rank            INTEGER,
            team_id         INTEGER,
            team_name       VARCHAR,
            played          INTEGER,
            win             INTEGER,
            draw            INTEGER,
            lose            INTEGER,
            goals_for       INTEGER,
            goals_against   INTEGER,
            goal_diff       INTEGER,
            points          INTEGER,
            form            VARCHAR,
            raw_json        VARIANT,
            _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)

    merge_sql = """
        MERGE INTO RAW_STANDINGS AS target
        USING (
            SELECT
                %(snapshot_date)s   AS snapshot_date,
                %(league_id)s       AS league_id,
                %(season)s          AS season,
                %(rank)s            AS rank,
                %(team_id)s         AS team_id,
                %(team_name)s       AS team_name,
                %(played)s          AS played,
                %(win)s             AS win,
                %(draw)s            AS draw,
                %(lose)s            AS lose,
                %(goals_for)s       AS goals_for,
                %(goals_against)s   AS goals_against,
                %(goal_diff)s       AS goal_diff,
                %(points)s          AS points,
                %(form)s            AS form,
                PARSE_JSON(%(raw_json)s) AS raw_json
        ) AS source
        ON  target.snapshot_date = source.snapshot_date
        AND target.team_id       = source.team_id
        AND target.league_id     = source.league_id
        WHEN MATCHED THEN UPDATE SET
            rank          = source.rank,
            played        = source.played,
            win           = source.win,
            draw          = source.draw,
            lose          = source.lose,
            goals_for     = source.goals_for,
            goals_against = source.goals_against,
            goal_diff     = source.goal_diff,
            points        = source.points,
            form          = source.form,
            raw_json      = source.raw_json,
            _loaded_at    = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            snapshot_date, league_id, season, rank,
            team_id, team_name, played, win, draw, lose,
            goals_for, goals_against, goal_diff, points, form, raw_json
        ) VALUES (
            source.snapshot_date, source.league_id, source.season, source.rank,
            source.team_id, source.team_name, source.played, source.win,
            source.draw, source.lose, source.goals_for, source.goals_against,
            source.goal_diff, source.points, source.form, source.raw_json
        )
    """

    for record in standings:
        cursor.execute(merge_sql, record)

    conn.commit()
    cursor.close()
    conn.close()

    logging.info("Standings loaded successfully")


# ============================================================
# DAG DEFINITION
# ============================================================
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_extract_standings",
    description="Extract league standings from API and load to Snowflake RAW",
    default_args=default_args,
    start_date=datetime(2024, 8, 1),
    schedule_interval="30 6 * * *",     # 6:30am — 30 mins after fixtures
    catchup=False,
    tags=["football", "extract", "raw"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_standings",
        python_callable=extract_standings,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id="load_standings_to_snowflake",
        python_callable=load_standings_to_snowflake,
        provide_context=True
    )

    extract_task >> load_task
