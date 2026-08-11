# ============================================================
# DAG: dag_extract_players
# Purpose: Extract top scorers and assisters from API
#          and load raw JSON into Snowflake RAW layer
# Schedule: Daily at 7am UTC (1 hour after fixtures DAG)
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
# FUNCTION 1: Extract top scorers from API
# ============================================================
def extract_players(**context):
    """
    Pulls top scorers for each league from API-Football.
    API returns paginated results — we pull page 1 only
    to stay within 100 requests/day free tier limit.
    Page 1 returns top 20 players per league.
    """
    execution_date = context["execution_date"]
    snapshot_date = execution_date.strftime("%Y-%m-%d")

    logging.info(f"Extracting top scorers snapshot for: {snapshot_date}")

    all_players = []

    for league_id in LEAGUE_IDS:
        url = "https://v3.football.api-sports.io/players/topscorers"
        headers = {"x-apisports-key": API_KEY}
        params = {
            "league": league_id,
            "season": SEASON
        }

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        data = response.json()
        players = data.get("response", [])

        logging.info(f"League {league_id}: {len(players)} top scorers found")

        for entry in players:
            player = entry["player"]
            stats = entry["statistics"][0]  # first stat block = current league

            all_players.append({
                "snapshot_date": snapshot_date,
                "league_id": league_id,
                "season": SEASON,
                "player_id": player["id"],
                "player_name": player["name"],
                "player_age": player["age"],
                "nationality": player["nationality"],
                "team_id": stats["team"]["id"],
                "team_name": stats["team"]["name"],
                "appearances": stats["games"]["appearences"],
                "goals": stats["goals"]["total"],
                "assists": stats["goals"]["assists"],
                "yellow_cards": stats["cards"]["yellow"],
                "red_cards": stats["cards"]["red"],
                "rating": stats["games"]["rating"],
                "raw_json": json.dumps(entry)
            })

    logging.info(f"Total player records extracted: {len(all_players)}")
    return all_players


# ============================================================
# FUNCTION 2: Load players into Snowflake RAW
# ============================================================
def load_players_to_snowflake(**context):
    """
    Loads top scorer snapshots into RAW_PLAYERS.
    Unique key: snapshot_date + player_id + league_id.
    Daily snapshots let us track player stat progression
    across the season over time.
    """
    ti = context["ti"]
    players = ti.xcom_pull(task_ids="extract_players")

    if not players:
        logging.info("No player data to load. Skipping.")
        return

    logging.info(f"Loading {len(players)} player records to Snowflake")

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
        CREATE TABLE IF NOT EXISTS RAW_PLAYERS (
            snapshot_date   DATE,
            league_id       INTEGER,
            season          INTEGER,
            player_id       INTEGER,
            player_name     VARCHAR,
            player_age      INTEGER,
            nationality     VARCHAR,
            team_id         INTEGER,
            team_name       VARCHAR,
            appearances     INTEGER,
            goals           INTEGER,
            assists         INTEGER,
            yellow_cards    INTEGER,
            red_cards       INTEGER,
            rating          FLOAT,
            raw_json        VARIANT,
            _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)

    merge_sql = """
        MERGE INTO RAW_PLAYERS AS target
        USING (
            SELECT
                %(snapshot_date)s   AS snapshot_date,
                %(league_id)s       AS league_id,
                %(season)s          AS season,
                %(player_id)s       AS player_id,
                %(player_name)s     AS player_name,
                %(player_age)s      AS player_age,
                %(nationality)s     AS nationality,
                %(team_id)s         AS team_id,
                %(team_name)s       AS team_name,
                %(appearances)s     AS appearances,
                %(goals)s           AS goals,
                %(assists)s         AS assists,
                %(yellow_cards)s    AS yellow_cards,
                %(red_cards)s       AS red_cards,
                %(rating)s          AS rating,
                PARSE_JSON(%(raw_json)s) AS raw_json
        ) AS source
        ON  target.snapshot_date = source.snapshot_date
        AND target.player_id     = source.player_id
        AND target.league_id     = source.league_id
        WHEN MATCHED THEN UPDATE SET
            goals        = source.goals,
            assists      = source.assists,
            appearances  = source.appearances,
            yellow_cards = source.yellow_cards,
            red_cards    = source.red_cards,
            rating       = source.rating,
            raw_json     = source.raw_json,
            _loaded_at   = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            snapshot_date, league_id, season,
            player_id, player_name, player_age, nationality,
            team_id, team_name, appearances, goals, assists,
            yellow_cards, red_cards, rating, raw_json
        ) VALUES (
            source.snapshot_date, source.league_id, source.season,
            source.player_id, source.player_name, source.player_age,
            source.nationality, source.team_id, source.team_name,
            source.appearances, source.goals, source.assists,
            source.yellow_cards, source.red_cards, source.rating,
            source.raw_json
        )
    """

    for player in players:
        cursor.execute(merge_sql, player)

    conn.commit()
    cursor.close()
    conn.close()

    logging.info("Player data loaded successfully")


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
    dag_id="dag_extract_players",
    description="Extract top scorers from API and load to Snowflake RAW",
    default_args=default_args,
    start_date=datetime(2024, 8, 1),
    schedule_interval="0 7 * * *",      # 7am — 1 hour after fixtures
    catchup=False,
    tags=["football", "extract", "raw"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_players",
        python_callable=extract_players,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id="load_players_to_snowflake",
        python_callable=load_players_to_snowflake,
        provide_context=True
    )

    extract_task >> load_task
