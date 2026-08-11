# football-dwh-pipeline

An end-to-end football analytics data platform built with Apache Airflow, dbt, and Snowflake. Extracts real football data from the API-Football REST API, loads it into a cloud data warehouse, and transforms it into an analytics-ready star schema.

---

## Architecture

API-Football (REST API)
│
▼
Apache Airflow (Orchestration)
Extract → Load → Schedule
│
▼
Snowflake RAW Database
(Raw JSON + structured columns)
│
▼
dbt (Transformation)
Staging → Marts
│
▼
Snowflake MARTS Database
(Star Schema — fact + dimension tables)


---

## Tech Stack

| Tool | Purpose |
|---|---|
| **API-Football** | REST API data source — fixtures, standings, player stats |
| **Apache Airflow** | Pipeline orchestration — scheduling, retries, monitoring |
| **Docker** | Containerized Airflow environment |
| **Snowflake** | Cloud data warehouse — RAW, STAGING, MARTS databases |
| **dbt Core** | SQL transformations — staging, testing, documentation |
| **GitHub Actions** | CI — automated dbt tests on every push |

---

## Data Pipeline

### Extract & Load (Airflow)

Three DAGs run on a daily schedule:

| DAG | Schedule | Data |
|---|---|---|
| `dag_extract_fixtures` | 6:00 AM UTC | Match results and scores |
| `dag_extract_standings` | 6:30 AM UTC | League table standings |
| `dag_extract_players` | 7:00 AM UTC | Top scorer statistics |

Each DAG uses an incremental watermark strategy — only pulls data for the previous day, merges into Snowflake RAW using MERGE to prevent duplicates.

### Transform (dbt)

Models are organized in three layers:

RAW → Staging (views) → Marts (tables)


| Layer | Models | Purpose |
|---|---|---|
| Staging | `stg_standings`, `stg_players` | Clean, cast, rename raw columns |
| Marts | `dim_teams` | Team dimension table |
| Marts | `fact_standings` | Daily standings fact table |
| Marts | `fact_player_stats` | Player performance fact table |

### Star Schema

dim_teams
│
├──→ fact_standings
└──→ fact_player_stats


---

## Snowflake Setup

Three databases with separation of concerns:

| Database | Purpose |
|---|---|
| `FOOTBALL_RAW` | Raw data landing zone — never modified |
| `FOOTBALL_STAGING` | dbt staging views |
| `FOOTBALL_MARTS` | Final analytics tables |

Three warehouses for workload isolation:

| Warehouse | Used By |
|---|---|
| `FOOTBALL_INGEST_WH` | Airflow DAGs |
| `FOOTBALL_TRANSFORM_WH` | dbt runs |
| `FOOTBALL_QUERY_WH` | Analysts |

---

## Project Structure

football-dwh-pipeline/
├── airflow/
│ ├── dags/
│ │ ├── dag_extract_fixtures.py
│ │ ├── dag_extract_players.py
│ │ └── dag_extract_standings.py
│ └── docker-compose.yml
├── dbt/
│ └── football_dwh/
│ ├── models/
│ │ ├── staging/
│ │ └── marts/
│ ├── dbt_project.yml
│ └── packages.yml
├── snowflake/
│ └── setup.sql
├── .env.example
├── requirements.txt
└── README.md


---

## Getting Started

### Prerequisites

- Docker Desktop
- Python 3.12+
- Snowflake account
- API-Football account (api-sports.io)

### Setup

**1. Clone the repo**
```bash
git clone https://github.com/ashrumochan-sahoo/football-dwh-pipeline.git
cd football-dwh-pipeline
```

**2. Configure environment variables**
```bash
cp .env.example .env
# Fill in your credentials in .env
```

**3. Run Snowflake setup**
```sql
-- Run snowflake/setup.sql in your Snowflake worksheet as ACCOUNTADMIN
```

**4. Start Airflow**
```bash
cd airflow
docker compose up -d
# Open http://localhost:8080 (admin/admin)
```

**5. Set up dbt**
```bash
cd dbt/football_dwh
python3.12 -m venv venv
source venv/bin/activate
pip install dbt-snowflake
dbt deps
dbt run
dbt test
```

---

## dbt Tests

13 data quality tests covering:
- Not null constraints on all primary keys
- Accepted values for league IDs (39=Premier League, 140=La Liga)
- Referential integrity across models

Run tests:
```bash
dbt test
```

---

## Leagues Covered

| League ID | League | Country |
|---|---|---|
| 39 | Premier League | England |
| 140 | La Liga | Spain |

---

## Author

Ashrumochan Sahoo — Data Engineer
[LinkedIn](https://linkedin.com/in/your-profile) | [GitHub](https://github.com/ashrumochan-sahoo)
