-- ============================================================
-- football-dwh-pipeline | Snowflake Setup Script
-- Run this ONCE as ACCOUNTADMIN after account creation
-- ============================================================

-- ============================================================
-- SECTION 1: WAREHOUSES
-- Separate warehouses = workload isolation + cost control
-- Each process gets its own compute, billed independently
-- ============================================================

-- Used by Airflow to load raw data
CREATE WAREHOUSE IF NOT EXISTS FOOTBALL_INGEST_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60        -- suspends after 60 seconds of inactivity
    AUTO_RESUME = TRUE       -- resumes automatically when a query hits it
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Used by Airflow for raw data ingestion';

-- Used by dbt to run transformations
CREATE WAREHOUSE IF NOT EXISTS FOOTBALL_TRANSFORM_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Used by dbt for staging and mart transformations';

-- Used for ad-hoc queries and analytics
CREATE WAREHOUSE IF NOT EXISTS FOOTBALL_QUERY_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Used for analytical queries on mart layer';

-- ============================================================
-- SECTION 2: DATABASES
-- Three databases = three layers of the pipeline
-- RAW → STAGING → MARTS (each owned by a separate role)
-- ============================================================

-- Raw layer: untouched data exactly as it came from the API
CREATE DATABASE IF NOT EXISTS FOOTBALL_RAW
    COMMENT = 'Raw API data — never modified after load';

-- Staging layer: light cleaning, type casting, renaming
CREATE DATABASE IF NOT EXISTS FOOTBALL_STAGING
    COMMENT = 'dbt staging models — cleaned and standardized';

-- Marts layer: final star schema, analytics-ready
CREATE DATABASE IF NOT EXISTS FOOTBALL_MARTS
    COMMENT = 'dbt mart models — fact and dimension tables';

-- ============================================================
-- SECTION 3: SCHEMAS
-- One schema per database to keep it clean
-- ============================================================

CREATE SCHEMA IF NOT EXISTS FOOTBALL_RAW.FOOTBALL
    COMMENT = 'Raw tables: fixtures, players, standings, teams';

CREATE SCHEMA IF NOT EXISTS FOOTBALL_STAGING.FOOTBALL
    COMMENT = 'Staging models: stg_fixtures, stg_players etc';

CREATE SCHEMA IF NOT EXISTS FOOTBALL_MARTS.FOOTBALL
    COMMENT = 'Mart models: fact_matches, dim_teams etc';

-- ============================================================
-- SECTION 4: ROLES
-- Each role maps to one pipeline layer
-- Principle of least privilege — each process only accesses what it needs
-- ============================================================

CREATE ROLE IF NOT EXISTS FOOTBALL_LOADER
    COMMENT = 'Used by Airflow — can only write to RAW database';

CREATE ROLE IF NOT EXISTS FOOTBALL_TRANSFORMER
    COMMENT = 'Used by dbt — can read RAW, write to STAGING and MARTS';

CREATE ROLE IF NOT EXISTS FOOTBALL_ANALYST
    COMMENT = 'Read-only access to MARTS layer';

-- ============================================================
-- SECTION 5: GRANT WAREHOUSE ACCESS TO ROLES
-- ============================================================

GRANT USAGE ON WAREHOUSE FOOTBALL_INGEST_WH    TO ROLE FOOTBALL_LOADER;
GRANT USAGE ON WAREHOUSE FOOTBALL_TRANSFORM_WH  TO ROLE FOOTBALL_TRANSFORMER;
GRANT USAGE ON WAREHOUSE FOOTBALL_QUERY_WH      TO ROLE FOOTBALL_ANALYST;

-- ============================================================
-- SECTION 6: GRANT DATABASE + SCHEMA ACCESS TO ROLES
-- ============================================================

-- LOADER: full access to RAW only
GRANT USAGE ON DATABASE FOOTBALL_RAW                    TO ROLE FOOTBALL_LOADER;
GRANT USAGE ON SCHEMA FOOTBALL_RAW.FOOTBALL             TO ROLE FOOTBALL_LOADER;
GRANT CREATE TABLE ON SCHEMA FOOTBALL_RAW.FOOTBALL      TO ROLE FOOTBALL_LOADER;
GRANT INSERT, UPDATE ON ALL TABLES IN SCHEMA FOOTBALL_RAW.FOOTBALL TO ROLE FOOTBALL_LOADER;
GRANT INSERT, UPDATE ON FUTURE TABLES IN SCHEMA FOOTBALL_RAW.FOOTBALL TO ROLE FOOTBALL_LOADER;

-- TRANSFORMER: read RAW, write STAGING and MARTS
GRANT USAGE ON DATABASE FOOTBALL_RAW                    TO ROLE FOOTBALL_TRANSFORMER;
GRANT USAGE ON SCHEMA FOOTBALL_RAW.FOOTBALL             TO ROLE FOOTBALL_TRANSFORMER;
GRANT SELECT ON ALL TABLES IN SCHEMA FOOTBALL_RAW.FOOTBALL TO ROLE FOOTBALL_TRANSFORMER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA FOOTBALL_RAW.FOOTBALL TO ROLE FOOTBALL_TRANSFORMER;

GRANT USAGE ON DATABASE FOOTBALL_STAGING                TO ROLE FOOTBALL_TRANSFORMER;
GRANT USAGE ON SCHEMA FOOTBALL_STAGING.FOOTBALL         TO ROLE FOOTBALL_TRANSFORMER;
GRANT CREATE TABLE ON SCHEMA FOOTBALL_STAGING.FOOTBALL  TO ROLE FOOTBALL_TRANSFORMER;
GRANT ALL ON ALL TABLES IN SCHEMA FOOTBALL_STAGING.FOOTBALL TO ROLE FOOTBALL_TRANSFORMER;
GRANT ALL ON FUTURE TABLES IN SCHEMA FOOTBALL_STAGING.FOOTBALL TO ROLE FOOTBALL_TRANSFORMER;

GRANT USAGE ON DATABASE FOOTBALL_MARTS                  TO ROLE FOOTBALL_TRANSFORMER;
GRANT USAGE ON SCHEMA FOOTBALL_MARTS.FOOTBALL           TO ROLE FOOTBALL_TRANSFORMER;
GRANT CREATE TABLE ON SCHEMA FOOTBALL_MARTS.FOOTBALL    TO ROLE FOOTBALL_TRANSFORMER;
GRANT ALL ON ALL TABLES IN SCHEMA FOOTBALL_MARTS.FOOTBALL TO ROLE FOOTBALL_TRANSFORMER;
GRANT ALL ON FUTURE TABLES IN SCHEMA FOOTBALL_MARTS.FOOTBALL TO ROLE FOOTBALL_TRANSFORMER;

-- ANALYST: read-only on MARTS
GRANT USAGE ON DATABASE FOOTBALL_MARTS                  TO ROLE FOOTBALL_ANALYST;
GRANT USAGE ON SCHEMA FOOTBALL_MARTS.FOOTBALL           TO ROLE FOOTBALL_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA FOOTBALL_MARTS.FOOTBALL TO ROLE FOOTBALL_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA FOOTBALL_MARTS.FOOTBALL TO ROLE FOOTBALL_ANALYST;

-- ============================================================
-- SECTION 7: ASSIGN ROLES TO YOUR USER
-- So you can switch into any role and test everything
-- ============================================================

GRANT ROLE FOOTBALL_LOADER      TO USER ashrumochan135;
GRANT ROLE FOOTBALL_TRANSFORMER TO USER ashrumochan135;
GRANT ROLE FOOTBALL_ANALYST     TO USER ashrumochan135;

-- ============================================================
-- SECTION 8: VERIFY SETUP
-- Run these after the script to confirm everything was created
-- ============================================================

SHOW WAREHOUSES;
SHOW DATABASES;
SHOW ROLES;