-- ============================================================
-- Model: dim_teams
-- Layer: Marts
-- Purpose: Dimension table for football teams
--          One row per team — deduped, clean, stable
--          Used to join against fact tables
-- Materialization: Table
-- ============================================================

WITH standings_teams AS (
    SELECT DISTINCT
        team_id,
        team_name,
        league_id
    FROM {{ ref('stg_standings') }}
),

players_teams AS (
    SELECT DISTINCT
        team_id,
        team_name,
        league_id
    FROM {{ ref('stg_players') }}
),

-- Combine teams from both sources
all_teams AS (
    SELECT * FROM standings_teams
    UNION
    SELECT * FROM players_teams
),

-- Add league name for readability
final AS (
    SELECT
        team_id,
        team_name,
        league_id,
        CASE
            WHEN league_id = 39  THEN 'Premier League'
            WHEN league_id = 140 THEN 'La Liga'
            ELSE 'Unknown'
        END                         AS league_name,
        CASE
            WHEN league_id = 39  THEN 'England'
            WHEN league_id = 140 THEN 'Spain'
            ELSE 'Unknown'
        END                         AS country,
        CURRENT_TIMESTAMP()         AS _created_at
    FROM all_teams
)

SELECT * FROM final
