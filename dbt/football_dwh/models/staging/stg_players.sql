-- ============================================================
-- Model: stg_players
-- Layer: Staging
-- Source: FOOTBALL_RAW.FOOTBALL.RAW_PLAYERS
-- Purpose: Clean and standardize raw player statistics
--          Rename columns, cast types, handle nulls
--          No joins, no business logic here
-- Materialization: View (no storage cost)
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('football_raw', 'raw_players') }}
),

cleaned AS (
    SELECT
        -- Primary identifiers
        snapshot_date                           AS snapshot_date,
        CAST(player_id AS INTEGER)              AS player_id,
        CAST(league_id AS INTEGER)              AS league_id,
        CAST(season AS INTEGER)                 AS season,
        CAST(team_id AS INTEGER)                AS team_id,

        -- Player info
        UPPER(TRIM(player_name))                AS player_name,
        CAST(player_age AS INTEGER)             AS player_age,
        UPPER(TRIM(nationality))                AS nationality,
        UPPER(TRIM(team_name))                  AS team_name,

        -- Performance metrics
        COALESCE(CAST(appearances AS INTEGER), 0)   AS appearances,
        COALESCE(CAST(goals AS INTEGER), 0)         AS goals,
        COALESCE(CAST(assists AS INTEGER), 0)       AS assists,
        COALESCE(CAST(yellow_cards AS INTEGER), 0)  AS yellow_cards,
        COALESCE(CAST(red_cards AS INTEGER), 0)     AS red_cards,

        -- Rating — handle nulls with 0
        COALESCE(CAST(rating AS FLOAT), 0)          AS rating,

        -- Derived columns
        COALESCE(CAST(goals AS INTEGER), 0) +
        COALESCE(CAST(assists AS INTEGER), 0)        AS goal_contributions,

        ROUND(
            COALESCE(CAST(goals AS FLOAT), 0) /
            NULLIF(CAST(appearances AS FLOAT), 0),
            2
        )                                            AS goals_per_game,

        -- Metadata
        _loaded_at                                   AS _loaded_at

    FROM source
    WHERE player_id IS NOT NULL
      AND team_id IS NOT NULL
      AND snapshot_date IS NOT NULL
)

SELECT * FROM cleaned
