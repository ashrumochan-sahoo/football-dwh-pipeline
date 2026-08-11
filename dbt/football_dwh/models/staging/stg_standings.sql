-- ============================================================
-- Model: stg_standings
-- Layer: Staging
-- Source: FOOTBALL_RAW.FOOTBALL.RAW_STANDINGS
-- Purpose: Clean and standardize raw standings data
--          Rename columns, cast types, handle nulls
--          No joins, no business logic here
-- Materialization: View (no storage cost)
-- ============================================================

WITH source AS (
    SELECT * FROM {{ source('football_raw', 'raw_standings') }}
),

cleaned AS (
    SELECT
        -- Primary identifiers
        snapshot_date                           AS snapshot_date,
        CAST(league_id AS INTEGER)              AS league_id,
        CAST(season AS INTEGER)                 AS season,
        CAST(team_id AS INTEGER)                AS team_id,

        -- Team info
        UPPER(TRIM(team_name))                  AS team_name,

        -- Standing metrics
        CAST(rank AS INTEGER)                   AS rank,
        CAST(points AS INTEGER)                 AS points,
        CAST(played AS INTEGER)                 AS matches_played,
        CAST(win AS INTEGER)                    AS wins,
        CAST(draw AS INTEGER)                   AS draws,
        CAST(lose AS INTEGER)                   AS losses,
        CAST(goals_for AS INTEGER)              AS goals_for,
        CAST(goals_against AS INTEGER)          AS goals_against,
        CAST(goal_diff AS INTEGER)              AS goal_difference,

        -- Form string — last 5 results e.g. WWDLD
        UPPER(TRIM(form))                       AS form,

        -- Win percentage derived column
        ROUND(
            CAST(win AS FLOAT) / NULLIF(CAST(played AS FLOAT), 0) * 100,
            2
        )                                       AS win_pct,

        -- Metadata
        _loaded_at                              AS _loaded_at

    FROM source
    WHERE team_id IS NOT NULL
      AND league_id IS NOT NULL
      AND snapshot_date IS NOT NULL
)

SELECT * FROM cleaned
