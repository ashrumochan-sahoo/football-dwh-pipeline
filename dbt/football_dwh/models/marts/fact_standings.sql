-- ============================================================
-- Model: fact_standings
-- Layer: Marts
-- Purpose: Fact table for daily league standings snapshots
--          One row per team per league per snapshot date
--          Tracks how standings change over time
-- Materialization: Table
-- ============================================================

WITH standings AS (
    SELECT * FROM {{ ref('stg_standings') }}
),

teams AS (
    SELECT * FROM {{ ref('dim_teams') }}
),

final AS (
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key([
            's.snapshot_date',
            's.team_id',
            's.league_id'
        ]) }}                           AS standing_id,

        -- Foreign keys
        s.team_id,
        s.league_id,
        s.snapshot_date,
        s.season,

        -- Team context from dim
        t.team_name,
        t.league_name,
        t.country,

        -- Standing metrics
        s.rank,
        s.points,
        s.matches_played,
        s.wins,
        s.draws,
        s.losses,
        s.goals_for,
        s.goals_against,
        s.goal_difference,
        s.form,
        s.win_pct,

        -- Metadata
        s._loaded_at

    FROM standings s
    LEFT JOIN teams t
        ON  s.team_id   = t.team_id
        AND s.league_id = t.league_id
)

SELECT * FROM final
