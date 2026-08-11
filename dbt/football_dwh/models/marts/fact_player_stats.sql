-- ============================================================
-- Model: fact_player_stats
-- Layer: Marts
-- Purpose: Fact table for player performance statistics
--          One row per player per league per snapshot date
--          Joined with dim_teams for team context
-- Materialization: Table
-- ============================================================

WITH players AS (
    SELECT * FROM {{ ref('stg_players') }}
),

teams AS (
    SELECT * FROM {{ ref('dim_teams') }}
),

final AS (
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key([
            'p.snapshot_date',
            'p.player_id',
            'p.league_id'
        ]) }}                           AS stat_id,

        -- Foreign keys
        p.player_id,
        p.team_id,
        p.league_id,
        p.snapshot_date,

        -- Player attributes
        p.player_name,
        p.player_age,
        p.nationality,
        p.season,

        -- Team context from dim
        t.team_name,
        t.league_name,
        t.country,

        -- Performance metrics
        p.appearances,
        p.goals,
        p.assists,
        p.yellow_cards,
        p.red_cards,
        p.rating,

        -- Derived metrics
        p.goal_contributions,
        p.goals_per_game,

        -- Metadata
        p._loaded_at

    FROM players p
    LEFT JOIN teams t
        ON  p.team_id   = t.team_id
        AND p.league_id = t.league_id
)

SELECT * FROM final
