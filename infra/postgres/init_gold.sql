-- ============================================================
-- init_gold.sql — Tables Gold PostgreSQL
-- Projet 3 Sentiment & Tendances — Groupe 3
-- Responsable : Meissa MARA (Personne 4 — Gold + Dashboard)
-- ============================================================
-- Ce script est exécuté automatiquement par PostgreSQL au
-- premier démarrage du conteneur (via /docker-entrypoint-initdb.d/).
-- ============================================================

-- ──────────────────────────────────────────────────────────────
-- 1. Événements de match (miroir du CSV data/match_events.csv)
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS match_events_gold (
    match_id        TEXT          NOT NULL,
    event_type      TEXT          NOT NULL,  -- goal | red_card | kickoff | penalty | var | halftime | fulltime
    event_minute    INTEGER       NOT NULL,
    event_timestamp TIMESTAMPTZ   NOT NULL,
    team_home       TEXT          NOT NULL,
    team_away       TEXT          NOT NULL,
    description     TEXT,
    PRIMARY KEY (match_id, event_type, event_minute)
);

COMMENT ON TABLE match_events_gold IS
    'Événements sportifs (buts, cartons, ...) utilisés pour corréler la timeline du sentiment.';

-- ──────────────────────────────────────────────────────────────
-- 2. Agrégats horaires de sentiment (table principale Gold)
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS hourly_sentiment (
    id                  SERIAL        PRIMARY KEY,
    hour_bucket         TIMESTAMPTZ   NOT NULL,  -- trunc à l'heure (DATE_TRUNC('hour', ...)
    match_id            TEXT,
    team_home           TEXT,
    team_away           TEXT,
    total_posts         INTEGER       NOT NULL DEFAULT 0,
    positive_count      INTEGER       NOT NULL DEFAULT 0,
    neutral_count       INTEGER       NOT NULL DEFAULT 0,
    negative_count      INTEGER       NOT NULL DEFAULT 0,
    avg_sentiment_score DOUBLE PRECISION,        -- moyenne sentiment_score ∈ [0,1]
    positive_ratio      DOUBLE PRECISION         -- positive_count / total_posts
        GENERATED ALWAYS AS (
            CASE WHEN total_posts > 0
                 THEN CAST(positive_count AS DOUBLE PRECISION) / total_posts
                 ELSE NULL END
        ) STORED,
    negative_ratio      DOUBLE PRECISION
        GENERATED ALWAYS AS (
            CASE WHEN total_posts > 0
                 THEN CAST(negative_count AS DOUBLE PRECISION) / total_posts
                 ELSE NULL END
        ) STORED,
    top_topics          JSONB,                   -- [{topic_label, count}, ...]
    created_at          TIMESTAMPTZ   DEFAULT NOW(),
    updated_at          TIMESTAMPTZ   DEFAULT NOW(),
    UNIQUE (hour_bucket, COALESCE(match_id, ''))
);

CREATE INDEX IF NOT EXISTS idx_hourly_sentiment_hour ON hourly_sentiment (hour_bucket);
CREATE INDEX IF NOT EXISTS idx_hourly_sentiment_match ON hourly_sentiment (match_id);
CREATE INDEX IF NOT EXISTS idx_hourly_sentiment_hour_match ON hourly_sentiment (hour_bucket, match_id);

COMMENT ON TABLE hourly_sentiment IS
    'Agrégats horaires : sentiment moyen, volumes, top topics par tranche horaire et match.';

-- ──────────────────────────────────────────────────────────────
-- 3. Heatmap du sentiment par équipe (par heure et par match)
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS team_sentiment_heatmap (
    id                  SERIAL        PRIMARY KEY,
    match_id            TEXT          NOT NULL,
    team                TEXT          NOT NULL,
    hour_bucket         TIMESTAMPTZ   NOT NULL,
    post_count          INTEGER       NOT NULL DEFAULT 0,
    avg_sentiment_score DOUBLE PRECISION,
    positive_ratio      DOUBLE PRECISION,
    negative_ratio      DOUBLE PRECISION,
    created_at          TIMESTAMPTZ   DEFAULT NOW(),
    UNIQUE (match_id, team, hour_bucket)
);

CREATE INDEX IF NOT EXISTS idx_heatmap_match_team ON team_sentiment_heatmap (match_id, team);
CREATE INDEX IF NOT EXISTS idx_heatmap_hour ON team_sentiment_heatmap (hour_bucket);

COMMENT ON TABLE team_sentiment_heatmap IS
    'Heatmap du sentiment par équipe × heure, pour le dashboard Superset.';

-- ──────────────────────────────────────────────────────────────
-- 4. Résumés de tendances générés par le LLM (Qwen/Ollama)
-- ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trend_summaries (
    id             SERIAL        PRIMARY KEY,
    match_id       TEXT          NOT NULL,
    hour_bucket    TIMESTAMPTZ   NOT NULL,
    summary        TEXT          NOT NULL,
    llm_model      TEXT          NOT NULL DEFAULT 'qwen3:8b',
    top_topics     JSONB,
    avg_sentiment  DOUBLE PRECISION,
    post_count     INTEGER,
    generated_at   TIMESTAMPTZ   DEFAULT NOW(),
    UNIQUE (match_id, hour_bucket)
);

CREATE INDEX IF NOT EXISTS idx_summaries_match ON trend_summaries (match_id);
CREATE INDEX IF NOT EXISTS idx_summaries_hour ON trend_summaries (hour_bucket);

COMMENT ON TABLE trend_summaries IS
    'Résumés narratifs des tendances générés par Qwen3:8b via Ollama.';

-- ──────────────────────────────────────────────────────────────
-- 5. Vue analytique prête pour Superset
--    Sentiment vs Timeline des buts (JOIN hourly_sentiment × match_events_gold)
-- ──────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_sentiment_vs_events AS
SELECT
    hs.hour_bucket,
    hs.match_id,
    hs.team_home,
    hs.team_away,
    hs.total_posts,
    hs.avg_sentiment_score,
    hs.positive_ratio,
    hs.negative_ratio,
    hs.top_topics,
    me.event_type,
    me.event_minute,
    me.description    AS event_description,
    me.event_timestamp
FROM hourly_sentiment hs
LEFT JOIN match_events_gold me
       ON hs.match_id = me.match_id
      AND me.event_timestamp >= hs.hour_bucket
      AND me.event_timestamp <  hs.hour_bucket + INTERVAL '1 hour';

COMMENT ON VIEW v_sentiment_vs_events IS
    'Joint les agrégats horaires avec les événements sportifs pour le graphique sentiment vs timeline.';
