"""
gold_aggregates.py — Projet 3 Sentiment & Tendances — Groupe 3
Responsable : Meissa MARA (Personne 4 — Gold + Dashboard Superset)

Rôle :
  Lit les posts enrichis depuis MongoDB (collection `enriched_posts`, couche NLP)
  et construit les agrégats horaires Gold dans PostgreSQL :
    • hourly_sentiment   — volume, sentiment moyen, top topics, ratios par heure/match
    • team_sentiment_heatmap — idem, ventilé par équipe mentionnée dans le post

Usage autonome :
    python gold_aggregates.py              # traite l'heure N-1 (par défaut)
    python gold_aggregates.py --backfill   # retraite toutes les heures disponibles
    python gold_aggregates.py --hour 2026-07-01T14:00:00+00:00

Appelé depuis le DAG Airflow via run_gold_for_partition().
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
from pymongo import MongoClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://app:app12345@mongo:27017/?authSource=admin",
)
MONGO_DB = os.getenv("MONGO_DB", "sentiment")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "enriched_posts")

PG_DSN = os.getenv(
    "PG_DSN",
    "postgresql://app:app12345@postgres:5432/gold",
)

MATCH_EVENTS_PATH = os.getenv(
    "MATCH_EVENTS_PATH",
    "/opt/airflow/data/match_events.csv",
)
TOP_TOPICS_K = int(os.getenv("TOP_TOPICS_K", "5"))


# ── Helpers ───────────────────────────────────────────────────────────────────

def _hour_bucket(dt: datetime) -> datetime:
    """Tronque un datetime à l'heure (UTC)."""
    return dt.replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)


def _parse_iso(value: str | None) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, AttributeError):
        return None


# ── Match events ──────────────────────────────────────────────────────────────

def load_match_events(path: str) -> list[dict]:
    """Charge le CSV d'événements match (Personne 1) et retourne une liste de dicts."""
    if not path or not os.path.isfile(path):
        log.warning("match_events.csv introuvable : %s", path)
        return []
    events: list[dict] = []
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            events.append(row)
    log.info("Événements match chargés : %d", len(events))
    return events


def upsert_match_events(conn, events: list[dict]) -> None:
    """Miroir du CSV dans la table match_events_gold pour les JOINs SQL."""
    if not events:
        return
    rows = []
    for ev in events:
        ts = _parse_iso(ev.get("event_timestamp"))
        if ts is None:
            continue
        rows.append((
            ev.get("match_id"),
            ev.get("event_type"),
            int(ev.get("event_minute", 0)),
            ts,
            ev.get("team_home"),
            ev.get("team_away"),
            ev.get("description"),
        ))
    if not rows:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO match_events_gold
                (match_id, event_type, event_minute, event_timestamp,
                 team_home, team_away, description)
            VALUES %s
            ON CONFLICT (match_id, event_type, event_minute) DO UPDATE
                SET event_timestamp = EXCLUDED.event_timestamp,
                    description     = EXCLUDED.description
            """,
            rows,
        )
    conn.commit()
    log.info("match_events_gold : %d événements upsertés", len(rows))


# ── MongoDB → agrégats ────────────────────────────────────────────────────────

def fetch_posts_for_hour(
    mongo_coll,
    hour: datetime,
) -> list[dict]:
    """Récupère tous les posts dont la partition horaire correspond à `hour`."""
    start = hour
    end = hour + timedelta(hours=1)
    # Les posts NLP ont un champ `nlp_processed_at` ou `collected_at`
    cursor = mongo_coll.find(
        {
            "$or": [
                {
                    "partition.year":  hour.year,
                    "partition.month": hour.month,
                    "partition.day":   hour.day,
                    "partition.hour":  hour.hour,
                },
                {
                    "collected_at": {
                        "$gte": start.isoformat(),
                        "$lt":  end.isoformat(),
                    }
                },
            ]
        },
        {
            "_id": 0,
            "id": 1,
            "sentiment_label": 1,
            "sentiment_score": 1,
            "topic_label": 1,
            "topic_keywords": 1,
            "match_id": 1,
            "team_home": 1,
            "team_away": 1,
            "collected_at": 1,
        },
    )
    posts = list(cursor)
    log.info("MongoDB : %d posts récupérés pour l'heure %s", len(posts), hour.isoformat())
    return posts


def compute_hourly_aggregates(
    hour: datetime,
    posts: list[dict],
) -> list[dict]:
    """
    Calcule les agrégats horaires depuis la liste de posts.

    Retourne une liste de dicts (un par match_id distinct, plus un None = global).
    """
    if not posts:
        return []

    # Grouper par match_id (None = pas de match associé)
    groups: dict[Optional[str], list[dict]] = defaultdict(list)
    for post in posts:
        groups[post.get("match_id")].append(post)

    results = []
    for match_id, group in groups.items():
        total = len(group)
        positive = sum(1 for p in group if p.get("sentiment_label") == "positive")
        neutral  = sum(1 for p in group if p.get("sentiment_label") == "neutral")
        negative = sum(1 for p in group if p.get("sentiment_label") == "negative")

        scores = [
            float(p["sentiment_score"])
            for p in group
            if p.get("sentiment_score") is not None
        ]
        avg_score = sum(scores) / len(scores) if scores else None

        # Top topics (topic_label le plus fréquent)
        topic_counter: Counter = Counter()
        for p in group:
            lbl = p.get("topic_label")
            if lbl and lbl.strip():
                topic_counter[lbl.strip()] += 1
        top_topics = [
            {"topic": t, "count": c}
            for t, c in topic_counter.most_common(TOP_TOPICS_K)
        ]

        # team_home / team_away à partir du premier post qui les a
        team_home = team_away = None
        for p in group:
            if p.get("team_home"):
                team_home = p["team_home"]
                team_away = p.get("team_away")
                break

        results.append(
            {
                "hour_bucket":         hour,
                "match_id":            match_id,
                "team_home":           team_home,
                "team_away":           team_away,
                "total_posts":         total,
                "positive_count":      positive,
                "neutral_count":       neutral,
                "negative_count":      negative,
                "avg_sentiment_score": avg_score,
                "top_topics":          json.dumps(top_topics, ensure_ascii=False),
            }
        )

    return results


def compute_team_heatmap(
    hour: datetime,
    posts: list[dict],
) -> list[dict]:
    """
    Construit la heatmap par équipe : pour chaque post qui mentionne une équipe
    (team_home ou team_away), on l'associe à l'équipe et on calcule sentiment moyen.
    """
    if not posts:
        return []

    # Clé : (match_id, team)
    team_groups: dict[tuple, list[dict]] = defaultdict(list)
    for p in posts:
        match_id = p.get("match_id")
        if not match_id:
            continue
        for team_field in ("team_home", "team_away"):
            team = p.get(team_field)
            if team:
                team_groups[(match_id, team)].append(p)

    results = []
    for (match_id, team), group in team_groups.items():
        total = len(group)
        positive = sum(1 for p in group if p.get("sentiment_label") == "positive")
        negative = sum(1 for p in group if p.get("sentiment_label") == "negative")
        scores = [
            float(p["sentiment_score"])
            for p in group
            if p.get("sentiment_score") is not None
        ]
        avg_score = sum(scores) / len(scores) if scores else None
        results.append(
            {
                "match_id":            match_id,
                "team":                team,
                "hour_bucket":         hour,
                "post_count":          total,
                "avg_sentiment_score": avg_score,
                "positive_ratio":      positive / total if total else None,
                "negative_ratio":      negative / total if total else None,
            }
        )

    return results


# ── PostgreSQL upserts ────────────────────────────────────────────────────────

def upsert_hourly_sentiment(conn, rows: list[dict]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO hourly_sentiment
                (hour_bucket, match_id, team_home, team_away,
                 total_posts, positive_count, neutral_count, negative_count,
                 avg_sentiment_score, top_topics, updated_at)
            VALUES %s
            ON CONFLICT (hour_bucket, match_id)
            DO UPDATE SET
                team_home           = EXCLUDED.team_home,
                team_away           = EXCLUDED.team_away,
                total_posts         = EXCLUDED.total_posts,
                positive_count      = EXCLUDED.positive_count,
                neutral_count       = EXCLUDED.neutral_count,
                negative_count      = EXCLUDED.negative_count,
                avg_sentiment_score = EXCLUDED.avg_sentiment_score,
                top_topics          = EXCLUDED.top_topics,
                updated_at          = NOW()
            """,
            [
                (
                    r["hour_bucket"],
                    r["match_id"] or "",
                    r["team_home"],
                    r["team_away"],
                    r["total_posts"],
                    r["positive_count"],
                    r["neutral_count"],
                    r["negative_count"],
                    r["avg_sentiment_score"],
                    r["top_topics"],
                    datetime.now(tz=timezone.utc),
                )
                for r in rows
            ],
        )
    conn.commit()
    log.info("hourly_sentiment : %d lignes upsertées", len(rows))


def upsert_team_heatmap(conn, rows: list[dict]) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO team_sentiment_heatmap
                (match_id, team, hour_bucket, post_count,
                 avg_sentiment_score, positive_ratio, negative_ratio)
            VALUES %s
            ON CONFLICT (match_id, team, hour_bucket)
            DO UPDATE SET
                post_count          = EXCLUDED.post_count,
                avg_sentiment_score = EXCLUDED.avg_sentiment_score,
                positive_ratio      = EXCLUDED.positive_ratio,
                negative_ratio      = EXCLUDED.negative_ratio
            """,
            [
                (
                    r["match_id"],
                    r["team"],
                    r["hour_bucket"],
                    r["post_count"],
                    r["avg_sentiment_score"],
                    r["positive_ratio"],
                    r["negative_ratio"],
                )
                for r in rows
            ],
        )
    conn.commit()
    log.info("team_sentiment_heatmap : %d lignes upsertées", len(rows))


# ── Point d'entrée principal ──────────────────────────────────────────────────

def run_gold_for_partition(
    year: int,
    month: int,
    day: int,
    hour: int,
) -> int:
    """
    Traite une partition horaire : lit MongoDB, calcule les agrégats Gold,
    upserte dans PostgreSQL.
    Retourne le nombre de posts traités.
    Appelé par le DAG Airflow.
    """
    target_hour = datetime(year, month, day, hour, tzinfo=timezone.utc)

    mongo_client = MongoClient(MONGO_URI)
    pg_conn = psycopg2.connect(PG_DSN)

    try:
        coll = mongo_client[MONGO_DB][MONGO_COLLECTION]
        posts = fetch_posts_for_hour(coll, target_hour)

        if not posts:
            log.info("Aucun post MongoDB pour %s — Gold ignoré.", target_hour.isoformat())
            return 0

        hourly_rows = compute_hourly_aggregates(target_hour, posts)
        heatmap_rows = compute_team_heatmap(target_hour, posts)

        # Miroir match_events dans PG (idempotent)
        events = load_match_events(MATCH_EVENTS_PATH)
        upsert_match_events(pg_conn, events)

        upsert_hourly_sentiment(pg_conn, hourly_rows)
        upsert_team_heatmap(pg_conn, heatmap_rows)

        return len(posts)
    finally:
        mongo_client.close()
        pg_conn.close()


def backfill_all_hours() -> None:
    """
    Retraite toutes les heures disponibles dans MongoDB.
    Utile pour rejouer le pipeline Gold depuis le début.
    """
    mongo_client = MongoClient(MONGO_URI)
    coll = mongo_client[MONGO_DB][MONGO_COLLECTION]

    # Récupère toutes les partitions distinctes présentes en MongoDB
    pipeline = [
        {
            "$group": {
                "_id": {
                    "year":  "$partition.year",
                    "month": "$partition.month",
                    "day":   "$partition.day",
                    "hour":  "$partition.hour",
                }
            }
        },
        {"$sort": {"_id.year": 1, "_id.month": 1, "_id.day": 1, "_id.hour": 1}},
    ]
    partitions = list(coll.aggregate(pipeline))
    mongo_client.close()

    log.info("Backfill Gold : %d partitions trouvées dans MongoDB.", len(partitions))
    for p in partitions:
        pid = p["_id"]
        if not all(pid.get(k) is not None for k in ("year", "month", "day", "hour")):
            continue
        run_gold_for_partition(pid["year"], pid["month"], pid["day"], pid["hour"])


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Gold aggregates : MongoDB → PostgreSQL"
    )
    parser.add_argument(
        "--hour",
        help="ISO 8601 de l'heure à traiter (ex: 2026-07-01T14:00:00+00:00). "
             "Par défaut : heure précédente.",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Retraite toutes les heures disponibles dans MongoDB.",
    )
    args = parser.parse_args()

    if args.backfill:
        backfill_all_hours()
    else:
        if args.hour:
            target = _parse_iso(args.hour)
            if target is None:
                raise ValueError(f"Format d'heure invalide : {args.hour}")
        else:
            now = datetime.now(tz=timezone.utc)
            target = _hour_bucket(now - timedelta(hours=1))

        run_gold_for_partition(
            target.year, target.month, target.day, target.hour
        )
