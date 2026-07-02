"""
summarizer.py — Projet 3 Sentiment & Tendances — Groupe 3
Responsable : Meissa MARA (Personne 4 — Gold + Dashboard Superset)

Rôle :
  Génère des résumés narratifs de tendances par heure/match à partir des
  agrégats Gold (PostgreSQL) en appelant le LLM Qwen3:8b local via Ollama.
  Les résumés sont stockés dans la table `trend_summaries` (PostgreSQL).

Usage autonome :
    python summarizer.py                           # traite toutes les heures non résumées
    python summarizer.py --hour 2026-07-01T14:00:00+00:00
    python summarizer.py --match wc2026-fr-pt      # toutes les heures d'un match

Appelé depuis le DAG Airflow via run_summaries_for_partition().
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

PG_DSN = os.getenv(
    "PG_DSN",
    "postgresql://app:app12345@postgres:5432/gold",
)

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL", "qwen3:8b")
OLLAMA_TIMEOUT  = int(os.getenv("OLLAMA_TIMEOUT", "120"))


# ── Prompt engineering ────────────────────────────────────────────────────────

def _build_summary_prompt(
    hour_bucket: datetime,
    match_id: Optional[str],
    team_home: Optional[str],
    team_away: Optional[str],
    total_posts: int,
    avg_sentiment: Optional[float],
    positive_count: int,
    neutral_count: int,
    negative_count: int,
    top_topics: list[dict],
    match_events: list[dict],
) -> str:
    hour_str = hour_bucket.strftime("%d/%m/%Y à %Hh")
    match_str = (
        f"{team_home} vs {team_away} (match ID : {match_id})"
        if match_id
        else "Hors match"
    )
    sentiment_desc = "neutre"
    if avg_sentiment is not None:
        if avg_sentiment >= 0.65:
            sentiment_desc = "globalement positif"
        elif avg_sentiment <= 0.35:
            sentiment_desc = "globalement négatif"

    topics_str = ", ".join(t["topic"] for t in top_topics[:5]) if top_topics else "non disponibles"

    events_lines = ""
    if match_events:
        events_lines = "\nÉvénements survenus sur cette tranche horaire :\n"
        for ev in match_events:
            events_lines += (
                f"  - {ev.get('event_type','?').upper()} "
                f"(min {ev.get('event_minute','?')}) : {ev.get('description','')}\n"
            )

    return (
        f"Tu es un analyste sportif spécialisé en data journalism.\n"
        f"Voici les données sociales pour la tranche horaire {hour_str} — {match_str} :\n\n"
        f"  • {total_posts} posts analysés\n"
        f"  • Sentiment {sentiment_desc} (score moyen : {avg_sentiment:.2f})\n"
        f"  • Répartition : {positive_count} positifs, "
        f"{neutral_count} neutres, {negative_count} négatifs\n"
        f"  • Sujets principaux : {topics_str}\n"
        f"{events_lines}\n"
        f"Rédige en 3 à 5 phrases un résumé journalistique concis des tendances "
        f"sur les réseaux sociaux pour cette période. "
        f"Mentionne les sujets dominants et l'ambiance générale des supporters. "
        f"Réponds uniquement en français, sans preamble ni formatage Markdown."
    )


# ── Appel Ollama ──────────────────────────────────────────────────────────────

def call_ollama(prompt: str) -> str:
    """Envoie le prompt à Ollama et retourne le texte généré."""
    url = f"{OLLAMA_BASE_URL}/api/generate"
    payload = {
        "model":  OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.7, "num_predict": 300},
    }
    try:
        resp = requests.post(url, json=payload, timeout=OLLAMA_TIMEOUT)
        resp.raise_for_status()
        return resp.json().get("response", "").strip()
    except requests.RequestException as exc:
        log.error("Erreur Ollama : %s", exc)
        return ""


# ── Lecture PostgreSQL ────────────────────────────────────────────────────────

def fetch_unsummarized_rows(
    conn,
    hour: Optional[datetime] = None,
    match_id: Optional[str] = None,
) -> list[dict]:
    """
    Récupère les agrégats hourly_sentiment qui n'ont pas encore de résumé
    dans trend_summaries.
    """
    where_clauses = [
        "NOT EXISTS ("
        "  SELECT 1 FROM trend_summaries ts"
        "  WHERE ts.match_id = hs.match_id"
        "    AND ts.hour_bucket = hs.hour_bucket"
        ")",
        "hs.total_posts > 0",
    ]
    params: list = []

    if hour is not None:
        where_clauses.append("hs.hour_bucket = %s")
        params.append(hour)
    if match_id is not None:
        where_clauses.append("hs.match_id = %s")
        params.append(match_id)

    where_str = " AND ".join(where_clauses)

    sql = f"""
        SELECT
            hs.hour_bucket,
            hs.match_id,
            hs.team_home,
            hs.team_away,
            hs.total_posts,
            hs.avg_sentiment_score,
            hs.positive_count,
            hs.neutral_count,
            hs.negative_count,
            hs.top_topics
        FROM hourly_sentiment hs
        WHERE {where_str}
        ORDER BY hs.hour_bucket
    """

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def fetch_events_for_hour(conn, match_id: str, hour: datetime) -> list[dict]:
    """Récupère les événements sportifs (buts, cartons...) sur une tranche horaire."""
    if not match_id:
        return []
    sql = """
        SELECT event_type, event_minute, description
        FROM match_events_gold
        WHERE match_id = %s
          AND event_timestamp >= %s
          AND event_timestamp <  %s
        ORDER BY event_minute
    """
    end = hour + timedelta(hours=1)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (match_id, hour, end))
        return [dict(r) for r in cur.fetchall()]


def upsert_summary(conn, row: dict, summary: str) -> None:
    """Insère ou met à jour un résumé dans trend_summaries."""
    sql = """
        INSERT INTO trend_summaries
            (match_id, hour_bucket, summary, llm_model, top_topics,
             avg_sentiment, post_count, generated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (match_id, hour_bucket) DO UPDATE
            SET summary       = EXCLUDED.summary,
                llm_model     = EXCLUDED.llm_model,
                avg_sentiment = EXCLUDED.avg_sentiment,
                post_count    = EXCLUDED.post_count,
                generated_at  = NOW()
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                row["match_id"],
                row["hour_bucket"],
                summary,
                OLLAMA_MODEL,
                json.dumps(row.get("top_topics") or [], ensure_ascii=False)
                if isinstance(row.get("top_topics"), list)
                else row.get("top_topics"),
                row.get("avg_sentiment_score"),
                row.get("total_posts"),
            ),
        )
    conn.commit()


# ── Pipeline principal ────────────────────────────────────────────────────────

def process_rows(rows: list[dict], conn) -> int:
    """Génère et stocke les résumés pour une liste de lignes hourly_sentiment."""
    if not rows:
        log.info("Aucun agrégat sans résumé trouvé.")
        return 0

    generated = 0
    for row in rows:
        hour: datetime = row["hour_bucket"]
        match_id: Optional[str] = row.get("match_id")
        top_topics_raw = row.get("top_topics")

        # Désérialise top_topics si c'est une string JSON (psycopg2 JSONB → dict/list)
        if isinstance(top_topics_raw, str):
            try:
                top_topics = json.loads(top_topics_raw)
            except (ValueError, TypeError):
                top_topics = []
        elif isinstance(top_topics_raw, list):
            top_topics = top_topics_raw
        else:
            top_topics = []

        # Récupère les événements sportifs sur cette tranche
        match_events = fetch_events_for_hour(conn, match_id or "", hour)

        prompt = _build_summary_prompt(
            hour_bucket=hour,
            match_id=match_id,
            team_home=row.get("team_home"),
            team_away=row.get("team_away"),
            total_posts=row.get("total_posts", 0),
            avg_sentiment=row.get("avg_sentiment_score"),
            positive_count=row.get("positive_count", 0),
            neutral_count=row.get("neutral_count", 0),
            negative_count=row.get("negative_count", 0),
            top_topics=top_topics,
            match_events=match_events,
        )

        log.info(
            "Génération résumé Ollama : match=%s heure=%s (%d posts)",
            match_id,
            hour.isoformat(),
            row.get("total_posts", 0),
        )
        summary = call_ollama(prompt)

        if not summary:
            log.warning("Résumé vide pour match=%s heure=%s — ignoré.", match_id, hour)
            continue

        upsert_summary(conn, row, summary)
        generated += 1
        log.info("Résumé stocké : match=%s heure=%s", match_id, hour.isoformat())

    return generated


def run_summaries_for_partition(
    year: int,
    month: int,
    day: int,
    hour: int,
) -> int:
    """
    Point d'entrée appelé par le DAG Airflow.
    Génère les résumés pour la partition horaire donnée.
    """
    target_hour = datetime(year, month, day, hour, tzinfo=timezone.utc)
    conn = psycopg2.connect(PG_DSN)
    try:
        rows = fetch_unsummarized_rows(conn, hour=target_hour)
        return process_rows(rows, conn)
    finally:
        conn.close()


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Summarizer Gold : génère des résumés Ollama depuis PostgreSQL."
    )
    parser.add_argument(
        "--hour",
        help="ISO 8601 de l'heure à résumer (ex: 2026-07-01T14:00:00+00:00). "
             "Par défaut : toutes les heures sans résumé.",
    )
    parser.add_argument(
        "--match",
        help="Filtre par match_id (ex: wc2026-fr-pt).",
    )
    args = parser.parse_args()

    hour_filter: Optional[datetime] = None
    if args.hour:
        hour_filter = datetime.fromisoformat(args.hour.replace("Z", "+00:00"))
        if hour_filter.tzinfo is None:
            hour_filter = hour_filter.replace(tzinfo=timezone.utc)

    conn = psycopg2.connect(PG_DSN)
    try:
        rows = fetch_unsummarized_rows(conn, hour=hour_filter, match_id=args.match)
        count = process_rows(rows, conn)
        log.info("Total résumés générés : %d", count)
    finally:
        conn.close()
