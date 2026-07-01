"""
NLP pipeline — Projet 3 Sentiment & Tendances — Groupe 3 (Hedi Mathlouthi)
Silver → Sentiment (Qwen3:8b via Ollama) + Topics (BERTopic) → MongoDB

Lit les fichiers Parquet Silver depuis MinIO, enrichit chaque post avec
sentiment et sujet, puis upsert dans MongoDB.
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Optional

import pyarrow.parquet as pq
import pymongo
import requests
import s3fs
from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio12345")
SILVER_BUCKET = "silver"

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://app:app12345@mongo:27017/?authSource=admin",
)
MONGO_DB = os.getenv("MONGO_DB", "sentiment")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "enriched_posts")
MONGO_AGGREGATES_COLLECTION = os.getenv("MONGO_AGGREGATES_COLLECTION", "topic_aggregates")
TOP_TOPICS_K = int(os.getenv("TOP_TOPICS_K", "5"))
MATCH_WINDOW_MINUTES = int(os.getenv("MATCH_WINDOW_MINUTES", "120"))

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen3:8b")
OLLAMA_BATCH_SIZE = int(os.getenv("OLLAMA_BATCH_SIZE", "6"))
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", "180"))

MATCH_EVENTS_PATH = os.getenv("MATCH_EVENTS_PATH", "/opt/airflow/data/match_events.csv")
MIN_DOCS_FOR_BERTOPIC = int(os.getenv("MIN_DOCS_FOR_BERTOPIC", "5"))

SENTIMENT_LABELS = ("positive", "neutral", "negative")


# ── MinIO ─────────────────────────────────────────────────────────────────────

def get_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        endpoint_url=MINIO_ENDPOINT,
        use_ssl=False,
    )


def silver_path(year: int, month: int, day: int, hour: int) -> str:
    return (
        f"{SILVER_BUCKET}/social/"
        f"year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}/data.parquet"
    )


def read_silver_partition(
    year: int, month: int, day: int, hour: int, fs: s3fs.S3FileSystem
) -> list[dict]:
    path = silver_path(year, month, day, hour)
    try:
        table = pq.read_table(path, filesystem=fs)
    except Exception as exc:
        log.warning("Impossible de lire Silver %s : %s", path, exc)
        return []
    records = table.to_pylist()
    log.info("Silver : %d docs lus depuis %s", len(records), path)
    return records


# ── Corrélation match (optionnelle) ───────────────────────────────────────────

def load_match_events(path: str) -> list[dict]:
    """Charge un CSV d'événements match si disponible (Personne 4)."""
    if not path or not os.path.isfile(path):
        return []
    import csv

    events: list[dict] = []
    with open(path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            events.append(row)
    log.info("Événements match chargés : %d depuis %s", len(events), path)
    return events


def _parse_iso(value: str) -> Optional[datetime]:
    if not value:
        return None
    value = value.strip()
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


def nearest_match_event(
    collected_at: str, events: list[dict], window_minutes: int = MATCH_WINDOW_MINUTES
) -> Optional[dict]:
    """Associe un post à l'événement sportif le plus proche (corrélation, pas entraînement)."""
    post_dt = _parse_iso(collected_at)
    if post_dt is None or not events:
        return None

    best: Optional[tuple[float, dict]] = None
    for event in events:
        event_ts = event.get("event_timestamp") or event.get("match_timestamp")
        event_dt = _parse_iso(str(event_ts or ""))
        if event_dt is None:
            continue
        delta_min = abs((post_dt - event_dt).total_seconds()) / 60.0
        if delta_min <= window_minutes and (best is None or delta_min < best[0]):
            best = (
                delta_min,
                {
                    "match_id": event.get("match_id"),
                    "match_timestamp": event_dt.isoformat(),
                    "event_type": event.get("event_type"),
                    "event_minute": event.get("event_minute"),
                    "team_home": event.get("team_home"),
                    "team_away": event.get("team_away"),
                    "event_description": event.get("description"),
                },
            )

    return best[1] if best else None


# ── Sentiment via Qwen3:8b (Ollama) ───────────────────────────────────────────

def _build_sentiment_prompt(texts: list[str]) -> str:
    numbered = "\n".join(f'{i}. """{t[:400]}"""' for i, t in enumerate(texts))
    return f"""Tu es un classificateur de sentiment multilingue pour des posts sportifs (Coupe du Monde).
Pour chaque texte, retourne UNIQUEMENT un JSON valide de la forme :
{{"results": [{{"index": 0, "label": "positive|neutral|negative", "score": 0.85}}]}}

Règles :
- label ∈ positive, neutral, negative
- score ∈ [0.0, 1.0] = confiance
- Contexte : réactions supporters, actualités, polémiques arbitrales
- Ne produis aucun texte hors JSON

Textes :
{numbered}
"""


def _parse_sentiment_response(raw: str, batch_size: int) -> list[dict]:
    """Parse la réponse Ollama ; fallback neutral si JSON invalide."""
    fallback = [
        {"label": "neutral", "score": 0.0, "method": "qwen3:8b_fallback"}
        for _ in range(batch_size)
    ]
    if not raw:
        return fallback

    cleaned = raw.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)

    try:
        payload = json.loads(cleaned)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", cleaned, re.DOTALL)
        if not match:
            log.warning("Réponse Ollama non parseable : %s", raw[:200])
            return fallback
        try:
            payload = json.loads(match.group())
        except json.JSONDecodeError:
            return fallback

    results = payload.get("results", payload if isinstance(payload, list) else [])
    by_index: dict[int, dict] = {}
    for item in results:
        idx = int(item.get("index", len(by_index)))
        label = str(item.get("label", "neutral")).lower().strip()
        if label not in SENTIMENT_LABELS:
            label = "neutral"
        score = float(item.get("score", 0.5))
        score = max(0.0, min(1.0, score))
        by_index[idx] = {
            "label": label,
            "score": score,
            "method": "qwen3:8b",
        }

    output: list[dict] = []
    for i in range(batch_size):
        output.append(by_index.get(i, fallback[0]))
    return output


def classify_sentiment_batch(texts: list[str]) -> list[dict]:
    """Classifie un lot de textes via Ollama (qwen3:8b)."""
    if not texts:
        return []

    prompt = _build_sentiment_prompt(texts)
    url = f"{OLLAMA_BASE_URL.rstrip('/')}/api/generate"
    try:
        response = requests.post(
            url,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "format": "json",
                "think": False,
                "options": {"temperature": 0.1, "num_predict": 512},
            },
            timeout=OLLAMA_TIMEOUT,
        )
        response.raise_for_status()
        raw = response.json().get("response", "")
    except requests.RequestException as exc:
        log.error("Erreur Ollama (%s) : %s", OLLAMA_MODEL, exc)
        return [
            {"label": "neutral", "score": 0.0, "method": "qwen3:8b_error"}
            for _ in texts
        ]

    return _parse_sentiment_response(raw, len(texts))


def classify_all_sentiments(records: list[dict]) -> list[dict]:
    """Classifie tous les posts par lots."""
    texts = [
        (r.get("text_normalized") or r.get("text") or "").strip()
        for r in records
    ]
    sentiments: list[dict] = []

    for start in range(0, len(texts), OLLAMA_BATCH_SIZE):
        batch = texts[start : start + OLLAMA_BATCH_SIZE]
        log.info(
            "Sentiment Qwen : lot %d-%d / %d",
            start + 1,
            start + len(batch),
            len(texts),
        )
        sentiments.extend(classify_sentiment_batch(batch))

    return sentiments


# ── BERTopic ──────────────────────────────────────────────────────────────────

def extract_topics(records: list[dict]) -> list[dict]:
    """
    Détecte les sujets dominants avec BERTopic sur text_normalized.
    Retourne une liste alignée avec records : topic_id, topic_label, topic_keywords.
    """
    default = {
        "topic_id": -1,
        "topic_label": "unclassified",
        "topic_keywords": [],
    }
    if len(records) < MIN_DOCS_FOR_BERTOPIC:
        log.info(
            "Trop peu de documents (%d) pour BERTopic — sujet unique.",
            len(records),
        )
        return [dict(default) for _ in records]

    texts = [
        (r.get("text_normalized") or r.get("text") or "").strip()
        for r in records
    ]

    try:
        from bertopic import BERTopic
        from sentence_transformers import SentenceTransformer
    except ImportError as exc:
        log.error("BERTopic non disponible : %s", exc)
        return [dict(default) for _ in records]

    embedding_model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
    min_topic_size = max(2, len(records) // 5)
    topic_model = BERTopic(
        embedding_model=embedding_model,
        language="multilingual",
        min_topic_size=min_topic_size,
        verbose=False,
        calculate_probabilities=False,
    )

    topics, _ = topic_model.fit_transform(texts)
    topic_info = topic_model.get_topic_info()

    label_map: dict[int, str] = {}
    keywords_map: dict[int, list[str]] = {}
    for _, row in topic_info.iterrows():
        tid = int(row["Topic"])
        if tid == -1:
            label_map[tid] = "outlier"
            keywords_map[tid] = []
            continue
        words = topic_model.get_topic(tid) or []
        keywords = [w for w, _ in words[:5]]
        label_map[tid] = ", ".join(keywords[:3]) or f"topic_{tid}"
        keywords_map[tid] = keywords

    results: list[dict] = []
    for topic_id in topics:
        tid = int(topic_id)
        results.append(
            {
                "topic_id": tid,
                "topic_label": label_map.get(tid, f"topic_{tid}"),
                "topic_keywords": keywords_map.get(tid, []),
            }
        )
    log.info("BERTopic : %d sujets détectés.", len(set(topics)))
    return results


# ── MongoDB ───────────────────────────────────────────────────────────────────

def get_mongo_db():
    client: MongoClient = pymongo.MongoClient(MONGO_URI)
    return client[MONGO_DB]


def get_mongo_collection() -> Collection:
    db = get_mongo_db()
    collection = db[MONGO_COLLECTION]
    ensure_indexes(collection)
    ensure_aggregate_indexes(db[MONGO_AGGREGATES_COLLECTION])
    return collection


def ensure_indexes(collection: Collection) -> None:
    collection.create_index([("id", ASCENDING)], unique=True, name="idx_id")
    collection.create_index([("collected_at", ASCENDING)], name="idx_collected_at")
    collection.create_index([("sentiment_label", ASCENDING)], name="idx_sentiment")
    collection.create_index([("topic_id", ASCENDING)], name="idx_topic_id")
    collection.create_index([("lang", ASCENDING)], name="idx_lang")
    collection.create_index([("match_id", ASCENDING)], name="idx_match_id")
    collection.create_index(
        [("partition.year", ASCENDING), ("partition.hour", ASCENDING)],
        name="idx_partition",
    )


def ensure_aggregate_indexes(collection: Collection) -> None:
    collection.create_index(
        [
            ("aggregate_type", ASCENDING),
            ("partition.year", ASCENDING),
            ("partition.month", ASCENDING),
            ("partition.day", ASCENDING),
            ("partition.hour", ASCENDING),
        ],
        unique=True,
        name="idx_hourly_aggregate",
        partialFilterExpression={"aggregate_type": "hourly"},
    )
    collection.create_index(
        [("aggregate_type", ASCENDING), ("match_id", ASCENDING)],
        unique=True,
        name="idx_match_aggregate",
        partialFilterExpression={"aggregate_type": "match"},
    )


def build_enriched_document(
    record: dict,
    sentiment: dict,
    topic: dict,
    partition: dict,
    match_event: Optional[dict],
) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    doc = {
        **record,
        "sentiment_label": sentiment["label"],
        "sentiment_score": sentiment["score"],
        "sentiment_method": sentiment.get("method", "qwen3:8b"),
        "topic_id": topic["topic_id"],
        "topic_label": topic["topic_label"],
        "topic_keywords": topic["topic_keywords"],
        "match_id": None,
        "match_timestamp": None,
        "event_type": None,
        "event_minute": None,
        "team_home": None,
        "team_away": None,
        "partition": partition,
        "nlp_processed_at": now,
        "nlp_model_sentiment": OLLAMA_MODEL,
        "nlp_model_topics": "bertopic+paraphrase-multilingual-MiniLM-L12-v2",
    }
    if match_event:
        doc.update(
            {
                "match_id": match_event.get("match_id"),
                "match_timestamp": match_event.get("match_timestamp"),
                "event_type": match_event.get("event_type"),
                "event_minute": match_event.get("event_minute"),
                "team_home": match_event.get("team_home"),
                "team_away": match_event.get("team_away"),
            }
        )
    return doc


def upsert_documents(collection: Collection, documents: list[dict]) -> int:
    if not documents:
        return 0
    for doc in documents:
        collection.replace_one({"id": doc["id"]}, doc, upsert=True)
    log.info("MongoDB : %d documents upsertés dans %s.%s", len(documents), MONGO_DB, MONGO_COLLECTION)
    return len(documents)


# ── Agrégats top topics (heure / match) ───────────────────────────────────────

def _top_topics_from_docs(documents: list[dict], top_k: int) -> list[dict]:
    """Compte les topics et retourne le top K."""
    counts: dict[tuple[int, str], int] = {}
    for doc in documents:
        topic_id = doc.get("topic_id", -1)
        if topic_id is None or int(topic_id) < 0:
            continue
        label = doc.get("topic_label") or f"topic_{topic_id}"
        key = (int(topic_id), str(label))
        counts[key] = counts.get(key, 0) + 1

    ranked = sorted(counts.items(), key=lambda item: item[1], reverse=True)
    return [
        {"topic_id": key[0], "topic_label": key[1], "count": count}
        for key, count in ranked[:top_k]
    ]


def _sentiment_breakdown(documents: list[dict]) -> dict[str, int]:
    breakdown = {"positive": 0, "neutral": 0, "negative": 0}
    for doc in documents:
        label = doc.get("sentiment_label", "neutral")
        if label in breakdown:
            breakdown[label] += 1
    return breakdown


def build_hourly_aggregate(documents: list[dict], partition: dict, top_k: int) -> dict:
    """Top thèmes + sentiment pour une partition horaire."""
    scores = [float(d.get("sentiment_score", 0.0)) for d in documents]
    return {
        "aggregate_type": "hourly",
        "partition": partition,
        "post_count": len(documents),
        "top_topics": _top_topics_from_docs(documents, top_k),
        "sentiment_breakdown": _sentiment_breakdown(documents),
        "avg_sentiment_score": round(sum(scores) / len(scores), 4) if scores else 0.0,
        "aggregated_at": datetime.now(timezone.utc).isoformat(),
    }


def build_match_aggregate(match_id: str, documents: list[dict], top_k: int) -> dict:
    """Top thèmes + sentiment pour un match (tous posts corrélés)."""
    sample = documents[0]
    scores = [float(d.get("sentiment_score", 0.0)) for d in documents]
    event_types: dict[str, int] = {}
    for doc in documents:
        event_type = doc.get("event_type")
        if event_type:
            event_types[event_type] = event_types.get(event_type, 0) + 1

    return {
        "aggregate_type": "match",
        "match_id": match_id,
        "team_home": sample.get("team_home"),
        "team_away": sample.get("team_away"),
        "post_count": len(documents),
        "top_topics": _top_topics_from_docs(documents, top_k),
        "sentiment_breakdown": _sentiment_breakdown(documents),
        "avg_sentiment_score": round(sum(scores) / len(scores), 4) if scores else 0.0,
        "events_breakdown": event_types,
        "aggregated_at": datetime.now(timezone.utc).isoformat(),
    }


def refresh_topic_aggregates(
    posts_collection: Collection,
    aggregates_collection: Collection,
    year: int,
    month: int,
    day: int,
    hour: int,
    match_ids: set[str],
    top_k: int = TOP_TOPICS_K,
) -> int:
    """
    Recalcule les agrégats horaires et par match à partir de MongoDB.
    Permet à Personne 4 de consommer directement le top des thèmes.
    """
    partition = {"year": year, "month": month, "day": day, "hour": hour}
    written = 0

    hourly_docs = list(
        posts_collection.find(
            {
                "partition.year": year,
                "partition.month": month,
                "partition.day": day,
                "partition.hour": hour,
            }
        )
    )
    if hourly_docs:
        hourly_agg = build_hourly_aggregate(hourly_docs, partition, top_k)
        aggregates_collection.replace_one(
            {"aggregate_type": "hourly", "partition": partition},
            hourly_agg,
            upsert=True,
        )
        written += 1
        log.info(
            "Agrégat horaire : %d posts, top topic = %s",
            hourly_agg["post_count"],
            hourly_agg["top_topics"][0]["topic_label"] if hourly_agg["top_topics"] else "n/a",
        )

    for match_id in sorted(match_ids):
        match_docs = list(posts_collection.find({"match_id": match_id}))
        if not match_docs:
            continue
        match_agg = build_match_aggregate(match_id, match_docs, top_k)
        aggregates_collection.replace_one(
            {"aggregate_type": "match", "match_id": match_id},
            match_agg,
            upsert=True,
        )
        written += 1
        log.info(
            "Agrégat match %s : %d posts, top topic = %s",
            match_id,
            match_agg["post_count"],
            match_agg["top_topics"][0]["topic_label"] if match_agg["top_topics"] else "n/a",
        )

    return written


# ── Pipeline principal ────────────────────────────────────────────────────────

def enrich_records(
    records: list[dict],
    year: int,
    month: int,
    day: int,
    hour: int,
    match_events: Optional[list[dict]] = None,
) -> list[dict]:
    if not records:
        return []

    sentiments = classify_all_sentiments(records)
    topics = extract_topics(records)
    partition = {"year": year, "month": month, "day": day, "hour": hour}
    events = match_events or []

    enriched: list[dict] = []
    for record, sentiment, topic in zip(records, sentiments, topics):
        match_event = nearest_match_event(record.get("collected_at", ""), events)
        enriched.append(
            build_enriched_document(record, sentiment, topic, partition, match_event)
        )
    return enriched


def run_nlp_for_partition(year: int, month: int, day: int, hour: int) -> int:
    """Point d'entrée appelé par Airflow ou en CLI."""
    fs = get_fs()
    records = read_silver_partition(year, month, day, hour, fs)
    if not records:
        log.info("Aucun document Silver à enrichir.")
        return 0

    match_events = load_match_events(MATCH_EVENTS_PATH)
    enriched = enrich_records(records, year, month, day, hour, match_events)

    db = get_mongo_db()
    posts_collection = db[MONGO_COLLECTION]
    ensure_indexes(posts_collection)
    aggregates_collection = db[MONGO_AGGREGATES_COLLECTION]
    ensure_aggregate_indexes(aggregates_collection)

    count = upsert_documents(posts_collection, enriched)

    match_ids = {doc["match_id"] for doc in enriched if doc.get("match_id")}
    refresh_topic_aggregates(
        posts_collection,
        aggregates_collection,
        year,
        month,
        day,
        hour,
        match_ids,
    )
    return count


def check_ollama_model() -> bool:
    """Vérifie que le modèle Ollama est disponible."""
    try:
        resp = requests.get(f"{OLLAMA_BASE_URL.rstrip('/')}/api/tags", timeout=10)
        resp.raise_for_status()
        models = [m.get("name", "") for m in resp.json().get("models", [])]
        target = OLLAMA_MODEL.split(":")[0]
        available = any(target in name for name in models)
        if not available:
            log.warning(
                "Modèle %s absent — exécuter : ollama pull %s",
                OLLAMA_MODEL,
                OLLAMA_MODEL,
            )
        return available
    except requests.RequestException as exc:
        log.warning("Ollama injoignable (%s) : %s", OLLAMA_BASE_URL, exc)
        return False


if __name__ == "__main__":
    check_ollama_model()
    now = datetime.now(timezone.utc)
    count = run_nlp_for_partition(now.year, now.month, now.day, now.hour)
    log.info("NLP terminé — %d documents enrichis.", count)
