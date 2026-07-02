"""
Silver layer — Projet 3 Sentiment & Tendances — Groupe 3
Bronze → Silver : déduplication, détection de langue, normalisation texte

Lit les fichiers Parquet Bronze depuis MinIO, nettoie et écrit en Silver.
"""

import logging
import os
import re
import unicodedata
from datetime import datetime, timezone
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from langdetect import detect, DetectorFactory, LangDetectException

# Reproductibilité de langdetect
DetectorFactory.seed = 42

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio12345")
BRONZE_BUCKET = "bronze"
SILVER_BUCKET = "silver"

# Langues acceptées (filtrage optionnel)
ACCEPTED_LANGS = {"fr", "en", "es", "de", "pt", "it", "ar"}

_URL_RE = re.compile(r"https?://\S+")
_MENTION_RE = re.compile(r"@\w+")
_HASHTAG_RE = re.compile(r"#(\w+)")
_SPACES_RE = re.compile(r"\s+")


_PARTITION_COLS = frozenset({"year", "month", "day", "hour"})


def read_parquet_file(path: str, fs: s3fs.S3FileSystem) -> pa.Table:
    """Lit un fichier Parquet sans inférer les colonnes hive du chemin (évite conflits de types)."""
    return pq.ParquetFile(path, filesystem=fs).read()


def strip_partition_cols(record: dict) -> dict:
    return {key: value for key, value in record.items() if key not in _PARTITION_COLS}


def get_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        endpoint_url=MINIO_ENDPOINT,
        use_ssl=False,
    )


def detect_language(text: str) -> str:
    """Détecte la langue du texte ; retourne 'unknown' si échec."""
    try:
        return detect(text[:500])
    except LangDetectException:
        return "unknown"


def normalize_text(text: str) -> str:
    """
    Normalise un texte brut :
    - mise en minuscules
    - suppression des URLs
    - suppression des mentions (@user)
    - décompaction des hashtags (#WorldCup → WorldCup)
    - normalisation Unicode NFKC
    - suppression des espaces multiples
    """
    text = text.lower()
    text = _URL_RE.sub("", text)
    text = _MENTION_RE.sub("", text)
    text = _HASHTAG_RE.sub(r"\1", text)
    text = unicodedata.normalize("NFKC", text)
    text = _SPACES_RE.sub(" ", text).strip()
    return text


# ── Pipeline Silver ───────────────────────────────────────────────────────────

def process_records(records: list[dict]) -> list[dict]:
    """Déduplique, enrichit et normalise une liste de documents Bronze."""
    seen_ids: set[str] = set()
    cleaned: list[dict] = []

    for record in records:
        doc_id = record.get("id", "")

        # Déduplication stricte par ID
        if doc_id in seen_ids:
            continue
        seen_ids.add(doc_id)

        text = (record.get("text") or "").strip()
        if not text:
            continue

        lang = detect_language(text)
        text_normalized = normalize_text(text)

        cleaned.append({
            **strip_partition_cols(record),
            "text_normalized": text_normalized,
            "lang": lang,
            "silver_processed_at": datetime.now(timezone.utc).isoformat(),
        })

    return cleaned


def clean_partition(
    year: int, month: int, day: int, hour: int, fs: s3fs.S3FileSystem
) -> int:
    """Nettoie une partition horaire Bronze et l'écrit en Silver."""
    bronze_path = (
        f"{BRONZE_BUCKET}/social/"
        f"year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}/data.parquet"
    )
    silver_path = (
        f"{SILVER_BUCKET}/social/"
        f"year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}/data.parquet"
    )

    try:
        table = read_parquet_file(bronze_path, fs)
    except Exception as exc:
        log.warning("Impossible de lire Bronze %s : %s", bronze_path, exc)
        return 0

    records = table.to_pylist()
    log.info("Bronze : %d docs lus depuis %s", len(records), bronze_path)

    cleaned = process_records(records)
    if not cleaned:
        log.info("Aucun document valide après nettoyage.")
        return 0

    silver_table = pa.Table.from_pylist(cleaned)
    pq.write_table(silver_table, silver_path, filesystem=fs)
    log.info(
        "Silver : %d docs écrits (%d dédupliqués) → %s",
        len(cleaned),
        len(records) - len(cleaned),
        silver_path,
    )
    return len(cleaned)


# ── Point d'entrée ────────────────────────────────────────────────────────────

def run_silver_for_partition(year: int, month: int, day: int, hour: int) -> int:
    fs = get_fs()
    return clean_partition(year, month, day, hour, fs)


if __name__ == "__main__":
    now = datetime.now(timezone.utc)
    count = run_silver_for_partition(now.year, now.month, now.day, now.hour)
    log.info("Silver terminé — %d documents traités.", count)
