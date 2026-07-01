# Databricks notebook source
# Projet 3 — Sentiment & Tendances sociales — Groupe 3
# Notebook Silver : Bronze → Silver (Déduplication, Langue, Normalisation)
# Auteur : Hassan HOUSSEIN HOUMED

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer — Nettoyage des données sociales
# MAGIC
# MAGIC Ce notebook lit les données brutes (**Bronze**) stockées dans MinIO,
# MAGIC applique les transformations suivantes puis écrit la couche **Silver** :
# MAGIC
# MAGIC 1. **Déduplication** — suppression des doublons par `id`
# MAGIC 2. **Détection de langue** — `langdetect` (+ fallback `spaCy`)
# MAGIC 3. **Normalisation texte** — minuscules, suppression URLs/mentions, Unicode NFKC
# MAGIC
# MAGIC **Pipeline global :**
# MAGIC ```
# MAGIC [Kafka] → [Bronze/MinIO Parquet] → [Silver/MinIO Parquet] → [NLP/MongoDB]
# MAGIC ```

# COMMAND ----------

# MAGIC %md ## 1. Dépendances

# COMMAND ----------

# Installation des packages si nécessaire (Databricks cluster)
# %pip install langdetect spacy pyarrow s3fs
# import spacy; spacy.cli.download("fr_core_news_sm")

import re
import unicodedata
import logging
from datetime import datetime, timezone
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from langdetect import detect, DetectorFactory, LangDetectException

DetectorFactory.seed = 42  # reproductibilité

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md ## 2. Configuration

# COMMAND ----------

# -- Paramètres MinIO / S3 --
MINIO_ENDPOINT   = "http://minio:9000"   # ou endpoint S3 réel en prod
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio12345"
BRONZE_BUCKET    = "bronze"
SILVER_BUCKET    = "silver"

# Partition à traiter (modifier selon le besoin)
EXEC_DATE = datetime.now(timezone.utc)
YEAR  = EXEC_DATE.year
MONTH = EXEC_DATE.month
DAY   = EXEC_DATE.day
HOUR  = EXEC_DATE.hour

print(f"Partition cible : {YEAR}-{MONTH:02d}-{DAY:02d} {HOUR:02d}h")

# COMMAND ----------

# MAGIC %md ## 3. Connexion au stockage

# COMMAND ----------

fs = s3fs.S3FileSystem(
    key=MINIO_ACCESS_KEY,
    secret=MINIO_SECRET_KEY,
    endpoint_url=MINIO_ENDPOINT,
    use_ssl=False,
)

bronze_path = (
    f"{BRONZE_BUCKET}/social/"
    f"year={YEAR}/month={MONTH:02d}/day={DAY:02d}/hour={HOUR:02d}/data.parquet"
)
silver_path = (
    f"{SILVER_BUCKET}/social/"
    f"year={YEAR}/month={MONTH:02d}/day={DAY:02d}/hour={HOUR:02d}/data.parquet"
)

print(f"Bronze : {bronze_path}")
print(f"Silver : {silver_path}")

# COMMAND ----------

# MAGIC %md ## 4. Lecture Bronze

# COMMAND ----------

try:
    bronze_table = pq.read_table(bronze_path, filesystem=fs)
    records = bronze_table.to_pylist()
    print(f"{len(records)} documents lus depuis Bronze.")
except Exception as e:
    print(f"Erreur lecture Bronze : {e}")
    records = []

# Aperçu
if records:
    print("\nExemple de document Bronze :")
    for k, v in list(records[0].items())[:6]:
        print(f"  {k}: {str(v)[:80]}")

# COMMAND ----------

# MAGIC %md ## 5. Fonctions de nettoyage

# COMMAND ----------

_URL_RE      = re.compile(r"https?://\S+")
_MENTION_RE  = re.compile(r"@\w+")
_HASHTAG_RE  = re.compile(r"#(\w+)")
_SPACES_RE   = re.compile(r"\s+")


def detect_language(text: str) -> str:
    """Détecte la langue ; retourne 'unknown' si détection impossible."""
    try:
        return detect(text[:500])
    except LangDetectException:
        return "unknown"


def normalize_text(text: str) -> str:
    """Normalise le texte brut pour le NLP."""
    text = text.lower()
    text = _URL_RE.sub("", text)
    text = _MENTION_RE.sub("", text)
    text = _HASHTAG_RE.sub(r"\1", text)            # #WorldCup → worldcup (après lower)
    text = unicodedata.normalize("NFKC", text)
    text = _SPACES_RE.sub(" ", text).strip()
    return text


def process_records(records: list) -> list:
    """Déduplique et enrichit une liste de documents."""
    seen_ids: set = set()
    cleaned = []

    for record in records:
        doc_id = record.get("id", "")
        if doc_id in seen_ids:
            continue
        seen_ids.add(doc_id)

        text = (record.get("text") or "").strip()
        if not text:
            continue

        cleaned.append({
            **record,
            "text_normalized": normalize_text(text),
            "lang": detect_language(text),
            "silver_processed_at": datetime.now(timezone.utc).isoformat(),
        })

    return cleaned

# COMMAND ----------

# MAGIC %md ## 6. Application du pipeline Silver

# COMMAND ----------

silver_records = process_records(records)

total_in  = len(records)
total_out = len(silver_records)
dupes     = total_in - total_out

print(f"Documents en entrée  : {total_in}")
print(f"Documents en sortie  : {total_out}")
print(f"Doublons supprimés   : {dupes}")

# Répartition des langues
if silver_records:
    from collections import Counter
    lang_counts = Counter(r["lang"] for r in silver_records)
    print("\nRépartition des langues :")
    for lang, count in lang_counts.most_common(10):
        print(f"  {lang}: {count} ({count/total_out*100:.1f}%)")

# COMMAND ----------

# MAGIC %md ## 7. Aperçu d'un document Silver

# COMMAND ----------

if silver_records:
    sample = silver_records[0]
    print("Exemple de document Silver :")
    for k, v in sample.items():
        print(f"  {k}: {str(v)[:100]}")

# COMMAND ----------

# MAGIC %md ## 8. Écriture Silver dans MinIO

# COMMAND ----------

if silver_records:
    silver_table = pa.Table.from_pylist(silver_records)
    pq.write_table(silver_table, silver_path, filesystem=fs)
    print(f"Silver écrit : {total_out} documents → {silver_path}")
else:
    print("Aucun document à écrire en Silver.")

# COMMAND ----------

# MAGIC %md ## 9. Validation

# COMMAND ----------

# Relecture pour vérifier
try:
    check_table = pq.read_table(silver_path, filesystem=fs)
    check_records = check_table.to_pylist()
    print(f"Validation OK : {len(check_records)} documents relus depuis Silver.")
    print(f"Colonnes : {check_table.column_names}")
except Exception as e:
    print(f"Erreur de validation : {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schéma du document Silver
# MAGIC
# MAGIC | Champ | Type | Description |
# MAGIC |-------|------|-------------|
# MAGIC | `id` | string | Hash MD5 du contenu (dédup) |
# MAGIC | `source` | string | `reddit` ou `rss` |
# MAGIC | `text` | string | Texte brut original |
# MAGIC | `text_normalized` | string | Texte nettoyé pour NLP |
# MAGIC | `lang` | string | Code langue ISO 639-1 |
# MAGIC | `collected_at` | string | Timestamp collecte (ISO 8601) |
# MAGIC | `created_at` | string | Timestamp publication source |
# MAGIC | `silver_processed_at` | string | Timestamp traitement Silver |
# MAGIC | `url` | string | Lien source |
# MAGIC | `author` | string | Auteur (Reddit) |
# MAGIC | `score` | int | Score Reddit (upvotes) |
