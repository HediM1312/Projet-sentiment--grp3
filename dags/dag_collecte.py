"""
DAG Airflow — Collecte horaire des posts sociaux
- Lance le collecteur toutes les heures
- Consomme Kafka → MinIO Bronze (Parquet)
- Nettoyage Silver : déduplication, détection langue, normalisation texte
- NLP : sentiment Qwen3:8b + BERTopic → MongoDB
- Gold : agrégats horaires PostgreSQL + résumés LLM (Personne 4 — Meissa MARA)
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = "social-raw"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio12345")
BRONZE_BUCKET = "bronze"

default_args = {
    "owner": "grp3",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ── Tâche 1 : lancer le collecteur ──────────────────────────────────────────

def task_collect(**context):
    import sys
    sys.path.insert(0, "/opt/airflow/collector")
    from collector import run_once
    run_once()


# ── Tâche 2 : consommer Kafka → MinIO Bronze ────────────────────────────────

def task_kafka_to_bronze(**context):
    """Lit les messages Kafka et les écrit en Parquet partitionné dans MinIO."""
    from kafka import KafkaConsumer
    import pyarrow as pa
    import pyarrow.parquet as pq
    import s3fs

    execution_date: datetime = context["execution_date"]
    partition_path = (
        f"s3://{BRONZE_BUCKET}/social/"
        f"year={execution_date.year}/"
        f"month={execution_date.month:02d}/"
        f"day={execution_date.day:02d}/"
        f"hour={execution_date.hour:02d}/"
    )

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10_000,  # 10 s sans nouveau message → stop
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id=f"airflow-bronze-{execution_date.strftime('%Y%m%d%H')}",
    )

    records = []
    for msg in consumer:
        records.append(msg.value)
    consumer.close()

    if not records:
        log.info("Aucun message Kafka à consommer.")
        return

    table = pa.Table.from_pylist(records)

    fs = s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        endpoint_url=MINIO_ENDPOINT,
        use_ssl=False,
    )

    pq.write_table(table, partition_path + "data.parquet", filesystem=fs)
    log.info("Bronze : %d documents écrits dans %s", len(records), partition_path)


# ── Tâche 3 : nettoyage Bronze → Silver ─────────────────────────────────────

def task_bronze_to_silver(**context):
    """Lit le Bronze Parquet de la partition courante et produit la couche Silver."""
    import sys
    sys.path.insert(0, "/opt/airflow/notebooks/silver")
    from silver_cleaning import run_silver_for_partition

    execution_date: datetime = context["execution_date"]
    count = run_silver_for_partition(
        execution_date.year,
        execution_date.month,
        execution_date.day,
        execution_date.hour,
    )
    log.info("Silver : %d documents nettoyés.", count)


# ── Tâche 4 : NLP Silver → MongoDB ──────────────────────────────────────────

def task_silver_to_nlp(**context):
    """Enrichit la couche Silver : sentiment Qwen3:8b + topics BERTopic → MongoDB."""
    import sys
    sys.path.insert(0, "/opt/airflow/notebooks/nlp")
    from nlp_pipeline import run_nlp_for_partition

    execution_date: datetime = context["execution_date"]
    count = run_nlp_for_partition(
        execution_date.year,
        execution_date.month,
        execution_date.day,
        execution_date.hour,
    )
    log.info("NLP : %d documents enrichis dans MongoDB.", count)


# ── DAG ──────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="dag_collecte_sociale",
    description="Collecte horaire posts sociaux → Kafka → Bronze → Silver → NLP → MongoDB",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["grp3", "collecte", "bronze", "silver", "nlp"],
) as dag:

    collect = PythonOperator(
        task_id="collecter_posts",
        python_callable=task_collect,
    )

    kafka_to_bronze = PythonOperator(
        task_id="kafka_vers_bronze",
        python_callable=task_kafka_to_bronze,
    )

    bronze_to_silver = PythonOperator(
        task_id="bronze_vers_silver",
        python_callable=task_bronze_to_silver,
    )

    silver_to_nlp = PythonOperator(
        task_id="silver_vers_nlp",
        python_callable=task_silver_to_nlp,
    )

    collect >> kafka_to_bronze >> bronze_to_silver >> silver_to_nlp


# ── Tâche 5 : NLP → Gold PostgreSQL (agrégats horaires) ─────────────────────

def task_nlp_to_gold(**context):
    """Lit MongoDB, calcule les agrégats Gold et upserte dans PostgreSQL."""
    import sys
    sys.path.insert(0, "/opt/airflow/notebooks/gold")
    from gold_aggregates import run_gold_for_partition

    execution_date: datetime = context["execution_date"]
    count = run_gold_for_partition(
        execution_date.year,
        execution_date.month,
        execution_date.day,
        execution_date.hour,
    )
    log.info("Gold : %d posts agrégés dans PostgreSQL.", count)


# ── Tâche 6 : Résumés LLM (Gold → trend_summaries) ──────────────────────────

def task_gold_summarize(**context):
    """Génère les résumés de tendances via Qwen3:8b et les stocke dans PostgreSQL."""
    import sys
    sys.path.insert(0, "/opt/airflow/notebooks/gold")
    from summarizer import run_summaries_for_partition

    execution_date: datetime = context["execution_date"]
    count = run_summaries_for_partition(
        execution_date.year,
        execution_date.month,
        execution_date.day,
        execution_date.hour,
    )
    log.info("Summarizer : %d résumés générés.", count)


# ── DAG (étendu avec les tâches Gold) ────────────────────────────────────────

with DAG(
    dag_id="dag_collecte_sociale",
    description=(
        "Collecte horaire posts sociaux → Kafka → Bronze → Silver → NLP → MongoDB"
        " → Gold PostgreSQL → Résumés LLM"
    ),
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["grp3", "collecte", "bronze", "silver", "nlp", "gold"],
) as dag:

    collect = PythonOperator(
        task_id="collecter_posts",
        python_callable=task_collect,
    )

    kafka_to_bronze = PythonOperator(
        task_id="kafka_vers_bronze",
        python_callable=task_kafka_to_bronze,
    )

    bronze_to_silver = PythonOperator(
        task_id="bronze_vers_silver",
        python_callable=task_bronze_to_silver,
    )

    silver_to_nlp = PythonOperator(
        task_id="silver_vers_nlp",
        python_callable=task_silver_to_nlp,
    )

    nlp_to_gold = PythonOperator(
        task_id="nlp_vers_gold",
        python_callable=task_nlp_to_gold,
    )

    gold_summarize = PythonOperator(
        task_id="gold_resumes_llm",
        python_callable=task_gold_summarize,
    )

    # Pipeline complet
    collect >> kafka_to_bronze >> bronze_to_silver >> silver_to_nlp >> nlp_to_gold >> gold_summarize