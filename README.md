# Projet 3 — Sentiment & Tendances sociales

**DAT — Master IA / Big Data — Groupe 3**

Pipeline batch d'analyse de sentiment sur les réseaux sociaux pendant la Coupe du Monde.

## Architecture

```
[Collecte horaire] → [Kafka] → [Bronze MinIO] → [Silver Spark] → [NLP] → [MongoDB + PostgreSQL] → [Superset]
```

## Répartition des tâches

| Personne | Module | Dossier |
|----------|--------|---------|
| Marcus | Infrastructure + Collecte | `docker-compose.yml`, `collector/`, `dags/` |
| P2 | Ingestion Bronze/Silver | `notebooks/silver/` |
| P3 | NLP (Sentiment + Topics) | `notebooks/nlp/` |
| P4 | Gold + Dashboard Superset | `notebooks/gold/`, `superset/` |

## Démarrage rapide

```bash
# 1. Lancer le stack
docker compose up -d

# 2. Récupérer le modèle LLM local
docker exec -it grp3-ollama ollama pull qwen3:4b

# 3. Accéder aux services
# MinIO console  : http://localhost:9003
# Airflow        : http://localhost:8080
# Superset       : http://localhost:8089
# Ollama         : http://localhost:11434
# Kafka          : localhost:9092
# MongoDB        : localhost:27017
```

## Services Docker

| Service | Port local | Usage |
|---------|-----------|-------|
| MinIO API | 9002 | Stockage Bronze/Silver/Gold |
| MinIO Console | 9003 | Interface web MinIO |
| Airflow | 8080 | Orchestration DAGs |
| Superset | 8089 | Dashboard |
| Ollama | 11434 | LLM local (Qwen) |
| Kafka | 9092 | Tampon d'ingestion |
| MongoDB | 27017 | Posts enrichis NLP |
| PostgreSQL | 5432 | Agrégats Gold + metadata Airflow |

> Ports MinIO et Superset décalés pour éviter les conflits avec un stack Hadoop existant.
