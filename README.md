# Projet 3 — Sentiment & Tendances sociales
**DAT — Master IA / Big Data — Groupe 3**
Pipeline batch d'analyse de sentiment sur les réseaux sociaux pendant la Coupe du Monde.
## Architecture [Collecte horaire] → [Kafka] → [Bronze MinIO] → [Silver Spark] → [NLP] → [MongoDB + PostgreSQL] → [Superset]

## Répartition des tâches
| Personne | Module | Dossier |
|----------|--------|---------|
| Marcus LINGUET | Infrastructure + Collecte | `docker-compose.yml`, `collector/`, `dags/` |
| Hassan HOUSSEIN HOUMED | Ingestion Bronze/Silver | `notebooks/silver/` |
| Hedi MATHLOUTHI | NLP (Sentiment + Topics) | `notebooks/nlp/` |
| Meissa MARA | Gold + Dashboard Superset | `notebooks/gold/`, `superset/` |
## Démarrage rapide
```bash
# 1. Lancer le stack
docker compose up -d
# 2. Rebuild Airflow (deps NLP) + récupérer le modèle Qwen
docker compose build airflow
docker compose up -d
docker exec -it grp3-ollama ollama pull qwen3:8b
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
| Ollama | 11434 | Sentiment NLP (qwen3:8b) + résumés |
| Kafka | 9092 | Tampon d'ingestion |
| MongoDB | 27017 | Posts enrichis NLP |
| PostgreSQL | 5432 | Agrégats Gold + metadata Airflow |
> Ports MinIO et Superset décalés pour éviter les conflits avec un stack Hadoop existant.
## Module NLP
| Fichier | Rôle |
|---------|------|
| `notebooks/nlp/nlp_pipeline.py` | Sentiment Qwen3:8b + BERTopic → MongoDB |
| `notebooks/nlp/MONGODB_SCHEMA.md` | Schéma documentaire + index + requêtes |
| `data/match_events.csv` | Événements match pour corrélation (optionnel) |
Variables d'environnement (déjà configurées dans `docker-compose.yml`) :
| Variable | Défaut | Description |
|----------|--------|-------------|
| `OLLAMA_MODEL` | `qwen3:8b` | Modèle sentiment |
| `OLLAMA_BASE_URL` | `http://ollama:11434` | API Ollama |
| `MONGO_URI` | `mongodb://app:app12345@mongo:27017/?authSource=admin` | MongoDB |
| `MATCH_EVENTS_PATH` | `/opt/airflow/data/match_events.csv` | CSV événements match |