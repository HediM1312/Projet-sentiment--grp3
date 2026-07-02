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

## Module Gold + Dashboard Superset (Personne 4 — Meissa MARA)

### Fichiers produits

| Fichier | Rôle |
|---------|------|
| `infra/postgres/init_gold.sql` | DDL des tables Gold (exécuté au 1er démarrage PostgreSQL) |
| `notebooks/gold/gold_aggregates.py` | Agrégats horaires MongoDB → PostgreSQL |
| `notebooks/gold/summarizer.py` | Résumés narratifs LLM (Qwen3:8b via Ollama) → PostgreSQL |
| `superset/superset_setup.py` | Configuration automatique du dashboard Superset via API |
| `notebooks/gold/requirements.txt` | Dépendances Python du module Gold |

### Tables Gold (PostgreSQL — base `gold`)

| Table | Description |
|-------|-------------|
| `match_events_gold` | Miroir du CSV `data/match_events.csv` pour les JOINs SQL |
| `hourly_sentiment` | Agrégats horaires : volume, sentiment moyen, top topics, ratios |
| `team_sentiment_heatmap` | Sentiment par équipe × heure (source heatmap Superset) |
| `trend_summaries` | Résumés narratifs LLM par heure/match |
| `v_sentiment_vs_events` | Vue : JOIN hourly_sentiment × match_events_gold |

### Pipeline Airflow (tâches Gold ajoutées)

```
collecter_posts → kafka_vers_bronze → bronze_vers_silver → silver_vers_nlp
   → nlp_vers_gold → gold_resumes_llm
```

- **`nlp_vers_gold`** : lit MongoDB, calcule les agrégats `hourly_sentiment` et `team_sentiment_heatmap`, upserte dans PostgreSQL.
- **`gold_resumes_llm`** : pour chaque agrégat sans résumé, appelle Qwen3:8b (Ollama) et stocke dans `trend_summaries`.

### Variables d'environnement Gold

| Variable | Défaut | Description |
|----------|--------|-------------|
| `PG_DSN` | `postgresql://app:app12345@postgres:5432/gold` | Connexion PostgreSQL |
| `TOP_TOPICS_K` | `5` | Nombre de top topics dans les agrégats |

### Configurer Superset

```bash
# Créer l'admin Superset (une seule fois)
docker exec -it grp3-superset superset fab create-admin \
    --username admin --firstname Admin --lastname Admin \
    --email admin@example.com --password admin

# Initialiser la base Superset
docker exec -it grp3-superset superset db upgrade
docker exec -it grp3-superset superset init

# Lancer le script de setup automatique (crée DB, datasets, charts, dashboard)
pip install requests psycopg2-binary
SUPERSET_URL=http://localhost:8089 python superset/superset_setup.py
```

Le dashboard **"Sentiment & Tendances — Coupe du Monde"** est alors accessible sur `http://localhost:8089`.

Il contient :
- **Courbe** : sentiment moyen vs timeline horaire (avec ligne neutre à 0.5)
- **Heatmap** : sentiment par équipe × heure
- **Barres empilées** : volume de posts (positifs / neutres / négatifs) par heure
- **Tableau** : résumés narratifs LLM par tranche horaire

### Utilisation manuelle (hors Airflow)

```bash
# Agrégats Gold pour une heure donnée
python notebooks/gold/gold_aggregates.py --hour 2026-07-01T14:00:00+00:00

# Backfill complet
python notebooks/gold/gold_aggregates.py --backfill

# Générer les résumés LLM
python notebooks/gold/summarizer.py --match wc2026-fr-pt
```