# Projet 3 — Sentiment & Tendances sociales
**DAT — Master IA / Big Data — Groupe 3**
Pipeline batch d'analyse de sentiment sur les réseaux sociaux pendant la Coupe du Monde.

## Architecture

```
[Collecte horaire] → [Kafka] → [Bronze MinIO/Parquet]
  → [Silver PyArrow] → [NLP Qwen3+BERTopic] → [MongoDB + PostgreSQL] → [Superset]
```

## Répartition des tâches

| Personne | Module | Dossier |
|----------|--------|---------|
| Marcus LINGUET | Infrastructure + Collecte | `docker-compose.yml`, `collector/`, `dags/` |
| Hassan HOUSSEIN HOUMED | Ingestion Bronze/Silver | `notebooks/silver/` |
| Hedi MATHLOUTHI | NLP (Sentiment + Topics) | `notebooks/nlp/` |
| Meissa MARA | Gold + Dashboard Superset | `notebooks/gold/`, `superset/` |

---

## Lancement (premier démarrage)

> À faire **une seule fois** par machine (ou après suppression des volumes Docker).

### 1. Démarrer l'infrastructure

```bash
cd Projet-sentiment--grp3

# Construire les images custom (Airflow + Superset avec driver PostgreSQL)
docker compose build airflow superset

# Lancer tous les services
docker compose up -d

# Vérifier que tout tourne (8 conteneurs attendus)
docker ps
```

### 2. Télécharger le modèle NLP (Ollama)

```bash
docker exec -it grp3-ollama ollama pull qwen3:8b
docker exec grp3-ollama ollama list
```

### 3. Initialiser Superset (une seule fois)

```bash
docker exec grp3-superset superset db upgrade
docker exec grp3-superset superset fab create-admin \
    --username admin --firstname Admin --lastname Admin \
    --email admin@example.com --password admin
docker exec grp3-superset superset init
```

> `User already exists admin` → normal, l'admin existe déjà.

### 4. Configurer le dashboard Superset

```bash
pip install requests psycopg2-binary
python superset/superset_setup.py
```

Le script crée la connexion PostgreSQL Gold, les 5 datasets, les 4 charts et le dashboard.

### 5. Tables Gold (si volume Postgres déjà existant)

`init_gold.sql` ne s'exécute qu'au **premier** démarrage. Si les tables manquent :

```bash
# Linux/Mac
cat infra/postgres/init_gold.sql | docker exec -i grp3-postgres psql -U app -d gold

# Windows PowerShell
Get-Content infra/postgres/init_gold.sql | docker exec -i grp3-postgres psql -U app -d gold
```

### 6. Accès aux services

| Service | URL | Identifiants |
|---------|-----|--------------|
| **Airflow** | http://localhost:8080 | `admin` + mot de passe généré |
| **Superset** | http://localhost:8089 | `admin` / `admin` |
| **MinIO Console** | http://localhost:9003 | `minio` / `minio12345` |
| **Ollama** | http://localhost:11434 | — |
| **MongoDB** | localhost:27017 | `app` / `app12345` |
| **PostgreSQL** | localhost:**5433** | `app` / `app12345` (base `gold`) |

**Mot de passe Airflow** (généré automatiquement au 1er démarrage) :

```bash
docker logs grp3-airflow 2>&1 | grep "Login with username"
```

---

## DÉMONSTRATION (pour la soutenance)

Le pipeline complet est : **Kafka → Bronze → Silver → NLP → Gold → Superset**.
La démo utilise ce même pipeline — les données entrent par Kafka et sortent dans Superset.

---

### AVANT LA SOUTENANCE — Préparer les données (à faire la veille ou le matin)

#### 1. Injecter des posts bruts dans Kafka

Le script `demo/inject_kafka_posts.py` publie 31 posts (fr/en/es/de, positifs/neutres/négatifs,
dont 1 doublon intentionnel) dans le topic `social-raw` — exactement comme le ferait le collecteur.

```bash
# Copier et exécuter à l'intérieur du réseau Docker
docker cp demo/inject_kafka_posts.py grp3-airflow:/tmp/inject_kafka_posts.py
docker exec grp3-airflow python /tmp/inject_kafka_posts.py
```

#### 2. Déclencher le pipeline complet via Airflow

```bash
# Option A — interface Airflow (recommandé)
# → http://localhost:8080  →  dag_collecte_sociale  →  ▶ Trigger DAG
# Le DAG enchaîne : kafka_vers_bronze → bronze_vers_silver → silver_vers_nlp → nlp_vers_gold → gold_resumes_llm
# Durée : ~5 min (NLP Qwen3:8b inclus)
```

```bash
# Option B — manuellement étape par étape (si le DAG ne se déclenche pas)
docker exec grp3-airflow python /opt/airflow/notebooks/silver/silver_cleaning.py
docker exec grp3-airflow python /opt/airflow/notebooks/nlp/nlp_pipeline.py
docker exec grp3-airflow python /opt/airflow/notebooks/gold/gold_aggregates.py
```

#### 3. Vérifier que les données sont dans Gold

```bash
docker exec grp3-postgres psql -U app -d gold -c \
  "SELECT hour_bucket, total_posts, avg_sentiment_score FROM hourly_sentiment ORDER BY hour_bucket;"
```

---

### PENDANT LA DÉMO (~2 min)

#### Étape 1 — Services actifs

```bash
docker ps --format "table {{.Names}}\t{{.Status}}" | grep grp3
```

#### Étape 2 — Données Gold traitées par le pipeline (PostgreSQL)

```bash
docker exec grp3-postgres psql -U app -d gold -c \
  "SELECT hour_bucket, total_posts, avg_sentiment_score FROM hourly_sentiment ORDER BY hour_bucket;"
```

#### Étape 3 — Dashboard Superset

```
http://localhost:8089   →   admin / admin   →   Sentiment & Tendances — Coupe du Monde
```

---

### Vérifications intermédiaires (Bronze et Silver dans MinIO)

```bash
# Fichiers Parquet Bronze (posts bruts partitionnés par heure)
docker exec grp3-minio mc alias set local http://minio:9000 minio minio12345 2>/dev/null
docker exec grp3-minio mc ls local/bronze/social/ --recursive

# Fichiers Parquet Silver (nettoyés : sans doublons, filtre 7 langues, normalisés)
docker exec grp3-minio mc ls local/silver/social/ --recursive
```

### Posts enrichis dans MongoDB (après NLP)

```bash
docker exec grp3-mongo mongosh \
  "mongodb://app:app12345@localhost:27017/sentiment?authSource=admin" \
  --quiet --eval "db.enriched_posts.countDocuments()"

docker exec grp3-mongo mongosh \
  "mongodb://app:app12345@localhost:27017/sentiment?authSource=admin" \
  --quiet --eval "db.enriched_posts.findOne({}, {text:0, text_normalized:0})"
```

---

## Pipeline complet (6 tâches Airflow)

```
collecter_posts       → Marcus  : Reddit/RSS → Kafka topic social-raw
kafka_vers_bronze     → Hassan  : Kafka → MinIO Bronze (Parquet partitionné heure)
bronze_vers_silver    → Hassan  : Bronze → Silver (dédup + 7 langues + normalisation)
silver_vers_nlp       → Hedi    : Silver → Sentiment Qwen3:8b + BERTopic → MongoDB
nlp_vers_gold         → Meissa  : MongoDB → agrégats PostgreSQL (hourly_sentiment, heatmap)
gold_resumes_llm      → Meissa  : PostgreSQL → résumés LLM Qwen3:8b → trend_summaries
```

---

## Vérifications détaillées

### MongoDB (NLP)

```bash
# Nombre de posts enrichis
docker exec grp3-mongo mongosh "mongodb://app:app12345@localhost:27017/sentiment?authSource=admin" \
  --quiet --eval "db.enriched_posts.countDocuments()"

# Exemple de post enrichi (sans le texte)
docker exec grp3-mongo mongosh "mongodb://app:app12345@localhost:27017/sentiment?authSource=admin" \
  --quiet --eval "db.enriched_posts.findOne({}, {text:0, text_normalized:0})"

# Top topics
docker exec grp3-mongo mongosh "mongodb://app:app12345@localhost:27017/sentiment?authSource=admin" \
  --quiet --eval "db.topic_aggregates.find().pretty()"
```

### PostgreSQL (Gold)

```bash
# Résumés LLM
docker exec grp3-postgres psql -U app -d gold \
  -c "SELECT match_id, hour_bucket, left(summary, 120) FROM trend_summaries LIMIT 5;"

# Heatmap équipes
docker exec grp3-postgres psql -U app -d gold \
  -c "SELECT team, hour_bucket, avg_sentiment_score FROM team_sentiment_heatmap ORDER BY team, hour_bucket;"
```

### Dépannage rapide

```bash
# Logs Airflow
docker logs grp3-airflow --tail 50

# Ollama disponible ?
curl http://localhost:11434/api/tags

# Reconfigurer Superset
python superset/superset_setup.py
```

---

## Services Docker

| Service | Port local | Usage |
|---------|-----------|-------|
| MinIO API | 9002 | Stockage Bronze/Silver/Gold |
| MinIO Console | 9003 | Interface web MinIO |
| Airflow | 8080 | Orchestration DAGs |
| Superset | 8089 | Dashboard BI |
| Ollama | 11434 | Sentiment NLP (qwen3:8b) + résumés |
| Kafka | 9092 | Tampon d'ingestion |
| MongoDB | 27017 | Posts enrichis NLP |
| PostgreSQL | **5433** | Agrégats Gold (port local décalé) |

> Ports MinIO (9002/9003) et PostgreSQL (5433) décalés pour éviter les conflits avec un stack Hadoop existant.

---

## Module Silver — Hassan HOUSSEIN HOUMED

| Fichier | Rôle |
|---------|------|
| `notebooks/silver/silver_cleaning.py` | Pipeline Bronze → Silver (production, appelé par Airflow) |
| `notebooks/silver/silver_notebook.py` | Version notebook exploratoire du même pipeline |
| `dags/dag_collecte.py` | DAG Airflow — orchestre les 6 étapes |

Transformations appliquées (Bronze → Silver) :
1. **Déduplication** — suppression par `id` (hash MD5 du texte)
2. **Filtre langue** — 7 langues conservées : `fr, en, es, de, pt, it, ar`
3. **Normalisation texte** — minuscules, URLs/mentions retirées, hashtags étendus, NFKC

---

## Module NLP — Hedi MATHLOUTHI

| Fichier | Rôle |
|---------|------|
| `notebooks/nlp/nlp_pipeline.py` | Sentiment Qwen3:8b + BERTopic → MongoDB |
| `notebooks/nlp/MONGODB_SCHEMA.md` | Schéma documentaire + index + requêtes |
| `data/match_events.csv` | Événements match pour corrélation (±120 min) |

Variables d'environnement :

| Variable | Défaut | Description |
|----------|--------|-------------|
| `OLLAMA_MODEL` | `qwen3:8b` | Modèle sentiment |
| `OLLAMA_BASE_URL` | `http://ollama:11434` | API Ollama |
| `MONGO_URI` | `mongodb://app:app12345@mongo:27017/?authSource=admin` | MongoDB |

---

## Module Gold + Dashboard — Meissa MARA

| Fichier | Rôle |
|---------|------|
| `infra/postgres/init_gold.sql` | DDL tables Gold (exécuté au 1er démarrage PostgreSQL) |
| `notebooks/gold/gold_aggregates.py` | MongoDB → agrégats PostgreSQL |
| `notebooks/gold/summarizer.py` | Résumés LLM (Qwen3:8b) → trend_summaries |
| `superset/superset_setup.py` | Configuration dashboard Superset via API REST |

Tables Gold (PostgreSQL — base `gold`) :

| Table | Description |
|-------|-------------|
| `match_events_gold` | Événements sportifs (buts, cartons, …) |
| `hourly_sentiment` | Agrégats horaires : volume, sentiment, top topics |
| `team_sentiment_heatmap` | Sentiment par équipe × heure |
| `trend_summaries` | Résumés narratifs LLM par heure/match |
| `v_sentiment_vs_events` | Vue JOIN hourly_sentiment × match_events_gold |
