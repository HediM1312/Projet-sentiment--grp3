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

## DÉMONSTRATION RAPIDE (pour la soutenance)

> Ces commandes montrent le pipeline de bout en bout avec des données représentatives
> d'un match France vs Portugal (Coupe du Monde 2026).

### Étape 1 — Vérifier que les services tournent

```bash
docker ps --format "table {{.Names}}\t{{.Status}}" | grep grp3
```

Résultat attendu : 7 conteneurs `Up` (postgres, minio, kafka, mongo, superset, airflow, ollama).

### Étape 2 — Injecter des données de démo (France 2-0 Portugal)

```bash
# Injection directe dans PostgreSQL Gold (données du match du 02/07/2026)
docker exec grp3-postgres psql -U app -d gold -c "
INSERT INTO match_events_gold (match_id, event_type, event_minute, event_timestamp, team_home, team_away, description)
VALUES
  ('wc2026-fr-pt','kickoff', 0,'2026-07-02 13:00:00+00','France','Portugal','Coup d''envoi'),
  ('wc2026-fr-pt','goal',   34,'2026-07-02 13:34:00+00','France','Portugal','But de Mbappé (34e)'),
  ('wc2026-fr-pt','goal',   78,'2026-07-02 14:18:00+00','France','Portugal','But de Dembélé (78e)'),
  ('wc2026-fr-pt','fulltime',90,'2026-07-02 14:45:00+00','France','Portugal','Fin du match (2-0)')
ON CONFLICT (match_id, event_type, event_minute) DO NOTHING;
"

docker exec grp3-postgres psql -U app -d gold -c "
INSERT INTO hourly_sentiment (hour_bucket, match_id, team_home, team_away, total_posts, positive_count, neutral_count, negative_count, avg_sentiment_score, top_topics)
VALUES
  ('2026-07-02 13:00+00','wc2026-fr-pt','France','Portugal', 620,310,205,105,0.61,'[{\"topic\":\"coup_envoi\",\"count\":180}]'),
  ('2026-07-02 14:00+00','wc2026-fr-pt','France','Portugal', 890,530,240,120,0.77,'[{\"topic\":\"but_mbappe\",\"count\":310}]'),
  ('2026-07-02 16:00+00','wc2026-fr-pt','France','Portugal',1100,720,240,140,0.83,'[{\"topic\":\"victoire\",\"count\":410}]')
ON CONFLICT (hour_bucket, match_id) DO UPDATE SET
  total_posts=EXCLUDED.total_posts, avg_sentiment_score=EXCLUDED.avg_sentiment_score;
"
```

### Étape 3 — Vérifier les données Gold (PostgreSQL)

```bash
# Agrégats horaires de sentiment
docker exec grp3-postgres psql -U app -d gold -c "
SELECT hour_bucket, total_posts, avg_sentiment_score, positive_count, negative_count
FROM hourly_sentiment
ORDER BY hour_bucket;"

# Événements du match
docker exec grp3-postgres psql -U app -d gold -c "
SELECT event_type, event_minute, description FROM match_events_gold ORDER BY event_minute;"

# Vue de corrélation sentiment × événements
docker exec grp3-postgres psql -U app -d gold -c "
SELECT hour_bucket, avg_sentiment_score, event_type, event_minute
FROM v_sentiment_vs_events
ORDER BY hour_bucket, event_minute;"
```

### Étape 4 — Vérifier le pipeline Silver (PyArrow + MinIO)

```bash
# Lancer le collecteur manuellement (publie dans Kafka)
docker exec grp3-airflow python /opt/airflow/collector/collector.py

# Vérifier les fichiers Parquet dans MinIO Bronze
docker exec grp3-minio mc alias set local http://minio:9000 minio minio12345 2>/dev/null
docker exec grp3-minio mc ls local/bronze/social/ --recursive

# Vérifier Silver
docker exec grp3-minio mc ls local/silver/social/ --recursive
```

### Étape 5 — Dashboard Superset en direct

```bash
# Reconfigurer le dashboard si besoin (connexion + datasets + charts)
python superset/superset_setup.py

# Ouvrir le dashboard
# → http://localhost:8089  |  admin / admin
# → Dashboard : "Sentiment & Tendances — Coupe du Monde"
```

Le dashboard contient :
- **Courbe** : sentiment moyen par heure (monte après chaque but)
- **Barres empilées** : volume posts Positifs / Neutres / Négatifs par heure
- **Heatmap** : sentiment par équipe × heure (France positif, Portugal négatif)
- **Tableau** : résumés narratifs LLM générés par Qwen3:8b

### Étape 6 — Pipeline NLP complet (optionnel, ~5 min avec Ollama)

```bash
# Déclencher le DAG complet depuis Airflow
# → http://localhost:8080  →  dag_collecte_sociale  →  ▶ Trigger DAG

# Ou lancer le NLP manuellement sur la partition courante
docker exec grp3-airflow python /opt/airflow/notebooks/nlp/nlp_pipeline.py

# Vérifier les posts enrichis dans MongoDB
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
