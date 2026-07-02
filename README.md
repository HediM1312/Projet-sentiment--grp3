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
## Lancement (premier démarrage)

> À faire **une seule fois** par machine (ou après suppression des volumes Docker).

### 1. Démarrer l'infrastructure

```powershell
# Cloner le repo puis se placer à la racine du projet
cd Projet-sentiment--grp3

# Construire les images custom (Airflow + Superset avec driver PostgreSQL)
docker compose build airflow superset

# Lancer tous les services
docker compose up -d

# Vérifier que tout tourne
docker ps
```

### 2. Télécharger le modèle NLP (Ollama)

```powershell
docker exec -it grp3-ollama ollama pull qwen3:8b
docker exec grp3-ollama ollama list
```

### 3. Initialiser Superset (une seule fois)

```powershell
docker exec grp3-superset superset db upgrade
docker exec grp3-superset superset fab create-admin --username admin --firstname Admin --lastname Admin --email admin@example.com --password admin
docker exec grp3-superset superset init
```

> Si `User already exists admin` → c'est normal, l'admin existe déjà.

### 4. Configurer le dashboard Superset

```powershell
pip install requests psycopg2-binary
python superset/superset_setup.py
```

Le script crée la connexion PostgreSQL Gold, les datasets, les charts et le dashboard.

### 5. Tables Gold PostgreSQL (si volume Postgres déjà existant)

`init_gold.sql` ne s'exécute qu'au **premier** démarrage de PostgreSQL. Si les tables Gold manquent :

```powershell
Get-Content infra/postgres/init_gold.sql | docker exec -i grp3-postgres psql -U app -d gold
```

### 6. Accès aux services

| Service | URL | Identifiants |
|---------|-----|--------------|
| **Airflow** | http://localhost:8080 | `admin` + mot de passe généré (voir ci-dessous) |
| **Superset** | http://localhost:8089 | `admin` / `admin` |
| **MinIO Console** | http://localhost:9003 | `minio` / `minio12345` |
| **Ollama** | http://localhost:11434 | — |
| **MongoDB** | localhost:27017 | `app` / `app12345` |
| **PostgreSQL** | localhost:5432 | `app` / `app12345` (base `gold`) |

**Mot de passe Airflow** (généré automatiquement au 1er démarrage) :

```powershell
docker logs grp3-airflow 2>&1 | Select-String "Login with username"
```

---

## Utilisation Test

### A. Lancer le pipeline complet

```powershell
# 1. S'assurer que le stack tourne
docker compose up -d

# 2. Ouvrir Airflow → activer le DAG → Trigger
#    http://localhost:8080  →  dag_collecte_sociale  →  ▶ Trigger DAG
```

Pipeline exécuté :

```
collecter_posts → kafka_vers_bronze → bronze_vers_silver → silver_vers_nlp
   → nlp_vers_gold → gold_resumes_llm
```

### B. Vérifier la couche NLP (MongoDB)

```powershell
docker exec grp3-mongo mongosh "mongodb://app:app12345@localhost:27017/sentiment?authSource=admin" --quiet --eval "db.enriched_posts.countDocuments()"

docker exec grp3-mongo mongosh "mongodb://app:app12345@localhost:27017/sentiment?authSource=admin" --quiet --eval "db.enriched_posts.findOne({}, {text:0, text_normalized:0})"

docker exec grp3-mongo mongosh "mongodb://app:app12345@localhost:27017/sentiment?authSource=admin" --quiet --eval "db.topic_aggregates.find().pretty()"
```

Résultat attendu : `countDocuments() > 0`, avec `sentiment_label`, `topic_label`, `topic_keywords`.

### C. Vérifier la couche Gold (PostgreSQL)

```powershell
docker exec grp3-postgres psql -U app -d gold -c "SELECT hour_bucket, total_posts, avg_sentiment_score, positive_count, negative_count FROM hourly_sentiment LIMIT 5;"

docker exec grp3-postgres psql -U app -d gold -c "SELECT match_id, hour_bucket, left(summary, 80) FROM trend_summaries LIMIT 5;"
```

### D. Tests manuels (hors Airflow)

```powershell
# NLP seul (partition horaire courante)
docker exec grp3-airflow python /opt/airflow/notebooks/nlp/nlp_pipeline.py

# NLP sur une heure précise (année, mois, jour, heure)
docker exec grp3-airflow python -c "import sys; sys.path.insert(0,'/opt/airflow/notebooks/nlp'); from nlp_pipeline import run_nlp_for_partition; print(run_nlp_for_partition(2026, 7, 2, 12))"

# Agrégats Gold (backfill depuis MongoDB)
docker exec grp3-airflow python /opt/airflow/notebooks/gold/gold_aggregates.py --backfill

# Résumés LLM pour un match
docker exec grp3-airflow python /opt/airflow/notebooks/gold/summarizer.py --match wc2026-fr-pt
```

### E. Vérifier les fichiers intermédiaires (MinIO)

```powershell
docker exec grp3-minio mc alias set local http://localhost:9000 minio minio12345
docker exec grp3-minio mc ls local/silver/social/ --recursive
docker exec grp3-minio mc ls local/bronze/social/ --recursive
```

### F. Consulter le dashboard

1. Ouvrir http://localhost:8089
2. Se connecter : `admin` / `admin`
3. Dashboard : **Sentiment & Tendances — Coupe du Monde**

Contenu :
- **Courbe** : sentiment moyen vs timeline horaire
- **Heatmap** : sentiment par équipe × heure
- **Barres empilées** : volume posts positifs / neutres / négatifs
- **Tableau** : résumés narratifs LLM

> Les graphiques restent vides tant que le pipeline n'a pas produit de données (MongoDB → Gold).

### G. Dépannage rapide

```powershell
# Logs d'une tâche Airflow (ex. NLP)
docker logs grp3-airflow --tail 100

# Vérifier qu'Ollama répond
curl http://localhost:11434/api/tags

# Reconfigurer Superset si besoin
python superset/superset_setup.py
```

---
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

> Déjà couvert dans la section **Lancement** (étapes 3 et 4). Relancer uniquement si besoin :

```powershell
pip install requests psycopg2-binary
python superset/superset_setup.py
```

Le dashboard **"Sentiment & Tendances — Coupe du Monde"** est alors accessible sur `http://localhost:8089`.

Il contient :
- **Courbe** : sentiment moyen vs timeline horaire (avec ligne neutre à 0.5)
- **Heatmap** : sentiment par équipe × heure
- **Barres empilées** : volume de posts (positifs / neutres / négatifs) par heure
- **Tableau** : résumés narratifs LLM par tranche horaire

### Utilisation manuelle (hors Airflow)

> Voir aussi la section **Utilisation Test** (partie D).

```powershell
# Agrégats Gold pour une heure donnée
docker exec grp3-airflow python /opt/airflow/notebooks/gold/gold_aggregates.py --hour 2026-07-01T14:00:00+00:00

# Backfill complet
docker exec grp3-airflow python /opt/airflow/notebooks/gold/gold_aggregates.py --backfill

# Générer les résumés LLM
docker exec grp3-airflow python /opt/airflow/notebooks/gold/summarizer.py --match wc2026-fr-pt
```