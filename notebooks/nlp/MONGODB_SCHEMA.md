# Modèle de documents MongoDB — Posts enrichis NLP

**Base :** `sentiment`  
**Collection :** `enriched_posts`  
**Responsable :** Personne 3 — Hedi MATHLOUTHI

## Connexion

| Contexte | URI |
|----------|-----|
| Depuis Docker (Airflow) | `mongodb://app:app12345@mongo:27017/?authSource=admin` |
| Depuis la machine hôte | `mongodb://app:app12345@localhost:27017/?authSource=admin` |

## Schéma du document

Chaque document = **post Silver** + **sentiment** + **topics** + **métadonnées NLP**.

```json
{
  "_id": "ObjectId(...)",

  "id": "a1b2c3d4e5f6...",
  "source": "reddit",
  "subreddit": "soccer",
  "author": "supporter42",
  "text": "What a goal at 78'! Remontada incoming!",
  "url": "https://reddit.com/r/soccer/...",
  "score": 142,
  "collected_at": "2026-07-01T14:05:00+00:00",
  "created_at": "2026-07-01T14:02:00+00:00",

  "text_normalized": "what a goal at 78 remontada incoming",
  "lang": "en",
  "silver_processed_at": "2026-07-01T14:10:00+00:00",

  "sentiment_label": "positive",
  "sentiment_score": 0.91,
  "sentiment_method": "qwen3:8b",

  "topic_id": 2,
  "topic_label": "goal, remontada, comeback",
  "topic_keywords": ["goal", "remontada", "comeback", "minute", "score"],

  "match_id": "wc2026-fr-pt",
  "match_timestamp": "2026-07-01T13:18:00+00:00",
  "event_type": "goal",
  "event_minute": "78",
  "team_home": "France",
  "team_away": "Portugal",

  "partition": {
    "year": 2026,
    "month": 7,
    "day": 1,
    "hour": 14
  },

  "nlp_processed_at": "2026-07-01T14:15:00+00:00",
  "nlp_model_sentiment": "qwen3:8b",
  "nlp_model_topics": "bertopic+paraphrase-multilingual-MiniLM-L12-v2"
}
```

### Champs hérités Silver (Bronze + nettoyage)

| Champ | Type | Description |
|-------|------|-------------|
| `id` | string | Identifiant unique (MD5), clé métier |
| `source` | string | `reddit` ou `rss` |
| `subreddit` | string? | Présent si source Reddit |
| `feed` | string? | Présent si source RSS |
| `author` | string? | Auteur Reddit (anonymisé en aval si besoin) |
| `text` | string | Texte brut |
| `url` | string | Lien source |
| `score` | int? | Score Reddit |
| `collected_at` | string | ISO 8601 — moment de collecte |
| `created_at` | string | Date de publication source |
| `text_normalized` | string | Texte nettoyé pour NLP |
| `lang` | string | Code ISO 639-1 ou `unknown` |
| `silver_processed_at` | string | Horodatage couche Silver |

### Champs NLP ajoutés

| Champ | Type | Description |
|-------|------|-------------|
| `sentiment_label` | string | `positive`, `neutral` ou `negative` |
| `sentiment_score` | float | Confiance ∈ [0.0, 1.0] |
| `sentiment_method` | string | `qwen3:8b` (Ollama) |
| `topic_id` | int | ID BERTopic (-1 = outlier) |
| `topic_label` | string | Libellé = top mots-clés du sujet |
| `topic_keywords` | string[] | 5 mots-clés max du sujet |
| `match_id` | string? | Identifiant du match corrélé |
| `match_timestamp` | string? | Horodatage de l'événement sportif le plus proche (±120 min) |
| `event_type` | string? | Type d'événement (`goal`, `red_card`, `kickoff`, …) |
| `event_minute` | string? | Minute du match |
| `team_home` / `team_away` | string? | Équipes du match |
| `partition` | object | Partition horaire MinIO source |
| `nlp_processed_at` | string | Horodatage traitement NLP |
| `nlp_model_sentiment` | string | Modèle Ollama (`qwen3:8b`) |
| `nlp_model_topics` | string | Modèle BERTopic + embeddings |

## Index

Créés automatiquement par `nlp_pipeline.py` :

| Index | Champs | Usage |
|-------|--------|-------|
| `idx_id` | `id` (unique) | Idempotence upsert |
| `idx_collected_at` | `collected_at` | Séries temporelles Superset |
| `idx_sentiment` | `sentiment_label` | Filtres sentiment |
| `idx_topic_id` | `topic_id` | Top topics |
| `idx_lang` | `lang` | Filtre langue |
| `idx_match_id` | `match_id` | Agrégats par match |
| `idx_partition` | `partition.year`, `partition.hour` | Requêtes par batch horaire |

## Collection `topic_aggregates` — top thèmes par heure / match

Recalculée automatiquement par `nlp_pipeline.py` après chaque batch NLP.

### Document agrégat horaire (`aggregate_type: "hourly"`)

```json
{
  "aggregate_type": "hourly",
  "partition": { "year": 2026, "month": 7, "day": 1, "hour": 14 },
  "post_count": 42,
  "top_topics": [
    { "topic_id": 2, "topic_label": "goal, remontada, comeback", "count": 15 },
    { "topic_id": 0, "topic_label": "referee, var, penalty", "count": 9 }
  ],
  "sentiment_breakdown": { "positive": 28, "neutral": 10, "negative": 4 },
  "avg_sentiment_score": 0.72,
  "aggregated_at": "2026-07-01T14:20:00+00:00"
}
```

### Document agrégat match (`aggregate_type: "match"`)

```json
{
  "aggregate_type": "match",
  "match_id": "wc2026-fr-pt",
  "team_home": "France",
  "team_away": "Portugal",
  "post_count": 87,
  "top_topics": [
    { "topic_id": 1, "topic_label": "penalty, referee, var", "count": 31 },
    { "topic_id": 2, "topic_label": "goal, minute, score", "count": 24 }
  ],
  "sentiment_breakdown": { "positive": 40, "neutral": 25, "negative": 22 },
  "avg_sentiment_score": 0.55,
  "events_breakdown": { "goal": 30, "red_card": 18, "kickoff": 5 },
  "aggregated_at": "2026-07-01T14:20:00+00:00"
}
```

## `match_events.csv` — corrélation, pas entraînement

> **Non, on n'entraîne aucun modèle sur ce fichier.** C'est normal et conforme à l'énoncé.

| Fichier | Rôle |
|---------|------|
| `match_events.csv` | **Métadonnées sportives** : buts, cartons, horaires → corréler le buzz aux événements |
| Modèles NLP | **Pré-entraînés** (Qwen3:8b, BERTopic) — l'énoncé interdit d'entraîner un modèle maison |

Le pipeline fait une **jointure temporelle** : si un post est publié dans les ±120 min d'un événement, il reçoit `match_id`, `event_type`, etc. Les agrégats par match comptent ensuite les top topics sur tous les posts corrélés.

```csv
match_id,event_type,event_minute,event_timestamp,team_home,team_away,description
wc2026-fr-pt,goal,78,2026-07-01T13:18:00+00:00,France,Portugal,But 78e minute
```

Variable d'environnement : `MATCH_EVENTS_PATH` (défaut `/opt/airflow/data/match_events.csv`).

Sans CSV, les champs `match_*` valent `null` — Personne 4 peut enrichir via PostgreSQL Gold.

## Requêtes utiles (Personne 4)

```javascript
// Sentiment moyen par heure
db.enriched_posts.aggregate([
  { $group: {
      _id: { year: "$partition.year", month: "$partition.month", day: "$partition.day", hour: "$partition.hour" },
      avg_score: { $avg: "$sentiment_score" },
      count: { $sum: 1 },
      positive: { $sum: { $cond: [{ $eq: ["$sentiment_label", "positive"] }, 1, 0] } },
      negative: { $sum: { $cond: [{ $eq: ["$sentiment_label", "negative"] }, 1, 0] } }
  }},
  { $sort: { "_id.year": 1, "_id.month": 1, "_id.day": 1, "_id.hour": 1 } }
])

// Top topics d'un match (collection pré-calculée)
db.topic_aggregates.find({ aggregate_type: "match", match_id: "wc2026-fr-pt" })

// Top topics d'une heure (collection pré-calculée)
db.topic_aggregates.find({
  aggregate_type: "hourly",
  "partition.year": 2026,
  "partition.month": 7,
  "partition.day": 1,
  "partition.hour": 14
})
```

## Exécution manuelle

```bash
# Depuis l'hôte (Ollama + MinIO + Mongo démarrés)
cd notebooks/nlp
pip install -r requirements.txt

export MINIO_ENDPOINT=http://localhost:9002
export OLLAMA_BASE_URL=http://localhost:11434
export MONGO_URI="mongodb://app:app12345@localhost:27017/?authSource=admin"

python nlp_pipeline.py
```
