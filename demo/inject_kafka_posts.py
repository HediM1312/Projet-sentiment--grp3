"""
Script de démonstration — injecte des posts bruts dans Kafka.

Simule exactement ce que le collecteur fait : publie des messages dans
le topic `social-raw` avec le même schéma JSON.

Les posts couvrent un match générique de Coupe du Monde 2026.
Ils passent ensuite par le pipeline complet :
  Bronze (Parquet/MinIO) → Silver (dédup + langue + normalisation)
  → NLP (sentiment Qwen3:8b + BERTopic → MongoDB)
  → Gold (agrégats PostgreSQL)
  → Superset (dashboard)

Usage :
    # Depuis l'intérieur du réseau Docker (recommandé)
    docker cp demo/inject_kafka_posts.py grp3-airflow:/tmp/inject_kafka_posts.py
    docker exec grp3-airflow python /tmp/inject_kafka_posts.py

    # Ou directement si Kafka est exposé sur localhost:9092
    python demo/inject_kafka_posts.py
"""

import json
import hashlib
import logging
import time
from datetime import datetime, timezone, timedelta

try:
    from kafka import KafkaProducer
except ImportError:
    raise SystemExit("Installez kafka-python : pip install kafka-python")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "kafka:9092"   # réseau Docker interne
KAFKA_TOPIC     = "social-raw"

# Base temporelle : match fictif (exemple Coupe du Monde 2026)
MATCH_START = datetime(2026, 7, 2, 13, 0, 0, tzinfo=timezone.utc)

def post_id(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()

def ts(offset_minutes: int) -> str:
    return (MATCH_START + timedelta(minutes=offset_minutes)).isoformat()

# ── Posts bruts (même format que le collecteur Reddit/RSS) ──────────────────
# Langue, sentiment et heure variés pour tester tout le pipeline
POSTS = [
    # ── Avant le match (négatifs/anticipation) ──────────────────────────────
    {"text": "C'est bientôt le coup d'envoi ! J'espère que ça va bien se passer pour notre équipe ce soir #CoupeDuMonde2026",
     "source": "reddit", "subreddit": "soccer", "offset": -30},
    {"text": "I'm so nervous for this World Cup match, anything could happen at this level",
     "source": "reddit", "subreddit": "worldcup", "offset": -25},
    {"text": "Espero que el partido sea emocionante hoy, la Copa del Mundo siempre nos da sorpresas #FIFA2026",
     "source": "reddit", "subreddit": "football", "offset": -20},
    {"text": "Not feeling great about tonight's game, the team has been inconsistent lately",
     "source": "rss", "subreddit": None, "offset": -15},
    {"text": "Le stade est plein, l'ambiance est incroyable ! On va gagner ce soir ! Allez !",
     "source": "reddit", "subreddit": "soccer", "offset": -10},

    # ── Coup d'envoi (neutres) ───────────────────────────────────────────────
    {"text": "Le match vient de commencer, coup d'envoi donné ! #CoupeDuMonde",
     "source": "rss", "subreddit": None, "offset": 1},
    {"text": "Kick off! Here we go, World Cup 2026 knockout stage match underway",
     "source": "reddit", "subreddit": "worldcup", "offset": 2},
    {"text": "Partido en marcha, los primeros minutos son siempre cruciales en el Mundial",
     "source": "reddit", "subreddit": "football", "offset": 3},

    # ── Doublon intentionnel (pour tester la déduplication Silver) ──────────
    {"text": "Kick off! Here we go, World Cup 2026 knockout stage match underway",
     "source": "reddit", "subreddit": "worldcup", "offset": 4},

    # ── Premier but (~34e min) — très positifs ───────────────────────────────
    {"text": "BUUUUT !!! 1-0 !! Quel magnifique but ! Le stade explose ! #CoupeDuMonde2026",
     "source": "reddit", "subreddit": "soccer", "offset": 34},
    {"text": "GOAL!!! 1-0 what a strike! The crowd is going absolutely wild right now!!!",
     "source": "reddit", "subreddit": "worldcup", "offset": 34},
    {"text": "¡¡GOL!! ¡Increíble! El primer gol de la noche, qué golazo espectacular #Mundial2026",
     "source": "reddit", "subreddit": "football", "offset": 35},
    {"text": "What a goal!! I can't believe that just happened, pure class from the striker",
     "source": "reddit", "subreddit": "soccer", "offset": 36},
    {"text": "Premier but inscrit à la 34e minute ! L'équipe prend les commandes de ce match de Coupe du Monde",
     "source": "rss", "subreddit": None, "offset": 35},
    {"text": "Tor! 1:0! Das ist fantastisch, der erste Treffer des Abends beim WM-Spiel!",
     "source": "reddit", "subreddit": "worldcup", "offset": 36},

    # ── Mi-temps (mixtes) ────────────────────────────────────────────────────
    {"text": "Mi-temps : 1-0. Bonne première mi-temps mais il faut rester concentrés pour la seconde",
     "source": "rss", "subreddit": None, "offset": 47},
    {"text": "Half time 1-0, solid performance but the second half will be tough",
     "source": "reddit", "subreddit": "soccer", "offset": 46},
    {"text": "Descanso 1-0, hay que mantenerse concentrado en la segunda parte del partido",
     "source": "reddit", "subreddit": "football", "offset": 47},
    {"text": "L'équipe adverse pousse fort en ce début de seconde mi-temps, la défense doit tenir",
     "source": "reddit", "subreddit": "soccer", "offset": 55},
    {"text": "This is getting very tense, the opposition is pushing hard for an equalizer",
     "source": "reddit", "subreddit": "worldcup", "offset": 60},

    # ── Deuxième but (~78e min) — euphorie ──────────────────────────────────
    {"text": "2-0 !!! C'EST MAGNIFIQUE !!! On va en finale !! #CoupeDuMonde2026 #Qatar2026",
     "source": "reddit", "subreddit": "soccer", "offset": 78},
    {"text": "2-0!!! INCREDIBLE!!! The match is virtually over now! What a performance!!!",
     "source": "reddit", "subreddit": "worldcup", "offset": 78},
    {"text": "¡2-0! ¡Ya casi es imposible remontar! ¡Qué actuación tan brillante del equipo!",
     "source": "reddit", "subreddit": "football", "offset": 79},
    {"text": "Deuxième but à la 78e minute, l'équipe assure sa qualification pour la prochaine phase",
     "source": "rss", "subreddit": None, "offset": 78},
    {"text": "2:0 Tor! Das Spiel ist praktisch entschieden, hervorragende Leistung heute Abend!",
     "source": "reddit", "subreddit": "soccer", "offset": 79},

    # ── Fin du match (positifs/célébration) ─────────────────────────────────
    {"text": "VICTOIRE 2-0 !! Qualification obtenue ! Incroyable soirée de Coupe du Monde !",
     "source": "reddit", "subreddit": "soccer", "offset": 91},
    {"text": "Full time 2-0! Absolutely deserved win, brilliant team performance tonight!",
     "source": "reddit", "subreddit": "worldcup", "offset": 91},
    {"text": "¡Final del partido! ¡Victoria merecida! ¡Qué noche tan especial en el Mundial!",
     "source": "reddit", "subreddit": "football", "offset": 92},
    {"text": "Victoire 2-0, l'équipe se qualifie pour la prochaine phase de la Coupe du Monde 2026",
     "source": "rss", "subreddit": None, "offset": 91},
    {"text": "What a night of football! 2-0 victory, the players deserve all the credit for this",
     "source": "reddit", "subreddit": "soccer", "offset": 93},

    # ── Post-match (quelques négatifs — supporters adverses) ────────────────
    {"text": "Devastating result, we played so poorly tonight and didn't deserve anything from this game",
     "source": "reddit", "subreddit": "worldcup", "offset": 95},
    {"text": "Déception totale ce soir, on n'a pas su concrétiser nos occasions en première mi-temps",
     "source": "reddit", "subreddit": "soccer", "offset": 96},
]


def make_producer() -> KafkaProducer:
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            )
            log.info("Connecté à Kafka %s", KAFKA_BOOTSTRAP)
            return producer
        except Exception as exc:
            log.warning("Tentative %d/10 échouée : %s", attempt + 1, exc)
            time.sleep(3)
    raise RuntimeError("Impossible de connecter à Kafka.")


def run():
    producer = make_producer()
    now = datetime.now(timezone.utc).isoformat()
    sent = 0

    for p in POSTS:
        collected_at = (MATCH_START + timedelta(minutes=p["offset"])).isoformat()
        doc = {
            "id":           post_id(p["text"]),
            "source":       p["source"],
            "subreddit":    p.get("subreddit"),
            "author":       "demo_user",
            "text":         p["text"],
            "url":          f"https://reddit.com/r/{p.get('subreddit','demo')}/demo_{sent}",
            "score":        0,
            "collected_at": collected_at,
            "created_at":   collected_at,
        }
        producer.send(KAFKA_TOPIC, doc)
        sent += 1
        log.info("[%2d] → Kafka : %s", sent, p["text"][:60])

    producer.flush()
    producer.close()
    log.info("✓ %d posts publiés dans le topic '%s'", sent, KAFKA_TOPIC)
    log.info("  Dont 1 doublon intentionnel → doit être filtré par Silver")
    log.info("  Langues : fr, en, es, de → toutes dans ACCEPTED_LANGS")


if __name__ == "__main__":
    run()
