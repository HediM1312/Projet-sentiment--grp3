"""
Collecteur de posts publics — Projet 3 Sentiment & Tendances
Sources supportées :
  - Reddit  (via API publique JSON, sans clé)
  - RSS     (flux d'actualités sportives)
Publie chaque post dans le topic Kafka `social-raw`.
"""

import json
import time
import hashlib
import logging
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from xml.etree import ElementTree

from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "social-raw"

# Mots-clés à surveiller (Coupe du Monde)
KEYWORDS = [
    "coupe du monde", "world cup", "fifa2026", "worldcup2026",
    "football", "goal", "penalty", "carton rouge",
]

# Subreddits à scraper
REDDIT_SUBREDDITS = ["soccer", "football", "worldcup"]

# Flux RSS sportifs publics
RSS_FEEDS = [
    "https://www.football365.fr/feed",
    "https://www.lequipe.fr/rss/actu_rss_Football.xml",
]


# ── Kafka producer ────────────────────────────────────────────────────────────

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
            log.warning("Kafka indisponible (tentative %d/10) : %s", attempt + 1, exc)
            time.sleep(5)
    raise RuntimeError("Impossible de se connecter à Kafka après 10 tentatives.")


# ── Utilitaires ───────────────────────────────────────────────────────────────

def dedupe_id(text: str) -> str:
    """Identifiant de déduplication basé sur le contenu."""
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def fetch_url(url: str, timeout: int = 15) -> str:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; SentimentBot/1.0)"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


# ── Collecteurs ───────────────────────────────────────────────────────────────

def collect_reddit(producer: KafkaProducer, subreddit: str) -> int:
    """Lit les 25 derniers posts d'un subreddit via l'API JSON publique."""
    url = f"https://www.reddit.com/r/{subreddit}/new.json?limit=25"
    try:
        raw = fetch_url(url)
        data = json.loads(raw)
    except Exception as exc:
        log.warning("Reddit r/%s erreur : %s", subreddit, exc)
        return 0

    count = 0
    for post in data.get("data", {}).get("children", []):
        p = post.get("data", {})
        text = f"{p.get('title', '')} {p.get('selftext', '')}".strip()
        doc = {
            "id": dedupe_id(text),
            "source": "reddit",
            "subreddit": subreddit,
            "author": p.get("author", ""),
            "text": text[:2000],
            "url": f"https://reddit.com{p.get('permalink', '')}",
            "score": p.get("score", 0),
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "created_at": datetime.fromtimestamp(
                p.get("created_utc", 0), tz=timezone.utc
            ).isoformat(),
        }
        producer.send(KAFKA_TOPIC, doc)
        count += 1

    log.info("Reddit r/%s : %d posts envoyés", subreddit, count)
    return count


def collect_rss(producer: KafkaProducer, feed_url: str) -> int:
    """Lit un flux RSS et publie les articles dans Kafka."""
    try:
        raw = fetch_url(feed_url)
        root = ElementTree.fromstring(raw)
    except Exception as exc:
        log.warning("RSS %s erreur : %s", feed_url, exc)
        return 0

    count = 0
    ns = {"atom": "http://www.w3.org/2005/Atom"}

    for item in root.iter("item"):
        title = item.findtext("title") or ""
        desc = item.findtext("description") or ""
        link = item.findtext("link") or ""
        pub_date = item.findtext("pubDate") or ""
        text = f"{title} {desc}".strip()

        doc = {
            "id": dedupe_id(link or text),
            "source": "rss",
            "feed": feed_url,
            "text": text[:2000],
            "url": link,
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "created_at": pub_date,
        }
        producer.send(KAFKA_TOPIC, doc)
        count += 1

    log.info("RSS %s : %d articles envoyés", feed_url, count)
    return count


# ── Point d'entrée ────────────────────────────────────────────────────────────

def run_once():
    producer = make_producer()
    total = 0

    for sub in REDDIT_SUBREDDITS:
        total += collect_reddit(producer, sub)
        time.sleep(2)  # respecter le rate-limit Reddit

    for feed in RSS_FEEDS:
        total += collect_rss(producer, feed)

    producer.flush()
    producer.close()
    log.info("Collecte terminée — %d documents publiés dans Kafka", total)


if __name__ == "__main__":
    run_once()
