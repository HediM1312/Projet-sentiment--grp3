"""
superset_setup.py — Projet 3 Sentiment & Tendances — Groupe 3
Responsable : Meissa MARA (Personne 4 — Gold + Dashboard Superset)

Script de configuration automatique de Apache Superset via son API REST.

Ce script :
  1. Crée (ou retrouve) la connexion à la base PostgreSQL Gold
  2. Enregistre les datasets (tables Gold)
  3. Crée les charts (ligne sentiment vs timeline, heatmap par équipe, barres topics)
  4. Assemble le dashboard final

Prérequis :
  - Superset démarré (http://localhost:8089 ou SUPERSET_URL)
  - Admin créé (SUPERSET_ADMIN / SUPERSET_PASSWORD)
  - Tables Gold créées dans PostgreSQL (init_gold.sql déjà exécuté)

Usage :
    python superset_setup.py
    SUPERSET_URL=http://localhost:8089 python superset_setup.py
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

SUPERSET_URL      = os.getenv("SUPERSET_URL", "http://localhost:8089")
SUPERSET_ADMIN    = os.getenv("SUPERSET_ADMIN", "admin")
SUPERSET_PASSWORD = os.getenv("SUPERSET_PASSWORD", "admin")

PG_HOST     = os.getenv("PG_HOST", "localhost")
PG_PORT     = int(os.getenv("PG_PORT", "5432"))
PG_DB       = os.getenv("PG_DB", "gold")
PG_USER     = os.getenv("PG_USER", "app")
PG_PASSWORD = os.getenv("PG_PASSWORD", "app12345")

DB_NAME_IN_SUPERSET = "grp3-gold-postgres"
DASHBOARD_TITLE     = "Sentiment & Tendances — Coupe du Monde"


# ── Session Superset (login + CSRF) ──────────────────────────────────────────

class SupersetClient:
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self._login(username, password)

    def _login(self, username: str, password: str) -> None:
        # Récupération du token CSRF
        resp = self.session.get(f"{self.base_url}/api/v1/security/csrf_token/")
        resp.raise_for_status()
        csrf = resp.json().get("result", "")

        resp = self.session.post(
            f"{self.base_url}/api/v1/security/login",
            json={
                "username": username,
                "password": password,
                "provider":  "db",
                "refresh":   True,
            },
            headers={"X-CSRFToken": csrf, "Referer": self.base_url},
        )
        resp.raise_for_status()
        tokens = resp.json()
        access_token = tokens.get("access_token") or tokens.get("token")
        if not access_token:
            raise RuntimeError(f"Login Superset échoué : {resp.text[:300]}")

        self.session.headers.update(
            {
                "Authorization": f"Bearer {access_token}",
                "X-CSRFToken":   csrf,
                "Referer":       self.base_url,
            }
        )
        log.info("Superset : authentification OK.")

    def get(self, path: str, **kwargs) -> requests.Response:
        return self.session.get(f"{self.base_url}{path}", **kwargs)

    def post(self, path: str, **kwargs) -> requests.Response:
        return self.session.post(f"{self.base_url}{path}", **kwargs)

    def put(self, path: str, **kwargs) -> requests.Response:
        return self.session.put(f"{self.base_url}{path}", **kwargs)


# ── Database connection ───────────────────────────────────────────────────────

def get_or_create_database(client: SupersetClient) -> int:
    """Crée ou retrouve la connexion PostgreSQL Gold dans Superset. Retourne l'id."""
    # Cherche si elle existe déjà
    resp = client.get("/api/v1/database/", params={"q": json.dumps({"filters": []})})
    resp.raise_for_status()
    for db in resp.json().get("result", []):
        if db.get("database_name") == DB_NAME_IN_SUPERSET:
            log.info("Database Superset déjà existante : id=%d", db["id"])
            return db["id"]

    sqlalchemy_uri = (
        f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )
    payload = {
        "database_name": DB_NAME_IN_SUPERSET,
        "sqlalchemy_uri": sqlalchemy_uri,
        "expose_in_sqllab": True,
        "allow_run_async": True,
        "extra": json.dumps({"allows_virtual_table_explore": True}),
    }
    resp = client.post("/api/v1/database/", json=payload)
    resp.raise_for_status()
    db_id = resp.json()["id"]
    log.info("Database Superset créée : id=%d", db_id)
    return db_id


# ── Datasets ──────────────────────────────────────────────────────────────────

DATASETS = [
    {
        "table_name":  "hourly_sentiment",
        "schema":      "public",
        "description": "Agrégats horaires de sentiment (Gold)",
    },
    {
        "table_name":  "team_sentiment_heatmap",
        "schema":      "public",
        "description": "Heatmap sentiment par équipe × heure",
    },
    {
        "table_name":  "match_events_gold",
        "schema":      "public",
        "description": "Événements sportifs (buts, cartons, …)",
    },
    {
        "table_name":  "trend_summaries",
        "schema":      "public",
        "description": "Résumés narratifs LLM par heure/match",
    },
    {
        "table_name":  "v_sentiment_vs_events",
        "schema":      "public",
        "description": "Vue : sentiment vs timeline des buts (JOIN hourly × events)",
    },
]


def get_or_create_dataset(client: SupersetClient, db_id: int, ds: dict) -> int:
    """Crée ou retrouve un dataset (table/vue). Retourne l'id."""
    table_name = ds["table_name"]

    resp = client.get(
        "/api/v1/dataset/",
        params={
            "q": json.dumps({
                "filters": [
                    {"col": "table_name", "opr": "eq", "val": table_name},
                    {"col": "database", "opr": "rel_o_m", "val": db_id},
                ]
            })
        },
    )
    resp.raise_for_status()
    results = resp.json().get("result", [])
    if results:
        log.info("Dataset déjà existant : %s (id=%d)", table_name, results[0]["id"])
        return results[0]["id"]

    payload = {
        "database":   db_id,
        "table_name": table_name,
        "schema":     ds.get("schema", "public"),
        "description": ds.get("description", ""),
    }
    resp = client.post("/api/v1/dataset/", json=payload)
    resp.raise_for_status()
    ds_id = resp.json()["id"]
    log.info("Dataset créé : %s (id=%d)", table_name, ds_id)
    return ds_id


# ── Charts ────────────────────────────────────────────────────────────────────

def create_chart_sentiment_timeline(client: SupersetClient, dataset_id: int) -> int:
    """Courbe : sentiment moyen vs heure, annoté des événements de match."""
    payload = {
        "slice_name":   "Sentiment moyen vs Timeline des buts",
        "viz_type":     "echarts_timeseries_line",
        "datasource_id":  dataset_id,
        "datasource_type": "table",
        "params": json.dumps({
            "metrics": [
                {
                    "label":       "Sentiment moyen",
                    "expressionType": "SIMPLE",
                    "column":       {"column_name": "avg_sentiment_score"},
                    "aggregate":    "AVG",
                }
            ],
            "groupby":          [],
            "x_axis":           "hour_bucket",
            "time_grain_sqla":  "PT1H",
            "title":            "Sentiment moyen vs Timeline de match",
            "x_axis_title":     "Heure",
            "y_axis_title":     "Score sentiment moyen",
            "color_scheme":     "supersetColors",
            "rich_tooltip":     True,
            "show_legend":      True,
            "annotation_layers": [
                {
                    "annotationType":   "FORMULA",
                    "name":             "Ligne neutre (0.5)",
                    "value":            "0.5",
                    "style":            "dashed",
                    "color":            "#888888",
                    "showMarkers":      False,
                    "opacity":          "",
                    "width":            1,
                }
            ],
        }),
        "description": "Évolution horaire du sentiment agrégé, corrélé aux événements de match.",
    }
    resp = client.post("/api/v1/chart/", json=payload)
    resp.raise_for_status()
    chart_id = resp.json()["id"]
    log.info("Chart créé : Sentiment timeline (id=%d)", chart_id)
    return chart_id


def create_chart_team_heatmap(client: SupersetClient, dataset_id: int) -> int:
    """Heatmap : sentiment moyen par équipe × heure."""
    payload = {
        "slice_name":   "Heatmap sentiment par équipe",
        "viz_type":     "heatmap_v2",
        "datasource_id":   dataset_id,
        "datasource_type": "table",
        "params": json.dumps({
            "all_columns_x":   "team",
            "all_columns_y":   "hour_bucket",
            "metric": {
                "label":          "Sentiment moyen",
                "expressionType": "SIMPLE",
                "column":         {"column_name": "avg_sentiment_score"},
                "aggregate":      "AVG",
            },
            "time_grain_sqla":  "PT1H",
            "linear_color_scheme": "rdYlGn",
            "xscale_interval":  1,
            "yscale_interval":  1,
            "canvas_image_rendering": "pixelated",
            "normalize_across": "heatmap",
            "left_margin":      "auto",
            "bottom_margin":    "auto",
            "value_bounds":     [0, 1],
            "show_values":      True,
        }),
        "description": "Heatmap du sentiment (0=négatif, 1=positif) par équipe et par heure.",
    }
    resp = client.post("/api/v1/chart/", json=payload)
    resp.raise_for_status()
    chart_id = resp.json()["id"]
    log.info("Chart créé : Heatmap équipes (id=%d)", chart_id)
    return chart_id


def create_chart_topic_volume(client: SupersetClient, dataset_id: int) -> int:
    """Barres : volume de posts par heure (positive/neutre/négative)."""
    payload = {
        "slice_name":   "Volume de posts par heure et sentiment",
        "viz_type":     "echarts_timeseries_bar",
        "datasource_id":   dataset_id,
        "datasource_type": "table",
        "params": json.dumps({
            "metrics": [
                {
                    "label":          "Positifs",
                    "expressionType": "SIMPLE",
                    "column":         {"column_name": "positive_count"},
                    "aggregate":      "SUM",
                },
                {
                    "label":          "Neutres",
                    "expressionType": "SIMPLE",
                    "column":         {"column_name": "neutral_count"},
                    "aggregate":      "SUM",
                },
                {
                    "label":          "Négatifs",
                    "expressionType": "SIMPLE",
                    "column":         {"column_name": "negative_count"},
                    "aggregate":      "SUM",
                },
            ],
            "groupby":         [],
            "x_axis":          "hour_bucket",
            "time_grain_sqla": "PT1H",
            "stack":           True,
            "color_scheme":    "bnbColors",
            "y_axis_title":    "Nombre de posts",
            "x_axis_title":    "Heure",
        }),
        "description": "Volume horaire des posts ventilé par polarité de sentiment.",
    }
    resp = client.post("/api/v1/chart/", json=payload)
    resp.raise_for_status()
    chart_id = resp.json()["id"]
    log.info("Chart créé : Volume posts (id=%d)", chart_id)
    return chart_id


def create_chart_summaries_table(client: SupersetClient, dataset_id: int) -> int:
    """Table : résumés LLM par heure/match."""
    payload = {
        "slice_name":   "Résumés de tendances (LLM)",
        "viz_type":     "table",
        "datasource_id":   dataset_id,
        "datasource_type": "table",
        "params": json.dumps({
            "all_columns": ["hour_bucket", "match_id", "summary", "avg_sentiment", "post_count"],
            "order_by_cols": [["hour_bucket", False]],
            "page_length":    20,
            "include_time":   False,
            "table_timestamp_format": "YYYY-MM-DD HH:mm",
        }),
        "description": "Résumés narratifs générés par Qwen3:8b pour chaque heure de match.",
    }
    resp = client.post("/api/v1/chart/", json=payload)
    resp.raise_for_status()
    chart_id = resp.json()["id"]
    log.info("Chart créé : Table résumés LLM (id=%d)", chart_id)
    return chart_id


# ── Dashboard ─────────────────────────────────────────────────────────────────

def create_dashboard(client: SupersetClient, chart_ids: list[int]) -> int:
    """Crée le dashboard principal et y place les charts."""
    # Vérifie si le dashboard existe déjà
    resp = client.get("/api/v1/dashboard/")
    resp.raise_for_status()
    for db in resp.json().get("result", []):
        if db.get("dashboard_title") == DASHBOARD_TITLE:
            log.info("Dashboard déjà existant : id=%d", db["id"])
            return db["id"]

    # Layout JSON (grille Superset)
    position_json = _build_dashboard_layout(chart_ids)

    payload = {
        "dashboard_title": DASHBOARD_TITLE,
        "status":          "published",
        "position_json":   json.dumps(position_json),
        "metadata":        json.dumps({"color_scheme": "supersetColors", "refresh_frequency": 3600}),
    }
    resp = client.post("/api/v1/dashboard/", json=payload)
    resp.raise_for_status()
    dash_id = resp.json()["id"]
    log.info("Dashboard créé : %s (id=%d)", DASHBOARD_TITLE, dash_id)
    return dash_id


def _build_dashboard_layout(chart_ids: list[int]) -> dict:
    """Génère le position_json Superset pour une grille 2×2 + 1 rangée."""
    root = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"children": ["GRID_ID"], "id": "ROOT_ID", "type": "ROOT"},
        "GRID_ID": {
            "children": ["ROW-1", "ROW-2"],
            "id":       "GRID_ID",
            "type":     "GRID",
        },
        "ROW-1": {
            "children": [],
            "id":       "ROW-1",
            "meta":     {"background": "BACKGROUND_TRANSPARENT"},
            "type":     "ROW",
        },
        "ROW-2": {
            "children": [],
            "id":       "ROW-2",
            "meta":     {"background": "BACKGROUND_TRANSPARENT"},
            "type":     "ROW",
        },
    }

    row_assignments = [
        ("ROW-1", 0, 12),  # chart 0 → ligne 1, largeur 12/2
        ("ROW-1", 1, 12),  # chart 1 → ligne 1, largeur 12/2
        ("ROW-2", 2, 12),  # chart 2 → ligne 2
        ("ROW-2", 3, 12),  # chart 3 → ligne 2
    ]

    for i, (chart_id) in enumerate(chart_ids[:4]):
        row_key, col, width = row_assignments[i]
        component_id = f"CHART-{i + 1}"
        root[component_id] = {
            "children": [],
            "id":       component_id,
            "meta":     {
                "chartId": chart_id,
                "height":  50,
                "sliceName": f"Chart {i + 1}",
                "width":   width // 2,
            },
            "type": "CHART",
        }
        root[row_key]["children"].append(component_id)

    return root


# ── Point d'entrée ────────────────────────────────────────────────────────────

def setup_superset() -> None:
    log.info("=== Superset Setup — Projet 3 Sentiment & Tendances ===")

    # Attendre que Superset soit prêt
    for attempt in range(10):
        try:
            resp = requests.get(f"{SUPERSET_URL}/health", timeout=5)
            if resp.status_code == 200:
                break
        except requests.RequestException:
            pass
        log.info("Superset pas encore prêt, attente... (%d/10)", attempt + 1)
        time.sleep(6)
    else:
        log.error("Superset inaccessible après 60 s — abandon.")
        sys.exit(1)

    client = SupersetClient(SUPERSET_URL, SUPERSET_ADMIN, SUPERSET_PASSWORD)

    # 1. Base de données
    db_id = get_or_create_database(client)

    # 2. Datasets
    dataset_ids: dict[str, int] = {}
    for ds in DATASETS:
        dataset_ids[ds["table_name"]] = get_or_create_dataset(client, db_id, ds)

    # 3. Charts
    chart_ids = [
        create_chart_sentiment_timeline(client, dataset_ids["v_sentiment_vs_events"]),
        create_chart_team_heatmap(client, dataset_ids["team_sentiment_heatmap"]),
        create_chart_topic_volume(client, dataset_ids["hourly_sentiment"]),
        create_chart_summaries_table(client, dataset_ids["trend_summaries"]),
    ]

    # 4. Dashboard
    dash_id = create_dashboard(client, chart_ids)

    log.info("=== Setup terminé. Dashboard id=%d ===", dash_id)
    log.info("Accès : %s/superset/dashboard/%d/", SUPERSET_URL, dash_id)


if __name__ == "__main__":
    setup_superset()
