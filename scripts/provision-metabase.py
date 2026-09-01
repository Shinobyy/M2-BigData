"""Provisionne les connexions et dashboards Metabase via l'API.

Idempotent : relancer le script ne crée pas de doublon, il réutilise ce qui
existe déjà (recherche par nom).

Nécessite METABASE_API_KEY dans .env (Admin settings > Authentication > API keys).
"""

import os
import sys
import requests
from dotenv import load_dotenv

load_dotenv()

MB_URL = os.getenv("METABASE_URL", "http://localhost:3000")
MB_API_KEY = os.getenv("METABASE_API_KEY")

if not MB_API_KEY:
    sys.exit("METABASE_API_KEY manquante dans .env")

# Vu du conteneur Metabase, ClickHouse s'appelle "clickhouse" (réseau compose).
CH_HOST = "clickhouse"
CH_PORT = 8123

DB_PILOTAGE = "ClickHouse — Pilotage"
DB_RECHERCHE = "ClickHouse — Recherche"

CONNECTIONS = {
    DB_PILOTAGE: ("pilotage_user", "pilotage_pwd"),
    DB_RECHERCHE: ("recherche_user", "recherche_pwd"),
}

# Cartes du dashboard pilotage existant à rebrancher sur la connexion Pilotage.
PILOTAGE_DASHBOARD_ID = 2
PILOTAGE_CARD_IDS = [40, 41, 42, 43, 44, 45, 47, 49]

# La carte "Logs" lit bronze._ingestion_log, hors périmètre de pilotage_user :
# elle est retirée du dashboard pilotage (la carte elle-même n'est pas supprimée,
# elle reste consultable via la connexion admin).
LOGS_CARD_ID = 51

RECHERCHE_CARDS = [
    {
        "name": "Prévalence par pathologie",
        "description": "Taille des cohortes par diagnostic. Cohortes < 5 patients exclues (RGPD).",
        "display": "bar",
        "sql": """
            SELECT
                pathologie AS "Pathologie",
                taille_cohorte AS "Nombre de patients"
            FROM gold.recherche_prevalence_pathologie
            ORDER BY taille_cohorte DESC
        """,
        "viz": {
            "graph.dimensions": ["Pathologie"],
            "graph.metrics": ["Nombre de patients"],
        },
    },
    {
        "name": "Description de cohorte : âge × sexe",
        "description": "Distribution des patients par tranche d'âge et sexe. Cohortes < 5 patients exclues (RGPD).",
        "display": "bar",
        "sql": """
            SELECT
                age_group AS "Tranche d'âge",
                sex AS "Sexe",
                taille_cohorte AS "Nombre de patients"
            FROM gold.recherche_cohorte_age_sexe
            ORDER BY age_group, sex
        """,
        "viz": {
            "graph.dimensions": ["Tranche d'âge", "Sexe"],
            "graph.metrics": ["Nombre de patients"],
        },
    },
    {
        "name": "Cohortes disponibles (détail)",
        "description": "Vue tabulaire des cohortes diffusables.",
        "display": "table",
        "sql": """
            SELECT
                pathologie AS "Pathologie",
                taille_cohorte AS "Nombre de patients"
            FROM gold.recherche_prevalence_pathologie
            ORDER BY taille_cohorte DESC
        """,
        "viz": {},
    },
]

DASHBOARD_RECHERCHE = "Recherche clinique — Cohortes"


def api(method, path, payload=None):
    response = requests.request(
        method,
        f"{MB_URL}/api{path}",
        headers={"x-api-key": MB_API_KEY, "Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )
    if response.status_code >= 400:
        sys.exit(f"{method} {path} -> {response.status_code}\n{response.text[:500]}")
    return response.json() if response.content else None


def ensure_database(name, user, password):
    for db in api("GET", "/database")["data"]:
        if db["name"] == name:
            print(f"Connexion déjà présente : {name} (id {db['id']})")
            return db["id"]
    created = api("POST", "/database", {
        "name": name,
        "engine": "clickhouse",
        "details": {
            "host": CH_HOST,
            "port": CH_PORT,
            "user": user,
            "password": password,
            "ssl": False,
            "tunnel-enabled": False,
            "advanced-options": False,
            # Restreint la connexion à la base gold : les autres n'apparaissent
            # même pas dans l'interface, en plus d'être refusées par ClickHouse.
            "db-filters-type": "inclusion",
            "db-filters-patterns": "gold",
        },
    })
    print(f"Connexion créée : {name} (id {created['id']})")
    return created["id"]


def native_query(database_id, sql):
    return {
        "type": "native",
        "native": {"query": sql.strip()},
        "database": database_id,
    }


def rebranch_cards(card_ids, database_id):
    for card_id in card_ids:
        card = api("GET", f"/card/{card_id}")
        query = card["dataset_query"]
        if query.get("database") == database_id:
            print(f"Carte {card_id} « {card['name']} » déjà sur la bonne connexion")
            continue
        query["database"] = database_id
        api("PUT", f"/card/{card_id}", {"dataset_query": query})
        print(f"Carte {card_id} « {card['name']} » rebranchée")


def ensure_card(spec, database_id):
    for card in api("GET", "/card"):
        if card["name"] == spec["name"] and not card["archived"]:
            print(f"Carte déjà présente : {spec['name']} (id {card['id']})")
            return card["id"]
    created = api("POST", "/card", {
        "name": spec["name"],
        "description": spec["description"],
        "display": spec["display"],
        "dataset_query": native_query(database_id, spec["sql"]),
        "visualization_settings": spec["viz"],
    })
    print(f"Carte créée : {spec['name']} (id {created['id']})")
    return created["id"]


def ensure_dashboard(name, card_ids):
    dashboard = None
    for item in api("GET", "/dashboard"):
        if item["name"] == name and not item["archived"]:
            dashboard = item
            break
    if dashboard is None:
        dashboard = api("POST", "/dashboard", {
            "name": name,
            "description": "Vues destinées à la recherche clinique. "
                           "Accès restreint : cohortes agrégées uniquement, "
                           "aucune donnée au grain patient.",
        })
        print(f"Dashboard créé : {name} (id {dashboard['id']})")
    else:
        print(f"Dashboard déjà présent : {name} (id {dashboard['id']})")

    dashcards = []
    for index, card_id in enumerate(card_ids):
        dashcards.append({
            "id": -(index + 1),
            "card_id": card_id,
            "row": (index // 2) * 8,
            "col": (index % 2) * 12,
            "size_x": 12,
            "size_y": 8,
            "parameter_mappings": [],
            "visualization_settings": {},
        })
    api("PUT", f"/dashboard/{dashboard['id']}", {"dashcards": dashcards})
    print(f"Dashboard « {name} » : {len(card_ids)} carte(s) placée(s)")
    return dashboard["id"]


def remove_card_from_dashboard(dashboard_id, card_id):
    dashboard = api("GET", f"/dashboard/{dashboard_id}")
    kept = [dc for dc in dashboard["dashcards"] if dc.get("card_id") != card_id]
    if len(kept) == len(dashboard["dashcards"]):
        print(f"Carte {card_id} déjà absente du dashboard {dashboard_id}")
        return
    api("PUT", f"/dashboard/{dashboard_id}", {"dashcards": kept})
    print(f"Carte {card_id} retirée du dashboard {dashboard_id} (la carte n'est pas supprimée)")


def main():
    pilotage_db = ensure_database(DB_PILOTAGE, *CONNECTIONS[DB_PILOTAGE])
    recherche_db = ensure_database(DB_RECHERCHE, *CONNECTIONS[DB_RECHERCHE])

    print("\n-- Rebranchement du dashboard pilotage --")
    rebranch_cards(PILOTAGE_CARD_IDS, pilotage_db)
    remove_card_from_dashboard(PILOTAGE_DASHBOARD_ID, LOGS_CARD_ID)

    print("\n-- Dashboard recherche --")
    card_ids = [ensure_card(spec, recherche_db) for spec in RECHERCHE_CARDS]
    ensure_dashboard(DASHBOARD_RECHERCHE, card_ids)

    print("\nTerminé.")


if __name__ == "__main__":
    main()
