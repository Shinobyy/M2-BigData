"""Provisionne les connexions et les cartes Metabase via l'API.

Idempotent : relancer le script ne crée pas de doublon, il réutilise ce qui
existe déjà (recherche par nom) et met à jour les requêtes en place.

Les cartes lisent directement les tables d'indicateurs de gold : elles
n'agrègent pas, les KPI sont déjà calculés par le pipeline.

Nécessite METABASE_API_KEY dans .env (Admin settings > Authentication > API keys,
groupe Administrators).
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

PILOTAGE_CARDS = [
    {
        "name": "Durée Moyenne du Séjour (DMS)",
        "description": "DMS par service. Les séjours en cours sont exclus du calcul.",
        "display": "bar",
        "sql": """
            SELECT service_label AS "Service", dms_jours AS "DMS (jours)"
            FROM gold.dms_par_service
            ORDER BY dms_jours DESC
        """,
        "viz": {"graph.dimensions": ["Service"], "graph.metrics": ["DMS (jours)"]},
    },
    {
        "name": "DMS - Détails",
        "description": "Volumes de séjours par service, terminés et en cours.",
        "display": "table",
        "sql": """
            SELECT
                service_label AS "Service",
                dms_jours AS "DMS (jours)",
                nb_sejours_termines AS "Séjours terminés",
                nb_sejours_en_cours AS "Séjours en cours"
            FROM gold.dms_par_service
            ORDER BY dms_jours DESC
        """,
        "viz": {},
    },
    {
        "name": "Activité des urgences",
        "description": "Nombre de passages aux urgences par jour.",
        "display": "bar",
        "sql": """
            SELECT jour AS "Jour", nb_passages AS "Nombre de passages"
            FROM gold.urgences_par_jour
            ORDER BY jour
        """,
        "viz": {"graph.dimensions": ["Jour"], "graph.metrics": ["Nombre de passages"]},
    },
    {
        "name": "Taux de réadmission à 30 jours",
        "description": "Part des séjours suivant une sortie de moins de 30 jours, "
                       "tous services confondus.",
        "display": "line",
        "sql": """
            SELECT mois AS "Mois",
                   taux_readmission_pct AS "Taux de réadmission (%)"
            FROM gold.readmission_par_mois
            ORDER BY mois
        """,
        "viz": {"graph.dimensions": ["Mois"], "graph.metrics": ["Taux de réadmission (%)"]},
    },
    {
        "name": "Taux de réadmission - Détails",
        "description": "Volumes sous-jacents au taux de réadmission.",
        "display": "table",
        "sql": """
            SELECT
                mois AS "Mois",
                nb_sejours AS "Nombre de séjours",
                nb_readmissions AS "Réadmissions",
                taux_readmission_pct AS "Taux de réadmission (%)"
            FROM gold.readmission_par_mois
            ORDER BY mois
        """,
        "viz": {},
    },
    {
        "name": "Surveillance des constantes",
        "description": "Relevés en alerte par jour, ventilés par type de constante.",
        "display": "area",
        "sql": """
            SELECT
                jour AS "Jour",
                nb_alertes_fc AS "Alertes FC",
                nb_alertes_spo2 AS "Alertes SpO2",
                nb_alertes_temp AS "Alertes température"
            FROM gold.alertes_par_jour
            ORDER BY jour
        """,
        "viz": {
            "graph.dimensions": ["Jour"],
            "graph.metrics": ["Alertes FC", "Alertes SpO2", "Alertes température"],
            "stackable.stack_type": "stacked",
        },
    },
    {
        "name": "Taux d'alerte et séjours concernés",
        "description": "Part des relevés en alerte et nombre de séjours touchés, "
                       "par jour, tous services confondus.",
        "display": "line",
        "sql": """
            SELECT jour AS "Jour",
                   round(100 * nb_alertes_total / nb_releves, 2) AS "Taux d'alerte (%)",
                   nb_sejours_concernes AS "Séjours concernés"
            FROM gold.alertes_par_jour
            ORDER BY jour
        """,
        "viz": {
            "graph.dimensions": ["Jour"],
            "graph.metrics": ["Taux d'alerte (%)", "Séjours concernés"],
        },
    },
    {
        "name": "Répartition par âge",
        "description": "Admissions par tranche d'âge.",
        "display": "bar",
        "sql": """
            SELECT age_group AS "Tranche d'âge", nb_admissions AS "Nombre d'admissions"
            FROM gold.admissions_par_age
            ORDER BY age_group
        """,
        "viz": {"graph.dimensions": ["Tranche d'âge"], "graph.metrics": ["Nombre d'admissions"]},
    },
    {
        "name": "DMS par catégorie de service",
        "description": "Durée moyenne de séjour au niveau catégorie, un cran au-dessus "
                       "du service dans la hiérarchie.",
        "display": "bar",
        "sql": """
            SELECT categorie AS "Catégorie", dms_jours AS "DMS (jours)"
            FROM gold.activite_dms_par_categorie_service
            ORDER BY dms_jours DESC
        """,
        "viz": {"graph.dimensions": ["Catégorie"], "graph.metrics": ["DMS (jours)"]},
    },
    {
        "name": "Actes réalisés par service",
        "description": "Volume d'actes CCAM par service.",
        "display": "bar",
        "sql": """
            SELECT service_code AS "Service", nb_actes AS "Nombre d'actes"
            FROM gold.actes_par_service
            ORDER BY nb_actes DESC
        """,
        "viz": {"graph.dimensions": ["Service"], "graph.metrics": ["Nombre d'actes"]},
    },
    {
        "name": "Actes les plus réalisés",
        "description": "Volume par acte CCAM, avec le nombre de séjours concernés : "
                       "300 actes sur 12 séjours ne se lit pas comme 300 sur 280.",
        "display": "table",
        "sql": """
            SELECT
                libelle AS "Acte",
                nb_actes AS "Nombre d'actes",
                nb_sejours_concernes AS "Séjours concernés"
            FROM gold.actes_par_code_ccam
            ORDER BY nb_actes DESC
        """,
        "viz": {},
    },
    {
        "name": "Densité d'actes par lit",
        "description": "Actes rapportés à la capacité du service. Un service absent du "
                       "référentiel de description sort à 0 (capacité inconnue).",
        "display": "bar",
        "sql": """
            SELECT service_code AS "Service",
                   densite_actes_par_lits AS "Actes par lit"
            FROM gold.densite_actes_par_lits
            ORDER BY densite_actes_par_lits DESC
        """,
        "viz": {"graph.dimensions": ["Service"], "graph.metrics": ["Actes par lit"]},
    },
    {
        "name": "Recettes T2A par service",
        "description": "Somme des tarifs des actes réalisés, par service.",
        "display": "bar",
        "sql": """
            SELECT service_code AS "Service",
                   montant_facture_euros AS "Recettes (euros)",
                   nb_actes AS "Nombre d'actes"
            FROM gold.montant_facture_par_service
            ORDER BY montant_facture_euros DESC
        """,
        "viz": {"graph.dimensions": ["Service"], "graph.metrics": ["Recettes (euros)"]},
    },
]

RECHERCHE_CARDS = [
    {
        "name": "Prévalence par pathologie",
        "description": "Taille des cohortes par diagnostic. Cohortes < 5 patients exclues (RGPD).",
        "display": "bar",
        "sql": """
            SELECT pathologie AS "Pathologie", taille_cohorte AS "Nombre de patients"
            FROM gold.prevalence_pathologie
            ORDER BY taille_cohorte DESC
        """,
        "viz": {"graph.dimensions": ["Pathologie"], "graph.metrics": ["Nombre de patients"]},
    },
    {
        "name": "Description de cohorte : âge × sexe",
        "description": "Distribution des patients par tranche d'âge et sexe. "
                       "Cohortes < 5 patients exclues (RGPD).",
        "display": "bar",
        "sql": """
            SELECT age_group AS "Tranche d'âge", sex AS "Sexe",
                   taille_cohorte AS "Nombre de patients"
            FROM gold.cohorte_age_sexe
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
                taille_cohorte AS "Nombre de patients",
                nb_sejours AS "Nombre de séjours"
            FROM gold.prevalence_pathologie
            ORDER BY taille_cohorte DESC
        """,
        "viz": {},
    },
]

DASHBOARD_PILOTAGE = "Dashboard ClickHouse - Big Data"
DASHBOARD_RECHERCHE = "Recherche clinique — Cohortes"


def api(method, path, payload=None):
    response = requests.request(
        method,
        f"{MB_URL}/api{path}",
        headers={"x-api-key": MB_API_KEY, "Content-Type": "application/json"},
        json=payload,
        timeout=60,
    )
    if response.status_code >= 400:
        sys.exit(f"{method} {path} -> {response.status_code}\n{response.text[:500]}")
    return response.json() if response.content else None


def ensure_database(name, user, password):
    for db in api("GET", "/database")["data"]:
        if db["name"] == name:
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


def upsert_card(spec, database_id):
    """Crée la carte, ou met à jour sa requête et sa connexion si elle existe."""
    payload = {
        "name": spec["name"],
        "description": spec["description"],
        "display": spec["display"],
        "dataset_query": {
            "type": "native",
            "native": {"query": spec["sql"].strip()},
            "database": database_id,
        },
        "visualization_settings": spec["viz"],
    }
    for card in api("GET", "/card"):
        if card["name"] == spec["name"] and not card["archived"]:
            api("PUT", f"/card/{card['id']}", payload)
            print(f"Carte mise à jour : {spec['name']} (id {card['id']})")
            return card["id"]
    created = api("POST", "/card", payload)
    print(f"Carte créée : {spec['name']} (id {created['id']})")
    return created["id"]


def ensure_dashboard(name, card_ids, description=None):
    dashboard = None
    for item in api("GET", "/dashboard"):
        if item["name"] == name and not item["archived"]:
            dashboard = item
            break
    if dashboard is None:
        dashboard = api("POST", "/dashboard", {"name": name, "description": description or ""})
        print(f"Dashboard créé : {name} (id {dashboard['id']})")

    dashcards = [{
        "id": -(index + 1),
        "card_id": card_id,
        "row": (index // 2) * 8,
        "col": (index % 2) * 12,
        "size_x": 12,
        "size_y": 8,
        "parameter_mappings": [],
        "visualization_settings": {},
    } for index, card_id in enumerate(card_ids)]
    api("PUT", f"/dashboard/{dashboard['id']}", {"dashcards": dashcards})
    print(f"Dashboard « {name} » : {len(card_ids)} carte(s) placée(s)")


def main():
    pilotage_db = ensure_database(DB_PILOTAGE, *CONNECTIONS[DB_PILOTAGE])
    recherche_db = ensure_database(DB_RECHERCHE, *CONNECTIONS[DB_RECHERCHE])

    print("\n-- Dashboard pilotage --")
    pilotage_ids = [upsert_card(spec, pilotage_db) for spec in PILOTAGE_CARDS]
    ensure_dashboard(DASHBOARD_PILOTAGE, pilotage_ids,
                     "Indicateurs de pilotage hospitalier, lus directement "
                     "depuis les tables d'indicateurs de la couche gold.")

    print("\n-- Dashboard recherche --")
    recherche_ids = [upsert_card(spec, recherche_db) for spec in RECHERCHE_CARDS]
    ensure_dashboard(DASHBOARD_RECHERCHE, recherche_ids,
                     "Vues destinées à la recherche clinique. Accès restreint : "
                     "cohortes agrégées uniquement, aucune donnée au grain patient.")

    print("\nTerminé.")


if __name__ == "__main__":
    main()
