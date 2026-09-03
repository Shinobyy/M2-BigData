"""Bronze -> Silver : construction du modèle en étoile, avec contrôles qualité.

Les requêtes elles-mêmes sont dans sql/silver/ : ce script ne fait que les
charger, y injecter le watermark et les envoyer à ClickHouse. Le SQL reste ainsi
lisible dans un éditeur SQL et copiable tel quel dans :8123/play pour déboguer.

Chaque étape suit le même principe :
  1. watermark : ne relire de bronze que ce qui n'a jamais été traité
  2. LIMIT 1 BY <clé> : ne garder qu'une version par clé DANS le lot entrant
  3. anti-jointure NOT IN : ne pas réécrire une ligne identique à celle déjà
     présente en silver -- en régime stable, 0 ligne écrite
Objectif : ne jamais insérer de doublon, plutôt que de le masquer à la lecture.

Les lignes écartées par les contrôles qualité sont tracées dans silver._rejets,
dans la même fenêtre de watermark que l'insertion.
"""

import json
import os
import sys
import requests
from dotenv import load_dotenv

load_dotenv()

clickhouse_host = os.getenv("CLICKHOUSE_HOST", "localhost")
clickhouse_port = os.getenv("CLICKHOUSE_PORT", "8123")
clickhouse_user = os.getenv("CLICKHOUSE_USER", "default")
clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "")

SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql", "silver")

# Table cible -> table source, pour savoir quel max(_ingested_at) fait avancer
# le watermark. C'est la seule information que le SQL ne porte pas lui-même.
SOURCES = {
    "dim_patient": "bronze.patients",
    "dim_service": "bronze.services",
    "dim_cim10": "bronze.cim10",
    "dim_ccam": "bronze.ccam",
    "fact_sejours": "bronze.sejours",
    "fact_monitoring": "bronze.monitoring",
    "fact_diagnostics": "bronze.diagnostics",
    "fact_actes": "bronze.actes",
}

# Ordre imposé par les dépendances : les dimensions avant les faits (contrôles
# d'intégrité référentielle), et fact_sejours avant les deux autres faits qui
# lui empruntent patient_id/service_code/age_at_admission.
TABLE_ORDER = ["dim_patient", "dim_service", "dim_cim10", "dim_ccam",
               "fact_sejours", "fact_monitoring", "fact_diagnostics", "fact_actes"]


def read_sql(file_name):
    with open(os.path.join(SQL_DIR, file_name), encoding="utf-8") as handle:
        return handle.read()


def run_query(query):
    return requests.post(
        f"http://{clickhouse_host}:{clickhouse_port}/",
        params={"query": query},
        auth=(clickhouse_user, clickhouse_password),
    )


def read_watermark(table_name):
    return (
        "(SELECT ifNull(max(last_ingested_at), toDateTime('1970-01-01')) "
        f"FROM silver._watermarks FINAL WHERE table_name = '{table_name}')"
    )


def advance_watermark(table_name, source_table):
    return run_query(
        f"INSERT INTO silver._watermarks (table_name, last_ingested_at) "
        f"SELECT '{table_name}', max(_ingested_at) FROM {source_table}"
    )


def written_rows(response):
    try:
        return int(json.loads(response.headers.get("X-ClickHouse-Summary", "")).get("written_rows", 0))
    except (ValueError, AttributeError):
        return 0


def main():
    failures = 0
    for name in TABLE_ORDER:
        watermark = read_watermark(name)

        response = run_query(read_sql(f"{name}.sql").replace("{watermark}", watermark))
        if response.status_code != 200:
            print(f"Failed on silver.{name}: {response.status_code} {response.text}")
            failures += 1
            continue
        inserted = written_rows(response)

        # Les rejets sont tracés AVANT l'avancement du watermark, pour couvrir
        # exactement la même fenêtre de données que l'insertion ci-dessus.
        rejected = 0
        rejects_file = f"{name}.rejects.sql"
        if os.path.exists(os.path.join(SQL_DIR, rejects_file)):
            reject_response = run_query(read_sql(rejects_file).replace("{watermark}", watermark))
            if reject_response.status_code != 200:
                print(f"Failed on rejets silver.{name}: "
                      f"{reject_response.status_code} {reject_response.text}")
                failures += 1
            else:
                rejected = written_rows(reject_response)

        advance_watermark(name, SOURCES[name])
        # Merge immédiat : la table persistée ne contient aucun doublon une fois
        # l'étape terminée, sans dépendre du merge asynchrone de ClickHouse.
        run_query(f"OPTIMIZE TABLE silver.{name} FINAL")

        suffix = f", {rejected} rejetée(s)" if rejected else ""
        print(f"Populated silver.{name}: {inserted} ligne(s) insérée(s){suffix}")

    return failures


if __name__ == "__main__":
    # Code de sortie non nul si une étape a échoué : sans ça, main.py
    # enchaînerait sur Gold avec un Silver incomplet et annoncerait un succès.
    sys.exit(1 if main() else 0)
