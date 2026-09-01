"""Silver -> Gold : calcul des indicateurs, une table par KPI.

Les requêtes sont dans sql/gold/, une par indicateur : le fichier porte le nom
de la table qu'il alimente, ce qui rend la correspondance KPI <-> requête
immédiate pour qui reprend le projet.

Pas de watermark ici, contrairement à Silver : un agrégat sur un mois change
dès qu'un jour de ce mois arrive, il faut donc le recalculer entièrement. En
revanche l'anti-jointure reste : seules les lignes dont la valeur a réellement
changé sont réécrites. En régime stable, 0 ligne écrite.

Aucun TRUNCATE : les tables restent lisibles pendant tout le cycle, les
ReplacingMergeTree se chargent d'écraser les versions précédentes.
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

SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql", "gold")

# Aucune dépendance entre ces tables (chacune lit Silver, jamais une autre table
# Gold) : l'ordre n'est ici que celui d'affichage dans les journaux.
TABLE_ORDER = ["dms_par_service", "urgences_par_jour", "readmission_par_service",
               "alertes_par_jour", "admissions_par_age",
               "prevalence_pathologie", "cohorte_age_sexe"]


def read_sql(file_name):
    with open(os.path.join(SQL_DIR, file_name), encoding="utf-8") as handle:
        return handle.read()


def run_query(query):
    return requests.post(
        f"http://{clickhouse_host}:{clickhouse_port}/",
        params={"query": query},
        auth=(clickhouse_user, clickhouse_password),
    )


def written_rows(response):
    try:
        return int(json.loads(response.headers.get("X-ClickHouse-Summary", "")).get("written_rows", 0))
    except (ValueError, AttributeError):
        return 0


def main():
    failures = 0
    for name in TABLE_ORDER:
        response = run_query(read_sql(f"{name}.sql"))
        if response.status_code != 200:
            print(f"Failed on gold.{name}: {response.status_code} {response.text}")
            failures += 1
            continue
        run_query(f"OPTIMIZE TABLE gold.{name} FINAL")
        print(f"Populated gold.{name}: {written_rows(response)} ligne(s) écrite(s)")
    return failures


if __name__ == "__main__":
    # Code de sortie non nul si une étape a échoué, pour que main.py et le
    # superviseur détectent l'incident sans avoir à parser les logs.
    sys.exit(1 if main() else 0)
