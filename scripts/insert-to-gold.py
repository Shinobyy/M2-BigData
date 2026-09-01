"""Silver -> Gold : calcul des indicateurs, une table par KPI.

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

# Le découpage en tranches vit ici, et non en Silver : c'est un choix de
# présentation, pas une propriété de la donnée. Silver stocke l'âge révolu à
# l'événement ; changer les bornes ne demande donc que de recalculer Gold.
# Constante partagée par les deux KPI qui l'utilisent, pour qu'ils ne puissent
# pas diverger.
AGE_GROUP = """
    multiIf(
        {age} < 18, '0-17',
        {age} <= 35, '18-35',
        {age} <= 50, '36-50',
        {age} <= 65, '51-65',
        '66+'
    )
"""

clickhouse_host = os.getenv("CLICKHOUSE_HOST", "localhost")
clickhouse_port = os.getenv("CLICKHOUSE_PORT", "8123")
clickhouse_user = os.getenv("CLICKHOUSE_USER", "default")
clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "")

GOLD_QUERIES = {
    # DMS par service et par mois. Les séjours en cours sont exclus de la
    # moyenne (durée inconnue) mais comptés à part.
    "dms_par_service": """
        INSERT INTO gold.dms_par_service
        SELECT service_code, service_label, mois, nb_sejours_termines,
               nb_sejours_en_cours, dms_jours, now() AS _processed_at
        FROM (
            SELECT
                f.service_code AS service_code,
                d.service_label AS service_label,
                toStartOfMonth(f.admission_ts) AS mois,
                countIf(f.discharge_ts IS NOT NULL) AS nb_sejours_termines,
                countIf(f.discharge_ts IS NULL) AS nb_sejours_en_cours,
                round(avgIf(f.duree_sejour_h, f.duree_sejour_h IS NOT NULL) / 24, 2) AS dms_jours
            FROM silver.fact_sejours AS f FINAL
            JOIN silver.dim_service AS d FINAL ON f.service_code = d.service_code
            GROUP BY service_code, service_label, mois
        )
        WHERE (service_code, mois, nb_sejours_termines, nb_sejours_en_cours, dms_jours) NOT IN (
            SELECT service_code, mois, nb_sejours_termines, nb_sejours_en_cours, dms_jours
            FROM gold.dms_par_service FINAL
        )
    """,
    "urgences_par_jour": """
        INSERT INTO gold.urgences_par_jour
        SELECT jour, nb_passages, now() AS _processed_at
        FROM (
            SELECT toDate(admission_ts) AS jour, count() AS nb_passages
            FROM silver.fact_sejours FINAL
            WHERE service_code = 'URGENCES'
            GROUP BY jour
        )
        WHERE (jour, nb_passages) NOT IN (
            SELECT jour, nb_passages FROM gold.urgences_par_jour FINAL
        )
    """,
    "readmission_par_service": """
        INSERT INTO gold.readmission_par_service
        SELECT service_code, service_label, mois, nb_sejours, nb_readmissions,
               taux_readmission_pct, now() AS _processed_at
        FROM (
            SELECT
                f.service_code AS service_code,
                d.service_label AS service_label,
                toStartOfMonth(f.admission_ts) AS mois,
                count() AS nb_sejours,
                sum(f.readmission_30j) AS nb_readmissions,
                round(100 * avg(f.readmission_30j), 2) AS taux_readmission_pct
            FROM silver.fact_sejours AS f FINAL
            JOIN silver.dim_service AS d FINAL ON f.service_code = d.service_code
            GROUP BY service_code, service_label, mois
        )
        WHERE (service_code, mois, nb_sejours, nb_readmissions, taux_readmission_pct) NOT IN (
            SELECT service_code, mois, nb_sejours, nb_readmissions, taux_readmission_pct
            FROM gold.readmission_par_service FINAL
        )
    """,
    # nb_alertes_total compte les relevés en alerte, pas la somme des trois
    # colonnes : un même relevé peut violer plusieurs seuils.
    "alertes_par_jour": """
        INSERT INTO gold.alertes_par_jour
        SELECT jour, service_code, service_label, nb_releves, nb_alertes_fc,
               nb_alertes_spo2, nb_alertes_temp, nb_alertes_total,
               nb_sejours_concernes, now() AS _processed_at
        FROM (
            SELECT
                toDate(m.ts) AS jour,
                m.service_code AS service_code,
                d.service_label AS service_label,
                count() AS nb_releves,
                countIf(m.is_alerte_fc = 1) AS nb_alertes_fc,
                countIf(m.is_alerte_spo2 = 1) AS nb_alertes_spo2,
                countIf(m.is_alerte_temp = 1) AS nb_alertes_temp,
                countIf(m.is_alerte_fc = 1 OR m.is_alerte_spo2 = 1 OR m.is_alerte_temp = 1) AS nb_alertes_total,
                uniqExactIf(m.stay_id, m.is_alerte_fc = 1 OR m.is_alerte_spo2 = 1 OR m.is_alerte_temp = 1) AS nb_sejours_concernes
            FROM silver.fact_monitoring m
            JOIN silver.dim_service AS d FINAL ON m.service_code = d.service_code
            GROUP BY jour, service_code, service_label
        )
        WHERE (jour, service_code, nb_releves, nb_alertes_fc, nb_alertes_spo2,
               nb_alertes_temp, nb_alertes_total, nb_sejours_concernes) NOT IN (
            SELECT jour, service_code, nb_releves, nb_alertes_fc, nb_alertes_spo2,
                   nb_alertes_temp, nb_alertes_total, nb_sejours_concernes
            FROM gold.alertes_par_jour FINAL
        )
    """,
    # Tranche d'âge À L'ADMISSION, lue sur le fait : un patient hospitalisé à
    # 17 ans puis à 19 ans compte dans deux tranches, ce qui est le
    # comportement voulu. Conséquence à connaître : la somme des nb_patients
    # peut dépasser le nombre de patients distincts, puisqu'un patient au long
    # cours peut légitimement figurer dans plusieurs tranches.
    "admissions_par_age": f"""
        INSERT INTO gold.admissions_par_age
        SELECT age_group, nb_admissions, nb_patients, now() AS _processed_at
        FROM (
            SELECT
                {AGE_GROUP.format(age="age_at_admission")} AS age_group,
                count() AS nb_admissions,
                uniqExact(patient_id) AS nb_patients
            FROM silver.fact_sejours FINAL
            GROUP BY age_group
        )
        WHERE (age_group, nb_admissions, nb_patients) NOT IN (
            SELECT age_group, nb_admissions, nb_patients FROM gold.admissions_par_age FINAL
        )
    """,
    # RGPD petits effectifs : le HAVING filtre à l'écriture, donc aucune cohorte
    # de moins de 5 patients n'est matérialisée dans l'entrepôt.
    "prevalence_pathologie": """
        INSERT INTO gold.prevalence_pathologie
        SELECT code_cim10, pathologie, taille_cohorte, nb_sejours, now() AS _processed_at
        FROM (
            SELECT
                f.code_cim10 AS code_cim10,
                c.libelle AS pathologie,
                uniqExact(f.patient_id) AS taille_cohorte,
                uniqExact(f.stay_id) AS nb_sejours
            FROM silver.fact_diagnostics AS f FINAL
            JOIN silver.dim_cim10 AS c FINAL ON f.code_cim10 = c.code_cim10
            GROUP BY code_cim10, pathologie
            HAVING taille_cohorte >= 5
        )
        WHERE (code_cim10, taille_cohorte, nb_sejours) NOT IN (
            SELECT code_cim10, taille_cohorte, nb_sejours FROM gold.prevalence_pathologie FINAL
        )
    """,
    # Description de cohorte : l'âge retenu est celui À L'INCLUSION, c'est-à-dire
    # à la première admission connue du patient (argMin sur admission_ts). C'est
    # la convention de la recherche clinique -- le "Table 1" d'un article décrit
    # la population à l'entrée dans l'étude, pas à la date de publication -- et
    # c'est la seule définition qui ne dérive pas avec le temps.
    # Conséquence assumée : la cohorte porte sur les patients ayant au moins un
    # séjour, et non sur l'ensemble des patients connus du référentiel.
    "cohorte_age_sexe": f"""
        INSERT INTO gold.cohorte_age_sexe
        SELECT age_group, sex, taille_cohorte, now() AS _processed_at
        FROM (
            SELECT
                {AGE_GROUP.format(age="i.age_inclusion")} AS age_group,
                p.sex AS sex,
                uniqExact(i.patient_id) AS taille_cohorte
            FROM (
                SELECT patient_id,
                       argMin(age_at_admission, admission_ts) AS age_inclusion
                FROM silver.fact_sejours FINAL
                GROUP BY patient_id
            ) AS i
            JOIN silver.dim_patient AS p FINAL ON i.patient_id = p.patient_id
            GROUP BY age_group, sex
            HAVING taille_cohorte >= 5
        )
        WHERE (age_group, sex, taille_cohorte) NOT IN (
            SELECT age_group, sex, taille_cohorte FROM gold.cohorte_age_sexe FINAL
        )
    """,
}

TABLE_ORDER = ["dms_par_service", "urgences_par_jour", "readmission_par_service",
               "alertes_par_jour", "admissions_par_age",
               "prevalence_pathologie", "cohorte_age_sexe"]


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
        response = run_query(GOLD_QUERIES[name])
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
