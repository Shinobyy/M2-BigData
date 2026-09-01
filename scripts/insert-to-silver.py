"""Bronze -> Silver : construction du modèle en étoile, avec contrôles qualité.

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

SILVER_STEPS = {
    # Contrôles : date de naissance renseignée et non future, sexe normalisable.
    # Doublons (retour quotidien du même patient) -> version la plus récente.
    # Aucune colonne d'âge : voir le commentaire de silver.dim_patient dans le
    # DDL. L'âge est une mesure de l'événement, il est porté par les faits.
    "dim_patient": {
        "source": "bronze.patients",
        "query": """
            INSERT INTO silver.dim_patient
            SELECT patient_id, birth_date, sex, region_code, _ingested_at
            FROM (
                SELECT
                    patient_id, birth_date, upper(sex) AS sex, region_code,
                    _ingested_at
                FROM bronze.patients
                WHERE isNotNull(birth_date) AND birth_date <= today()
                  AND upper(sex) IN ('M', 'F')
                  AND _ingested_at > {watermark}
                ORDER BY _ingested_at DESC
                LIMIT 1 BY patient_id
            )
            WHERE (patient_id, birth_date, sex, region_code) NOT IN (
                SELECT patient_id, birth_date, sex, region_code
                FROM silver.dim_patient FINAL
            )
        """,
        "rejects": """
            INSERT INTO silver._rejets (source_table, regle, cle, details, _ingested_at)
            SELECT
                'bronze.patients',
                multiIf(
                    isNull(birth_date), 'date de naissance manquante',
                    birth_date > today(), 'date de naissance dans le futur',
                    'sexe non normalisable (attendu M ou F)'
                ),
                patient_id,
                concat('birth_date=', toString(birth_date), ' sex=', sex),
                _ingested_at
            FROM bronze.patients
            WHERE _ingested_at > {watermark}
              AND NOT (isNotNull(birth_date) AND birth_date <= today() AND upper(sex) IN ('M', 'F'))
        """,
    },
    "dim_service": {
        "source": "bronze.services",
        "query": """
            INSERT INTO silver.dim_service
            SELECT service_code, service_label, _ingested_at
            FROM (
                SELECT service_code, service_label, _ingested_at
                FROM bronze.services
                WHERE service_code != '' AND service_label != ''
                  AND _ingested_at > {watermark}
                ORDER BY _ingested_at DESC
                LIMIT 1 BY service_code
            )
            WHERE (service_code, service_label) NOT IN (
                SELECT service_code, service_label FROM silver.dim_service FINAL
            )
        """,
        "rejects": """
            INSERT INTO silver._rejets (source_table, regle, cle, details, _ingested_at)
            SELECT 'bronze.services', 'code ou libelle vide', service_code,
                   concat('service_label=', service_label), _ingested_at
            FROM bronze.services
            WHERE _ingested_at > {watermark}
              AND NOT (service_code != '' AND service_label != '')
        """,
    },
    "dim_cim10": {
        "source": "bronze.cim10",
        "query": """
            INSERT INTO silver.dim_cim10
            SELECT code_cim10, libelle, _ingested_at
            FROM (
                SELECT code_cim10, libelle, _ingested_at
                FROM bronze.cim10
                WHERE code_cim10 != '' AND libelle != ''
                  AND _ingested_at > {watermark}
                ORDER BY _ingested_at DESC
                LIMIT 1 BY code_cim10
            )
            WHERE (code_cim10, libelle) NOT IN (
                SELECT code_cim10, libelle FROM silver.dim_cim10 FINAL
            )
        """,
        "rejects": """
            INSERT INTO silver._rejets (source_table, regle, cle, details, _ingested_at)
            SELECT 'bronze.cim10', 'code ou libelle vide', code_cim10,
                   concat('libelle=', libelle), _ingested_at
            FROM bronze.cim10
            WHERE _ingested_at > {watermark}
              AND NOT (code_cim10 != '' AND libelle != '')
        """,
    },
    # Contrôles : admission renseignée, cohérence temporelle, intégrité
    # référentielle. Un séjour sans date de sortie est légitime (patient encore
    # hospitalisé), il est conservé.
    # readmission_30j a besoin de tout l'historique du patient : on reprend donc
    # l'intégralité des séjours des patients concernés par une nouveauté.
    "fact_sejours": {
        "source": "bronze.sejours",
        "query": """
            INSERT INTO silver.fact_sejours
            SELECT
                stay_id, patient_id, service_code, admission_ts, discharge_ts,
                if(isNull(discharge_ts), NULL, dateDiff('hour', admission_ts, discharge_ts)) AS duree_sejour_h,
                admission_mode, discharge_mode,
                if(
                    sortie_precedente IS NOT NULL
                    AND dateDiff('day', sortie_precedente, admission_ts) <= 30,
                    1, 0
                ) AS readmission_30j,
                -- Âge à la date de l'admission, et non à la date du calcul :
                -- un patient admis à 12 ans puis à 42 ans doit compter dans
                -- deux tranches différentes, pas deux fois dans la même.
                toUInt8(age('year', birth_date, admission_ts)) AS age_at_admission,
                _ingested_at,
                now() AS _processed_at
            FROM (
                SELECT
                    *,
                    lagInFrame(discharge_ts) OVER (PARTITION BY patient_id ORDER BY admission_ts) AS sortie_precedente
                FROM (
                    SELECT b.stay_id AS stay_id, b.patient_id AS patient_id,
                           b.service_code AS service_code, b.admission_ts AS admission_ts,
                           b.discharge_ts AS discharge_ts, b.admission_mode AS admission_mode,
                           b.discharge_mode AS discharge_mode, b._ingested_at AS _ingested_at,
                           p.birth_date AS birth_date
                    -- La jointure sur dim_patient sert à deux choses à la fois :
                    -- ramener birth_date (pour l'âge à l'admission) et faire
                    -- office de contrôle d'intégrité référentielle, un séjour
                    -- dont le patient est inconnu ne trouvant pas de contrepartie.
                    FROM bronze.sejours AS b
                    JOIN silver.dim_patient AS p FINAL ON b.patient_id = p.patient_id
                    WHERE isNotNull(b.admission_ts)
                      AND (b.discharge_ts IS NULL OR b.discharge_ts >= b.admission_ts)
                      AND b.service_code IN (SELECT service_code FROM silver.dim_service FINAL)
                      AND b.patient_id IN (
                        SELECT DISTINCT patient_id FROM bronze.sejours
                        WHERE _ingested_at > {watermark}
                      )
                    ORDER BY b._ingested_at DESC
                    LIMIT 1 BY b.stay_id
                )
            )
            WHERE (stay_id, patient_id, service_code, admission_ts, discharge_ts,
                   duree_sejour_h, admission_mode, discharge_mode, readmission_30j,
                   age_at_admission) NOT IN (
                SELECT stay_id, patient_id, service_code, admission_ts, discharge_ts,
                       duree_sejour_h, admission_mode, discharge_mode, readmission_30j,
                       age_at_admission
                FROM silver.fact_sejours FINAL
            )
        """,
        "rejects": """
            INSERT INTO silver._rejets (source_table, regle, cle, details, _ingested_at)
            SELECT
                'bronze.sejours',
                multiIf(
                    isNull(admission_ts), 'date d''admission manquante',
                    isNotNull(discharge_ts) AND discharge_ts < admission_ts,
                        'incoherence temporelle (sortie avant admission)',
                    patient_id NOT IN (SELECT patient_id FROM silver.dim_patient FINAL),
                        'patient inconnu du referentiel',
                    'service inconnu du referentiel'
                ),
                stay_id,
                concat('patient_id=', patient_id, ' service_code=', service_code,
                       ' admission=', toString(admission_ts),
                       ' sortie=', ifNull(toString(discharge_ts), 'NULL')),
                _ingested_at
            FROM bronze.sejours
            WHERE _ingested_at > {watermark}
              AND NOT (
                isNotNull(admission_ts)
                AND (discharge_ts IS NULL OR discharge_ts >= admission_ts)
                AND patient_id IN (SELECT patient_id FROM silver.dim_patient FINAL)
                AND service_code IN (SELECT service_code FROM silver.dim_service FINAL)
              )
        """,
    },
    # Contrôle : plages physiologiques. Flux volumineux et purement additif ->
    # watermark seul, pas d'anti-jointure qui coûterait cher sur ce volume.
    # patient_id/service_code dénormalisés depuis fact_sejours.
    "fact_monitoring": {
        "source": "bronze.monitoring",
        "query": """
            INSERT INTO silver.fact_monitoring
            SELECT
                m.stay_id, m.ts, s.patient_id, s.service_code,
                m.heart_rate, m.spo2, m.temp_c,
                if(m.heart_rate > 120 OR m.heart_rate < 50, 1, 0) AS is_alerte_fc,
                if(m.spo2 < 90, 1, 0) AS is_alerte_spo2,
                if(m.temp_c > 38.5 OR m.temp_c < 35, 1, 0) AS is_alerte_temp,
                m._ingested_at
            FROM bronze.monitoring m
            JOIN silver.fact_sejours AS s FINAL ON m.stay_id = s.stay_id
            WHERE m.heart_rate BETWEEN 20 AND 250
              AND m.spo2 BETWEEN 50 AND 100
              AND m.temp_c BETWEEN 30 AND 45
              AND m._ingested_at > {watermark}
        """,
        "rejects": """
            INSERT INTO silver._rejets (source_table, regle, cle, details, _ingested_at)
            SELECT
                'bronze.monitoring',
                -- Un relevé peut violer plusieurs bornes : on ne liste que
                -- celles réellement en cause. concat_ws ne convient pas ici
                -- (il propage le NULL et conserve les séparateurs des chaines
                -- vides), d'où le filtrage explicite du tableau.
                arrayStringConcat(arrayFilter(x -> x != '', [
                    if(NOT (heart_rate BETWEEN 20 AND 250), 'FC hors plage 20-250', ''),
                    if(NOT (spo2 BETWEEN 50 AND 100), 'SpO2 hors plage 50-100', ''),
                    if(NOT (temp_c BETWEEN 30 AND 45), 'temperature hors plage 30-45', '')
                ]), ' + '),
                concat(stay_id, '@', toString(ts)),
                concat('heart_rate=', toString(heart_rate),
                       ' spo2=', toString(spo2),
                       ' temp_c=', toString(temp_c)),
                _ingested_at
            FROM bronze.monitoring
            WHERE _ingested_at > {watermark}
              AND NOT (
                heart_rate BETWEEN 20 AND 250
                AND spo2 BETWEEN 50 AND 100
                AND temp_c BETWEEN 30 AND 45
              )
        """,
    },
    # Aplati depuis bronze.diagnostics via ARRAY JOIN (1 ligne bronze -> N lignes
    # silver). Contrôle : le code CIM-10 doit exister au référentiel.
    "fact_diagnostics": {
        "source": "bronze.diagnostics",
        "query": """
            INSERT INTO silver.fact_diagnostics
            SELECT stay_id, code_cim10, patient_id, service_code, type,
                   age_at_diagnostic, _ingested_at
            FROM (
                SELECT
                    b.stay_id AS stay_id, d.code_cim10 AS code_cim10,
                    s.patient_id AS patient_id, s.service_code AS service_code,
                    d.type AS type,
                    -- Le grain (séjour x code) n'a pas de date propre : la date
                    -- de référence est l'admission du séjour, déjà ramenée par
                    -- la jointure qui dénormalise patient_id et service_code.
                    s.age_at_admission AS age_at_diagnostic,
                    b._ingested_at AS _ingested_at
                FROM bronze.diagnostics b
                ARRAY JOIN b.diagnostics AS d
                JOIN silver.fact_sejours AS s FINAL ON b.stay_id = s.stay_id
                WHERE d.code_cim10 IN (SELECT code_cim10 FROM silver.dim_cim10 FINAL)
                  AND b._ingested_at > {watermark}
                ORDER BY b._ingested_at DESC
                LIMIT 1 BY b.stay_id, d.code_cim10
            )
            WHERE (stay_id, code_cim10, patient_id, service_code, type,
                   age_at_diagnostic) NOT IN (
                SELECT stay_id, code_cim10, patient_id, service_code, type,
                       age_at_diagnostic
                FROM silver.fact_diagnostics FINAL
            )
        """,
        "rejects": """
            INSERT INTO silver._rejets (source_table, regle, cle, details, _ingested_at)
            SELECT 'bronze.diagnostics', 'code CIM-10 inconnu du referentiel',
                   concat(stay_id, '/', d.code_cim10),
                   concat('type=', d.type), _ingested_at
            FROM bronze.diagnostics
            ARRAY JOIN diagnostics AS d
            WHERE _ingested_at > {watermark}
              AND d.code_cim10 NOT IN (SELECT code_cim10 FROM silver.dim_cim10 FINAL)
        """,
    },
}

# Ordre imposé par les dépendances : les dimensions avant les faits (contrôles
# d'intégrité référentielle), et fact_sejours avant les deux autres faits qui
# lui empruntent patient_id/service_code.
TABLE_ORDER = ["dim_patient", "dim_service", "dim_cim10",
               "fact_sejours", "fact_monitoring", "fact_diagnostics"]


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
        step = SILVER_STEPS[name]
        watermark = read_watermark(name)

        response = run_query(step["query"].replace("{watermark}", watermark))
        if response.status_code != 200:
            print(f"Failed on silver.{name}: {response.status_code} {response.text}")
            failures += 1
            continue
        inserted = written_rows(response)

        # Les rejets sont tracés AVANT l'avancement du watermark, pour couvrir
        # exactement la même fenêtre de données que l'insertion ci-dessus.
        rejected = 0
        if "rejects" in step:
            reject_response = run_query(step["rejects"].replace("{watermark}", watermark))
            if reject_response.status_code != 200:
                print(f"Failed on rejets silver.{name}: "
                      f"{reject_response.status_code} {reject_response.text}")
                failures += 1
            else:
                rejected = written_rows(reject_response)

        advance_watermark(name, step["source"])
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
