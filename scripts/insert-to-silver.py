import json
import os
import requests
from dotenv import load_dotenv

load_dotenv()

clickhouse_host = os.getenv("CLICKHOUSE_HOST", "localhost")
clickhouse_port = os.getenv("CLICKHOUSE_PORT", "8123")
clickhouse_user = os.getenv("CLICKHOUSE_USER", "default")
clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "")

# Chaque étape suit le même principe :
#   1. watermark : ne relire de bronze que ce qui n'a jamais été traité
#   2. LIMIT 1 BY <clé> : ne garder qu'une version par clé DANS le lot entrant
#   3. anti-jointure NOT IN : ne pas réécrire une ligne identique à celle déjà
#      présente en silver -- en régime stable, 0 ligne écrite
# Objectif : ne jamais insérer de doublon, plutôt que de le masquer à la lecture.

SILVER_STEPS = {
    # patients : doublons (retour quotidien du même patient) -> garde la version la plus récente
    "patients": {
        "source": "bronze.patients",
        "query": """
            INSERT INTO silver.patients
            SELECT patient_id, birth_date, sex, region_code, _ingested_at
            FROM (
                SELECT patient_id, birth_date, upper(sex) AS sex, region_code, _ingested_at
                FROM bronze.patients
                WHERE isNotNull(birth_date) AND birth_date <= today()
                  AND upper(sex) IN ('M', 'F')
                  AND _ingested_at > {watermark}
                ORDER BY _ingested_at DESC
                LIMIT 1 BY patient_id
            )
            WHERE (patient_id, birth_date, sex, region_code) NOT IN (
                SELECT patient_id, birth_date, sex, region_code FROM silver.patients FINAL
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
    "services": {
        "source": "bronze.services",
        "query": """
            INSERT INTO silver.services
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
                SELECT service_code, service_label FROM silver.services FINAL
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
    "cim10": {
        "source": "bronze.cim10",
        "query": """
            INSERT INTO silver.cim10
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
                SELECT code_cim10, libelle FROM silver.cim10 FINAL
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
    # sejours : cohérence temporelle (discharge_ts < admission_ts -> écarté)
    # séjour en cours : discharge_ts NULL est légitime, pas un rejet
    "sejours": {
        "source": "bronze.sejours",
        "query": """
            INSERT INTO silver.sejours
            SELECT stay_id, patient_id, service_code, admission_ts,
                   discharge_ts, admission_mode, discharge_mode, _ingested_at
            FROM (
                SELECT stay_id, patient_id, service_code, admission_ts,
                       discharge_ts, admission_mode, discharge_mode, _ingested_at
                FROM bronze.sejours
                WHERE isNotNull(admission_ts)
                  AND (discharge_ts IS NULL OR discharge_ts >= admission_ts)
                  AND patient_id IN (SELECT patient_id FROM silver.patients FINAL)
                  AND service_code IN (SELECT service_code FROM silver.services FINAL)
                  AND _ingested_at > {watermark}
                ORDER BY _ingested_at DESC
                LIMIT 1 BY stay_id
            )
            WHERE (stay_id, patient_id, service_code, admission_ts, discharge_ts,
                   admission_mode, discharge_mode) NOT IN (
                SELECT stay_id, patient_id, service_code, admission_ts, discharge_ts,
                       admission_mode, discharge_mode
                FROM silver.sejours FINAL
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
                    patient_id NOT IN (SELECT patient_id FROM silver.patients FINAL),
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
                AND patient_id IN (SELECT patient_id FROM silver.patients FINAL)
                AND service_code IN (SELECT service_code FROM silver.services FINAL)
              )
        """,
    },
    # monitoring : valeurs hors plage physiologique. Flux volumineux, purement
    # additif (une clé stay_id+ts n'est jamais redéposée) -> le watermark suffit,
    # pas besoin d'anti-jointure qui coûterait cher sur ce volume.
    "monitoring": {
        "source": "bronze.monitoring",
        "query": """
            INSERT INTO silver.monitoring
            SELECT stay_id, ts, heart_rate, spo2, temp_c, _ingested_at
            FROM bronze.monitoring
            WHERE heart_rate BETWEEN 20 AND 250
              AND spo2 BETWEEN 50 AND 100
              AND temp_c BETWEEN 30 AND 45
              AND _ingested_at > {watermark}
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
    "diagnostic": {
        "source": "bronze.diagnostics",
        "query": """
            INSERT INTO silver.diagnostic
            SELECT stay_id, code_cim10, type, _ingested_at
            FROM (
                SELECT stay_id, d.code_cim10 AS code_cim10, d.type AS type, _ingested_at
                FROM bronze.diagnostics
                ARRAY JOIN diagnostics AS d
                WHERE d.code_cim10 IN (SELECT code_cim10 FROM silver.cim10 FINAL)
                  AND _ingested_at > {watermark}
                ORDER BY _ingested_at DESC
                LIMIT 1 BY stay_id, code_cim10
            )
            WHERE (stay_id, code_cim10, type) NOT IN (
                SELECT stay_id, code_cim10, type FROM silver.diagnostic FINAL
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
              AND d.code_cim10 NOT IN (SELECT code_cim10 FROM silver.cim10 FINAL)
        """,
    },
}

# Ordre important : patients/services/cim10 avant sejours (référencés dans son WHERE)
TABLE_ORDER = ["patients", "services", "cim10", "sejours", "monitoring", "diagnostic"]


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
    summary = response.headers.get("X-ClickHouse-Summary", "")
    try:
        return int(json.loads(summary).get("written_rows", 0))
    except (ValueError, AttributeError):
        return 0


def main():
    for name in TABLE_ORDER:
        step = SILVER_STEPS[name]
        watermark = read_watermark(name)

        response = run_query(step["query"].replace("{watermark}", watermark))
        if response.status_code != 200:
            print(f"Failed on silver.{name}: {response.status_code} {response.text}")
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
            else:
                rejected = written_rows(reject_response)

        advance_watermark(name, step["source"])
        # Merge immédiat : la table persistée ne contient aucun doublon une fois
        # l'étape terminée, sans dépendre du merge asynchrone de ClickHouse.
        run_query(f"OPTIMIZE TABLE silver.{name} FINAL")

        suffix = f", {rejected} rejetée(s)" if rejected else ""
        print(f"Populated silver.{name}: {inserted} ligne(s) insérée(s){suffix}")


if __name__ == "__main__":
    main()
