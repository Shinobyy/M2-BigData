import requests
import os
from dotenv import load_dotenv

load_dotenv()

clickhouse_host = os.getenv("CLICKHOUSE_HOST", "localhost")
clickhouse_port = os.getenv("CLICKHOUSE_PORT", "8123")
clickhouse_user = os.getenv("CLICKHOUSE_USER", "default")
clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "")

SILVER_QUERIES = {
    # patients : doublons (retour quotidien du même patient) -> on garde la version la plus récente
    "patients": """
        INSERT INTO silver.patients
        SELECT patient_id, birth_date, sex, region_code
        FROM (
            SELECT
                patient_id,
                birth_date,
                upper(sex) AS sex,
                region_code
            FROM bronze.patients
            WHERE isNotNull(birth_date) AND birth_date <= today()
              AND upper(sex) IN ('M', 'F')
            ORDER BY _ingested_at DESC
            LIMIT 1 BY patient_id
        )
    """,
    "services": """
        INSERT INTO silver.services
        SELECT DISTINCT service_code, service_label
        FROM bronze.services
        WHERE service_code != '' AND service_label != ''
    """,
    "cim10": """
        INSERT INTO silver.cim10
        SELECT DISTINCT code_cim10, libelle
        FROM bronze.cim10
        WHERE code_cim10 != '' AND libelle != ''
    """,
    # sejours : cohérence temporelle (discharge_ts < admission_ts -> écarté)
    # séjour en cours : discharge_ts NULL est légitime, pas un rejet
    "sejours": """
        INSERT INTO silver.sejours
        SELECT DISTINCT stay_id, patient_id, service_code, admission_ts,
               discharge_ts, admission_mode, discharge_mode
        FROM bronze.sejours
        WHERE isNotNull(admission_ts)
          AND (discharge_ts IS NULL OR discharge_ts >= admission_ts)
          AND patient_id IN (SELECT patient_id FROM silver.patients)
          AND service_code IN (SELECT service_code FROM silver.services)
    """,
    # monitoring : valeurs hors plage physiologique
    "monitoring": """
        INSERT INTO silver.monitoring
        SELECT DISTINCT stay_id, ts, heart_rate, spo2, temp_c
        FROM bronze.monitoring
        WHERE heart_rate BETWEEN 20 AND 250
          AND spo2 BETWEEN 50 AND 100
          AND temp_c BETWEEN 30 AND 45
    """,
    "diagnostic": """
        INSERT INTO silver.diagnostic
        SELECT DISTINCT stay_id, d.code_cim10, d.type
        FROM bronze.diagnostics
        ARRAY JOIN diagnostics AS d
        WHERE d.code_cim10 IN (SELECT code_cim10 FROM silver.cim10)
    """,
}

# Ordre important : patients/services/cim10 avant sejours (référencés dans son WHERE)
TABLE_ORDER = ["patients", "services", "cim10", "sejours", "monitoring", "diagnostic"]

def run_query(name, query):
    response = requests.post(
        f"http://{clickhouse_host}:{clickhouse_port}/",
        params={"query": query},
        auth=(clickhouse_user, clickhouse_password),
    )
    if response.status_code == 200:
        print(f"Populated silver.{name}")
    else:
        print(f"Failed on silver.{name}: {response.status_code} {response.text}")

def main():
    for name in TABLE_ORDER:
        run_query(name, SILVER_QUERIES[name])

if __name__ == "__main__":
    main()
