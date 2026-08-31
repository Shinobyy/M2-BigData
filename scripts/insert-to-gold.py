import requests
import os
from dotenv import load_dotenv

load_dotenv()

clickhouse_host = os.getenv("CLICKHOUSE_HOST", "localhost")
clickhouse_port = os.getenv("CLICKHOUSE_PORT", "8123")
clickhouse_user = os.getenv("CLICKHOUSE_USER", "default")
clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "")

GOLD_QUERIES = {
    "dim_patient": """
        INSERT INTO gold.dim_patient
        SELECT
            patient_id,
            birth_date,
            sex,
            region_code,
            CASE
                WHEN age('year', birth_date, today()) < 18 THEN '0-17'
                WHEN age('year', birth_date, today()) BETWEEN 18 AND 35 THEN '18-35'
                WHEN age('year', birth_date, today()) BETWEEN 36 AND 50 THEN '36-50'
                WHEN age('year', birth_date, today()) BETWEEN 51 AND 65 THEN '51-65'
                ELSE '66+'
            END AS age_group
        FROM silver.patients
    """,
    "dim_service": """
        INSERT INTO gold.dim_service
        SELECT service_code, service_label
        FROM silver.services
    """,
    "dim_cim10": """
        INSERT INTO gold.dim_cim10
        SELECT code_cim10, libelle
        FROM silver.cim10
    """,
    # grain = 1 séjour ; readmission_30j calculée par rapport à la sortie précédente du même patient
    "fact_sejours": """
        INSERT INTO gold.fact_sejours
        SELECT
            stay_id,
            patient_id,
            service_code,
            admission_ts,
            discharge_ts,
            if(isNull(discharge_ts), NULL, dateDiff('hour', admission_ts, discharge_ts)) AS duree_sejour_h,
            admission_mode,
            discharge_mode,
            if(
                sortie_precedente IS NOT NULL
                AND dateDiff('day', sortie_precedente, admission_ts) <= 30,
                1, 0
            ) AS readmission_30j
        FROM (
            SELECT
                *,
                lagInFrame(discharge_ts) OVER (PARTITION BY patient_id ORDER BY admission_ts) AS sortie_precedente
            FROM silver.sejours
        )
    """,
    # grain = 1 relevé ; patient_id/service_code dénormalisés pour éviter tout join fact -> fact
    "fact_monitoring": """
        INSERT INTO gold.fact_monitoring
        SELECT
            m.stay_id,
            m.ts,
            s.patient_id,
            s.service_code,
            m.heart_rate,
            m.spo2,
            m.temp_c,
            if(m.heart_rate > 120 OR m.heart_rate < 50, 1, 0) AS is_alerte_fc,
            if(m.spo2 < 90, 1, 0) AS is_alerte_spo2,
            if(m.temp_c > 38.5 OR m.temp_c < 35, 1, 0) AS is_alerte_temp
        FROM silver.monitoring m
        JOIN silver.sejours s ON m.stay_id = s.stay_id
    """,
    # grain = 1 diagnostic ; patient_id/service_code dénormalisés, idem
    "fact_diagnostics": """
        INSERT INTO gold.fact_diagnostics
        SELECT
            d.stay_id,
            d.code_cim10,
            s.patient_id,
            s.service_code,
            d.type
        FROM silver.diagnostic d
        JOIN silver.sejours s ON d.stay_id = s.stay_id
    """,
}

# Dimensions avant facts (les facts dénormalisent patient_id/service_code depuis silver.sejours,
# pas depuis les dims, donc l'ordre n'est pas strictement requis ici, mais gardé pour la lisibilité)
TABLE_ORDER = ["dim_patient", "dim_service", "dim_cim10", "fact_sejours", "fact_monitoring", "fact_diagnostics"]

def run_query(name, query):
    response = requests.post(
        f"http://{clickhouse_host}:{clickhouse_port}/",
        params={"query": query},
        auth=(clickhouse_user, clickhouse_password),
    )
    if response.status_code == 200:
        print(f"Populated gold.{name}")
    else:
        print(f"Failed on gold.{name}: {response.status_code} {response.text}")

def main():
    for name in TABLE_ORDER:
        run_query(name, GOLD_QUERIES[name])

if __name__ == "__main__":
    main()
