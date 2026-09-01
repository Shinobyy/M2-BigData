import requests
import os
from dotenv import load_dotenv

load_dotenv()

clickhouse_host = os.getenv("CLICKHOUSE_HOST", "localhost")
clickhouse_port = os.getenv("CLICKHOUSE_PORT", "8123")
clickhouse_user = os.getenv("CLICKHOUSE_USER", "default")
clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "")

# Même principe qu'en Silver : watermark + anti-jointure, pour n'insérer que ce
# qui est réellement nouveau ou modifié. Aucune ligne en double n'est écrite.

GOLD_STEPS = {
    "dim_patient": {
        "source": "silver.patients FINAL",
        "query": """
            INSERT INTO gold.dim_patient
            SELECT patient_id, birth_date, sex, region_code, age_group, _ingested_at
            FROM (
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
                    END AS age_group,
                    _ingested_at
                FROM silver.patients FINAL
                WHERE _ingested_at > {watermark}
            )
            WHERE (patient_id, birth_date, sex, region_code, age_group) NOT IN (
                SELECT patient_id, birth_date, sex, region_code, age_group
                FROM gold.dim_patient FINAL
            )
        """,
    },
    "dim_service": {
        "source": "silver.services FINAL",
        "query": """
            INSERT INTO gold.dim_service
            SELECT service_code, service_label, _ingested_at
            FROM silver.services FINAL
            WHERE _ingested_at > {watermark}
              AND (service_code, service_label) NOT IN (
                SELECT service_code, service_label FROM gold.dim_service FINAL
              )
        """,
    },
    "dim_cim10": {
        "source": "silver.cim10 FINAL",
        "query": """
            INSERT INTO gold.dim_cim10
            SELECT code_cim10, libelle, _ingested_at
            FROM silver.cim10 FINAL
            WHERE _ingested_at > {watermark}
              AND (code_cim10, libelle) NOT IN (
                SELECT code_cim10, libelle FROM gold.dim_cim10 FINAL
              )
        """,
    },
    # readmission_30j (lagInFrame) a besoin de TOUT l'historique du patient : on
    # ne reprend que les patients ayant un nouveau séjour, mais sur l'intégralité
    # de leur historique. L'anti-jointure évite de réécrire les séjours de ces
    # patients dont le calcul n'a pas changé.
    "fact_sejours": {
        "source": "silver.sejours FINAL",
        "query": """
            INSERT INTO gold.fact_sejours
            SELECT stay_id, patient_id, service_code, admission_ts, discharge_ts,
                   duree_sejour_h, admission_mode, discharge_mode, readmission_30j,
                   _ingested_at, now() AS _processed_at
            FROM (
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
                    ) AS readmission_30j,
                    _ingested_at
                FROM (
                    SELECT
                        *,
                        lagInFrame(discharge_ts) OVER (PARTITION BY patient_id ORDER BY admission_ts) AS sortie_precedente
                    FROM silver.sejours FINAL
                    WHERE patient_id IN (
                        SELECT DISTINCT patient_id
                        FROM silver.sejours FINAL
                        WHERE _ingested_at > {watermark}
                    )
                )
            )
            WHERE (stay_id, patient_id, service_code, admission_ts, discharge_ts,
                   duree_sejour_h, admission_mode, discharge_mode, readmission_30j) NOT IN (
                SELECT stay_id, patient_id, service_code, admission_ts, discharge_ts,
                       duree_sejour_h, admission_mode, discharge_mode, readmission_30j
                FROM gold.fact_sejours FINAL
            )
        """,
    },
    # Flux volumineux, purement additif -> watermark seul, pas d'anti-jointure.
    "fact_monitoring": {
        "source": "silver.monitoring",
        "query": """
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
                if(m.temp_c > 38.5 OR m.temp_c < 35, 1, 0) AS is_alerte_temp,
                m._ingested_at
            FROM silver.monitoring m
            JOIN silver.sejours AS s FINAL ON m.stay_id = s.stay_id
            WHERE m._ingested_at > {watermark}
        """,
    },
    "fact_diagnostics": {
        "source": "silver.diagnostic FINAL",
        "query": """
            INSERT INTO gold.fact_diagnostics
            SELECT stay_id, code_cim10, patient_id, service_code, type, _ingested_at
            FROM (
                SELECT d.stay_id AS stay_id, d.code_cim10 AS code_cim10,
                       s.patient_id AS patient_id, s.service_code AS service_code,
                       d.type AS type, d._ingested_at AS _ingested_at
                FROM silver.diagnostic AS d FINAL
                JOIN silver.sejours AS s FINAL ON d.stay_id = s.stay_id
                WHERE d._ingested_at > {watermark}
            )
            WHERE (stay_id, code_cim10, patient_id, service_code, type) NOT IN (
                SELECT stay_id, code_cim10, patient_id, service_code, type
                FROM gold.fact_diagnostics FINAL
            )
        """,
    },
}

TABLE_ORDER = ["dim_patient", "dim_service", "dim_cim10", "fact_sejours", "fact_monitoring", "fact_diagnostics"]


def run_query(query):
    return requests.post(
        f"http://{clickhouse_host}:{clickhouse_port}/",
        params={"query": query},
        auth=(clickhouse_user, clickhouse_password),
    )


def read_watermark(table_name):
    return (
        "(SELECT ifNull(max(last_ingested_at), toDateTime('1970-01-01')) "
        f"FROM silver._watermarks FINAL WHERE table_name = 'gold.{table_name}')"
    )


def advance_watermark(table_name, source_table):
    return run_query(
        f"INSERT INTO silver._watermarks (table_name, last_ingested_at) "
        f"SELECT 'gold.{table_name}', max(_ingested_at) FROM {source_table}"
    )


def main():
    for name in TABLE_ORDER:
        step = GOLD_STEPS[name]
        query = step["query"].replace("{watermark}", read_watermark(name))
        response = run_query(query)
        if response.status_code != 200:
            print(f"Failed on gold.{name}: {response.status_code} {response.text}")
            continue
        inserted = response.headers.get("X-ClickHouse-Summary", "")
        advance_watermark(name, step["source"])
        run_query(f"OPTIMIZE TABLE gold.{name} FINAL")
        print(f"Populated gold.{name} {inserted}")


if __name__ == "__main__":
    main()
