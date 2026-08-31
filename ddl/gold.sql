-- Gold : modèle en étoile / fact constellation (voir diagrams/gold.puml)
-- stay_id est une dimension dégénérée partagée par les 3 facts : pas de FK
-- d'un fact vers un autre. patient_id/service_code sont dénormalisés sur
-- fact_monitoring/fact_diagnostics pour éviter tout join fact -> fact.

CREATE DATABASE IF NOT EXISTS gold;

-- Dimensions

CREATE TABLE IF NOT EXISTS gold.dim_patient
(
    patient_id   String,
    birth_date   Date32,
    sex          String,
    region_code  String,
    age_group    String
)
ENGINE = ReplacingMergeTree
ORDER BY patient_id;

CREATE TABLE IF NOT EXISTS gold.dim_service
(
    service_code   String,
    service_label  String
)
ENGINE = ReplacingMergeTree
ORDER BY service_code;

CREATE TABLE IF NOT EXISTS gold.dim_cim10
(
    code_cim10  String,
    libelle     String
)
ENGINE = ReplacingMergeTree
ORDER BY code_cim10;

-- Facts

-- grain = 1 séjour
CREATE TABLE IF NOT EXISTS gold.fact_sejours
(
    stay_id          String,
    patient_id       String,
    service_code     String,
    admission_ts     DateTime,
    discharge_ts     Nullable(DateTime),
    duree_sejour_h   Nullable(Float64),
    admission_mode   String,
    discharge_mode   String,
    readmission_30j  UInt8
)
ENGINE = MergeTree
ORDER BY (service_code, admission_ts);

-- grain = 1 relevé
CREATE TABLE IF NOT EXISTS gold.fact_monitoring
(
    stay_id         String,
    ts              DateTime,
    patient_id      String,
    service_code    String,
    heart_rate      Float32,
    spo2            Float32,
    temp_c          Float32,
    is_alerte_fc    UInt8,
    is_alerte_spo2  UInt8,
    is_alerte_temp  UInt8
)
ENGINE = MergeTree
ORDER BY (stay_id, ts);

-- grain = 1 diagnostic
CREATE TABLE IF NOT EXISTS gold.fact_diagnostics
(
    stay_id       String,
    code_cim10    String,
    patient_id    String,
    service_code  String,
    type          String
)
ENGINE = MergeTree
ORDER BY (stay_id, code_cim10);

-- Exemple de peuplement (à faire une fois silver.* rempli) :
--
-- INSERT INTO gold.fact_sejours
-- SELECT
--     stay_id, patient_id, service_code, admission_ts, discharge_ts,
--     dateDiff('hour', admission_ts, discharge_ts) AS duree_sejour_h,
--     admission_mode, discharge_mode,
--     0 AS readmission_30j -- à calculer via lagInFrame sur silver.sejours
-- FROM silver.sejours;
--
-- INSERT INTO gold.fact_monitoring
-- SELECT
--     m.stay_id, m.ts, s.patient_id, s.service_code,
--     m.heart_rate, m.spo2, m.temp_c,
--     (m.heart_rate > 120 OR m.heart_rate < 50) AS is_alerte_fc,
--     (m.spo2 < 90) AS is_alerte_spo2,
--     (m.temp_c > 38.5 OR m.temp_c < 35) AS is_alerte_temp
-- FROM silver.monitoring m
-- JOIN silver.sejours s ON m.stay_id = s.stay_id;
--
-- INSERT INTO gold.fact_diagnostics
-- SELECT d.stay_id, d.code_cim10, s.patient_id, s.service_code, d.type
-- FROM silver.diagnostic d
-- JOIN silver.sejours s ON d.stay_id = s.stay_id;
