-- Silver : données nettoyées, conformes, relationnelles (voir diagrams/silver.puml)

CREATE DATABASE IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.patients
(
    patient_id   String,
    birth_date   Date32,
    sex          String,
    region_code  String
)
ENGINE = MergeTree
ORDER BY patient_id;

CREATE TABLE IF NOT EXISTS silver.sejours
(
    stay_id         String,
    patient_id      String,
    service_code    String,
    admission_ts    DateTime,
    discharge_ts    Nullable(DateTime),
    admission_mode  String,
    discharge_mode  String
)
ENGINE = MergeTree
ORDER BY stay_id;

-- Aplati depuis bronze.diagnostics via ARRAY JOIN : 1 ligne bronze -> N lignes silver
CREATE TABLE IF NOT EXISTS silver.diagnostic
(
    stay_id     String,
    code_cim10  String,
    type        String
)
ENGINE = MergeTree
ORDER BY (stay_id, code_cim10);

CREATE TABLE IF NOT EXISTS silver.monitoring
(
    stay_id     String,
    ts          DateTime,
    heart_rate  Float32,
    spo2        Float32,
    temp_c      Float32
)
ENGINE = MergeTree
ORDER BY (stay_id, ts);

CREATE TABLE IF NOT EXISTS silver.services
(
    service_code   String,
    service_label  String
)
ENGINE = MergeTree
ORDER BY service_code;

CREATE TABLE IF NOT EXISTS silver.cim10
(
    code_cim10  String,
    libelle     String
)
ENGINE = MergeTree
ORDER BY code_cim10;

-- Exemple de peuplement (nettoyage + intégrité référentielle) :
--
-- INSERT INTO silver.diagnostic
-- SELECT stay_id, d.code_cim10, d.type
-- FROM bronze.diagnostics
-- ARRAY JOIN diagnostics AS d;
--
-- INSERT INTO silver.sejours
-- SELECT s.*
-- FROM bronze.sejours s
-- WHERE s.discharge_ts >= s.admission_ts
--   AND s.patient_id IN (SELECT patient_id FROM bronze.patients)
--   AND s.service_code IN (SELECT service_code FROM bronze.services);
