-- Bronze : données brutes, telles qu'ingérées (voir diagrams/bronze.puml)

CREATE DATABASE IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.patients
(
    patient_id    String,
    birth_date    Date32,
    sex           String,
    region_code   String,
    _ingested_at  DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY patient_id;

CREATE TABLE IF NOT EXISTS bronze.sejours
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

CREATE TABLE IF NOT EXISTS bronze.diagnostics
(
    stay_id      String,
    diagnostics  Array(Tuple(code_cim10 String, type String))
)
ENGINE = MergeTree
ORDER BY stay_id;

CREATE TABLE IF NOT EXISTS bronze.monitoring
(
    stay_id     String,
    ts          DateTime,
    heart_rate  Float32,
    spo2        Float32,
    temp_c      Float32
)
ENGINE = MergeTree
ORDER BY (stay_id, ts);

CREATE TABLE IF NOT EXISTS bronze.services
(
    service_code   String,
    service_label  String
)
ENGINE = MergeTree
ORDER BY service_code;

CREATE TABLE IF NOT EXISTS bronze.cim10
(
    code_cim10  String,
    libelle     String
)
ENGINE = MergeTree
ORDER BY code_cim10;
