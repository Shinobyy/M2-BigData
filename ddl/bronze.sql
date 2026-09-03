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
    discharge_mode  String,
    _ingested_at    DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY stay_id;

CREATE TABLE IF NOT EXISTS bronze.diagnostics
(
    stay_id       String,
    diagnostics   Array(Tuple(code_cim10 String, type String)),
    _ingested_at  DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY stay_id;

CREATE TABLE IF NOT EXISTS bronze.monitoring
(
    stay_id       String,
    ts            DateTime,
    heart_rate    Float32,
    spo2          Float32,
    temp_c        Float32,
    _ingested_at  DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (stay_id, ts);

CREATE TABLE IF NOT EXISTS bronze.services
(
    service_code   String,
    service_label  String,
    _ingested_at   DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY service_code;

CREATE TABLE IF NOT EXISTS bronze.cim10
(
    code_cim10    String,
    libelle       String,
    _ingested_at  DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY code_cim10;

-- Traçabilité au niveau fichier : d'où vient chaque lot de données et quand il a été traité.
CREATE TABLE IF NOT EXISTS bronze._ingestion_log
(
    ingested_at  DateTime DEFAULT now(),
    table_name   String,
    source_file  String,
    status       String
)
ENGINE = MergeTree
ORDER BY ingested_at;

CREATE TABLE IF NOT EXISTS bronze.description_service
(
    service_code   String,
    categorie      String,
    capacite_lits  UInt32,
    pole           String,
    _ingested_at   DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY service_code;

CREATE TABLE IF NOT EXISTS bronze.ccam
(
    code_ccam      String,
    libelle        String,
    tarif_euros    UInt32,
    _ingested_at   DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY code_ccam;

CREATE TABLE IF NOT EXISTS bronze.actes
(
    stay_id        String,
    code_ccam      String,
    acte_ts         DateTime,
    _ingested_at   DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (stay_id, acte_ts);