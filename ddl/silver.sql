-- Silver : données nettoyées, conformes, relationnelles (voir diagrams/silver.puml)

CREATE DATABASE IF NOT EXISTS silver;

-- Watermark d'ingestion par table : "jusqu'à quel _ingested_at source ai-je lu ?".
-- Table dédiée plutôt que max(_ingested_at) sur la table cible, car une table
-- cible peut ne recevoir aucune ligne sur un cycle (rien n'a changé) sans que
-- cela signifie qu'il faille relire la source depuis le début au cycle suivant.
-- Traçabilité des contrôles qualité : une ligne par enregistrement écarté, avec
-- la règle qui l'a rejeté et les valeurs en cause. Le sujet demande de "tracer
-- ce que vous écartez" : sans cette table, on saurait combien de lignes ont été
-- perdues, mais ni lesquelles ni pourquoi.
CREATE TABLE IF NOT EXISTS silver._rejets
(
    detected_at   DateTime DEFAULT now(),
    source_table  String,
    regle         String,
    cle           String,
    details       String,
    _ingested_at  DateTime
)
ENGINE = MergeTree
ORDER BY (source_table, detected_at);

CREATE TABLE IF NOT EXISTS silver._watermarks
(
    table_name        String,
    last_ingested_at  DateTime,
    updated_at        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY table_name;

-- ReplacingMergeTree(_ingested_at) : garde la ligne la plus récente par
-- patient_id. Alimenté en incrémental (watermark _ingested_at), pas de
-- TRUNCATE : toute nouvelle ligne bronze est par construction plus récente
-- que ce qui existe déjà ici, inutile de rescanner tout l'historique.
CREATE TABLE IF NOT EXISTS silver.patients
(
    patient_id    String,
    birth_date    Date32,
    sex           String,
    region_code   String,
    _ingested_at  DateTime
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY patient_id;

-- ReplacingMergeTree(_ingested_at) : si un séjour est redéposé plus tard avec
-- une date de sortie renseignée (séjour en cours puis clôturé), la version la
-- plus récente écrase l'ancienne au lieu de créer un doublon. Alimenté en
-- incrémental (watermark _ingested_at).
CREATE TABLE IF NOT EXISTS silver.sejours
(
    stay_id         String,
    patient_id      String,
    service_code    String,
    admission_ts    DateTime,
    discharge_ts    Nullable(DateTime),
    admission_mode  String,
    discharge_mode  String,
    _ingested_at    DateTime
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY stay_id;

-- Aplati depuis bronze.diagnostics via ARRAY JOIN : 1 ligne bronze -> N lignes silver
CREATE TABLE IF NOT EXISTS silver.diagnostic
(
    stay_id       String,
    code_cim10    String,
    type          String,
    _ingested_at  DateTime
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY (stay_id, code_cim10);

-- _ingested_at propagé depuis bronze.monitoring : sert de watermark pour
-- l'ingestion incrémentale (monitoring est le flux volumineux, cf. contrainte
-- "Volume" du sujet -- on ne rescane pas tout l'historique à chaque run).
CREATE TABLE IF NOT EXISTS silver.monitoring
(
    stay_id       String,
    ts            DateTime,
    heart_rate    Float32,
    spo2          Float32,
    temp_c        Float32,
    _ingested_at  DateTime
)
ENGINE = MergeTree
ORDER BY (stay_id, ts);

-- Même logique incrémentale que patients (référentiels, changent rarement).
CREATE TABLE IF NOT EXISTS silver.services
(
    service_code   String,
    service_label  String,
    _ingested_at   DateTime
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY service_code;

CREATE TABLE IF NOT EXISTS silver.cim10
(
    code_cim10    String,
    libelle       String,
    _ingested_at  DateTime
)
ENGINE = ReplacingMergeTree(_ingested_at)
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
