-- Silver : modèle en étoile au grain fin, nettoyé et contrôlé
-- (voir diagrams/silver.puml)
--
-- Les données brutes de Bronze sont transformées directement vers le modèle
-- dimensionnel : les contrôles qualité, la déduplication et le calcul des
-- mesures se font pendant cette transformation. Il n'y a pas de couche
-- intermédiaire normalisée.

CREATE DATABASE IF NOT EXISTS silver;

-- Watermark d'ingestion par table : "jusqu'à quel _ingested_at source ai-je lu ?".
-- Table dédiée plutôt que max(_ingested_at) sur la table cible, car une table
-- cible peut ne recevoir aucune ligne sur un cycle (rien n'a changé) sans que
-- cela signifie qu'il faille relire la source depuis le début au cycle suivant.
CREATE TABLE IF NOT EXISTS silver._watermarks
(
    table_name        String,
    last_ingested_at  DateTime,
    updated_at        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY table_name;

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

-- ---------------------------------------------------------------- Dimensions

-- ReplacingMergeTree(_ingested_at) : garde la version la plus récente par clé.
-- Contrôles appliqués : date de naissance valide, sexe normalisé en M/F.
--
-- Pas de colonne d'âge ici, volontairement : l'âge n'est pas un attribut du
-- patient, c'est un attribut de l'ÉVÉNEMENT. Le stocker dans la dimension
-- imposerait une date de référence (today() au moment du calcul) qui serait
-- fausse pour tout fait antérieur, et qui ne serait jamais rafraîchie puisque
-- le watermark empêche de relire une ligne déjà traitée. L'âge est donc porté
-- par les faits (age_at_admission, age_at_diagnostic), calculé à la date de
-- l'événement. birth_date reste ici : c'est la seule matière première utile.
CREATE TABLE IF NOT EXISTS silver.dim_patient
(
    patient_id    String,
    birth_date    Date32,
    sex           String,
    region_code   String,
    _ingested_at  DateTime
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY patient_id;

-- service_label / categorie / pole forment une hierarchie a trois niveaux
-- d'agregation croissants, stockee a plat : l'eclater en dim_categorie /
-- dim_pole obligerait chaque requete a enchainer trois jointures pour decrire
-- une seule entite.
-- capacite_lits est un attribut numerique de la dimension, PAS une mesure :
-- il ne s'additionne pas apres jointure sur un fait, il sert de denominateur
-- (taux d'occupation).
CREATE TABLE IF NOT EXISTS silver.dim_service
(
    service_code   String,
    service_label  String,
    categorie      String,
    pole           String,
    capacite_lits  UInt32,
    _ingested_at   DateTime
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY service_code;

CREATE TABLE IF NOT EXISTS silver.dim_cim10
(
    code_cim10    String,
    libelle       String,
    _ingested_at  DateTime
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY code_cim10;

-- --------------------------------------------------------------------- Faits
-- stay_id est une dimension dégénérée partagée par les 3 faits : jamais une
-- clé étrangère d'un fait vers un autre. patient_id/service_code sont
-- dénormalisés sur chaque fait pour éviter les jointures fait-à-fait.

-- grain = 1 séjour.
-- ORDER BY stay_id seul : en ClickHouse la clé de tri est aussi la clé de
-- déduplication, elle ne doit donc contenir que des colonnes immuables pour
-- l'entité. Y mettre service_code ferait apparaître deux lignes pour un même
-- séjour si son service était corrigé.
-- Deux colonnes techniques distinctes :
--   _ingested_at  = date d'ingestion de la source (watermark de lecture)
--   _processed_at = date de calcul de la ligne (version d'écrasement)
-- readmission_30j (lagInFrame sur tout l'historique du patient) oblige à
-- recalculer d'anciens séjours quand le patient revient : ces lignes ont un
-- _ingested_at ancien mais doivent écraser la version précédente.
CREATE TABLE IF NOT EXISTS silver.fact_sejours
(
    stay_id          String,
    patient_id       String,
    service_code     String,
    admission_ts     DateTime,
    discharge_ts     Nullable(DateTime),
    duree_sejour_h   Nullable(Float64),
    admission_mode   String,
    discharge_mode   String,
    readmission_30j  UInt8,
    -- Âge révolu du patient LE JOUR DE SON ADMISSION. Mesure de l'événement,
    -- figée une fois pour toutes : elle ne bouge plus quand le patient
    -- vieillit. Le découpage en tranches est laissé à Gold, pour qu'un
    -- changement de bornes n'oblige pas à reconstruire Silver.
    age_at_admission UInt8,
    _ingested_at     DateTime,
    _processed_at    DateTime
)
ENGINE = ReplacingMergeTree(_processed_at)
ORDER BY stay_id;

-- grain = 1 relevé. Flux volumineux et purement additif (une clé stay_id+ts
-- n'est jamais redéposée) : MergeTree simple, pas de déduplication à faire.
CREATE TABLE IF NOT EXISTS silver.fact_monitoring
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
    is_alerte_temp  UInt8,
    _ingested_at    DateTime
)
ENGINE = MergeTree
ORDER BY (stay_id, ts);

CREATE TABLE IF NOT EXISTS silver.fact_diagnostics
(
    stay_id            String,
    code_cim10         String,
    patient_id         String,
    service_code       String,
    type               String,
    age_at_diagnostic  UInt8,
    _ingested_at       DateTime
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY (stay_id, code_cim10);

CREATE TABLE IF NOT EXISTS silver.dim_ccam
(
    code_ccam     String,
    libelle       String,
    tarif_euros   UInt32,
    _ingested_at  DateTime
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY code_ccam;

CREATE TABLE IF NOT EXISTS silver.fact_actes
(
    stay_id       String,
    code_ccam     String,
    acte_ts       DateTime,
    patient_id    String,
    service_code  String,
    _ingested_at  DateTime
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY (stay_id, code_ccam, acte_ts);
