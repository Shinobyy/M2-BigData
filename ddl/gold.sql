-- Gold : modèle en étoile / fact constellation (voir diagrams/gold.puml)
-- stay_id est une dimension dégénérée partagée par les 3 facts : pas de FK
-- d'un fact vers un autre. patient_id/service_code sont dénormalisés sur
-- fact_monitoring/fact_diagnostics pour éviter tout join fact -> fact.

CREATE DATABASE IF NOT EXISTS gold;

-- Dimensions

-- ReplacingMergeTree(_ingested_at) : versionné et alimenté en incrémental
-- comme silver.patients (voir plus bas), pas de TRUNCATE.
CREATE TABLE IF NOT EXISTS gold.dim_patient
(
    patient_id    String,
    birth_date    Date32,
    sex           String,
    region_code   String,
    age_group     String,
    _ingested_at  DateTime
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY patient_id;

CREATE TABLE IF NOT EXISTS gold.dim_service
(
    service_code   String,
    service_label  String,
    _ingested_at   DateTime
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY service_code;

CREATE TABLE IF NOT EXISTS gold.dim_cim10
(
    code_cim10    String,
    libelle       String,
    _ingested_at  DateTime
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY code_cim10;

-- Facts

-- grain = 1 séjour.
-- Deux colonnes techniques distinctes, et c'est volontaire :
--   _ingested_at  = date d'ingestion de la source (watermark : "jusqu'où j'ai lu")
--   _processed_at = date de calcul de la ligne (version ReplacingMergeTree)
-- Nécessaire car readmission_30j (lagInFrame sur tout l'historique du patient)
-- oblige à RECALCULER d'anciens séjours quand le patient revient : ces lignes
-- recalculées ont un _ingested_at ancien mais doivent quand même écraser la
-- version précédente -> c'est _processed_at qui arbitre.
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
    readmission_30j  UInt8,
    _ingested_at     DateTime,
    _processed_at    DateTime
)
ENGINE = ReplacingMergeTree(_processed_at)
ORDER BY (service_code, admission_ts, stay_id);

-- grain = 1 relevé ; _ingested_at propagé pour le même watermark incrémental qu'en Silver
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
    is_alerte_temp  UInt8,
    _ingested_at    DateTime
)
ENGINE = MergeTree
ORDER BY (stay_id, ts);

-- grain = 1 diagnostic ; projection pure de silver.diagnostic -> _ingested_at
-- suffit comme version (pas de recalcul rétroactif ici, contrairement à fact_sejours)
CREATE TABLE IF NOT EXISTS gold.fact_diagnostics
(
    stay_id       String,
    code_cim10    String,
    patient_id    String,
    service_code  String,
    type          String,
    _ingested_at  DateTime
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY (stay_id, code_cim10);

-- Vues "recherche clinique" : seules vues exposées au rôle recherche (cf. droits
-- d'accès plus bas). Le filtre HAVING >= 5 applique la règle RGPD "petits effectifs" :
-- aucune cohorte de moins de 5 patients n'est diffusée.

-- SQL SECURITY DEFINER : la vue s'exécute avec les droits du créateur (default),
-- pas de l'appelant (recherche_user) -- sinon recherche_user devrait aussi avoir
-- SELECT sur fact_diagnostics/dim_patient, ce qui casserait le cloisonnement.
CREATE VIEW IF NOT EXISTS gold.recherche_prevalence_pathologie
DEFINER = default SQL SECURITY DEFINER
AS
SELECT
    dc.libelle AS pathologie,
    uniqExact(fd.patient_id) AS taille_cohorte
FROM gold.fact_diagnostics fd
JOIN gold.dim_cim10 dc USING (code_cim10)
GROUP BY dc.libelle
HAVING taille_cohorte >= 5;

CREATE VIEW IF NOT EXISTS gold.recherche_cohorte_age_sexe
DEFINER = default SQL SECURITY DEFINER
AS
SELECT
    dp.age_group,
    dp.sex,
    uniqExact(dp.patient_id) AS taille_cohorte
FROM gold.dim_patient dp
GROUP BY dp.age_group, dp.sex
HAVING taille_cohorte >= 5;

-- Cloisonnement des droits (RGPD) : deux rôles distincts, deux périmètres de
-- données disjoints. pilotage_user ne voit jamais les diagnostics individuels
-- ni les données brutes patient ; recherche_user ne voit que des vues déjà
-- agrégées et filtrées sur les petits effectifs (jamais les facts au grain fin).

-- final = 1 : applique automatiquement FINAL à toute lecture sur les tables
-- ReplacingMergeTree. Sans ça, Metabase verrait les doublons transitoires entre
-- une réinsertion (séjours recalculés) et le merge en arrière-plan de ClickHouse.
CREATE USER IF NOT EXISTS pilotage_user IDENTIFIED WITH plaintext_password BY 'pilotage_pwd' SETTINGS final = 1;
CREATE USER IF NOT EXISTS recherche_user IDENTIFIED WITH plaintext_password BY 'recherche_pwd' SETTINGS final = 1;

GRANT SELECT ON gold.fact_sejours TO pilotage_user;
GRANT SELECT ON gold.fact_monitoring TO pilotage_user;
GRANT SELECT ON gold.dim_service TO pilotage_user;
GRANT SELECT ON gold.dim_patient TO pilotage_user;

GRANT SELECT ON gold.recherche_prevalence_pathologie TO recherche_user;
GRANT SELECT ON gold.recherche_cohorte_age_sexe TO recherche_user;

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
