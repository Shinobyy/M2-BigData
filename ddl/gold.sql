-- Gold : une table par indicateur métier (voir diagrams/gold.puml)
--
-- Chaque table contient le KPI déjà agrégé, prêt à être affiché sans calcul :
-- les dashboards lisent, ils n'agrègent pas. Le grain de chaque table est son
-- axe d'analyse (service x mois, jour, tranche d'age...).
--
-- Toutes en ReplacingMergeTree(_processed_at) ordonnées sur leur grain : un
-- recalcul réécrit la ligne correspondante au lieu de la dupliquer, et les
-- tables restent consultables en permanence (pas de TRUNCATE, donc pas de
-- fenêtre pendant laquelle un dashboard verrait une table vide).

CREATE DATABASE IF NOT EXISTS gold;

-- Découpage en tranches d'âge, défini une seule fois et partagé par les deux
-- KPI qui l'utilisent (admissions_par_age, cohorte_age_sexe) : ils ne peuvent
-- donc pas diverger. Le bornage vit ici, en Gold, et non en Silver : c'est un
-- choix de présentation, pas une propriété de la donnée. Silver stocke l'âge
-- révolu à l'événement, changer les bornes ne demande qu'un recalcul de Gold.
CREATE FUNCTION IF NOT EXISTS age_group AS (a) -> multiIf(
    a < 18, '0-17',
    a <= 35, '18-35',
    a <= 50, '36-50',
    a <= 65, '51-65',
    '66+'
);

-- ------------------------------------------------- Pilotage hospitalier

-- Durée moyenne de séjour, par service et par mois.
-- Les séjours en cours (discharge_ts NULL) sont exclus du calcul : leur durée
-- n'est pas encore connue. Ils restent comptés dans nb_sejours_en_cours.
CREATE TABLE IF NOT EXISTS gold.dms_par_service
(
    service_code           String,
    service_label          String,
    mois                   Date,
    nb_sejours_termines    UInt32,
    nb_sejours_en_cours    UInt32,
    dms_jours              Float64,
    _processed_at          DateTime
)
ENGINE = ReplacingMergeTree(_processed_at)
ORDER BY (service_code, mois);

-- Activité des urgences : nombre de passages par jour.
CREATE TABLE IF NOT EXISTS gold.urgences_par_jour
(
    jour           Date,
    nb_passages    UInt32,
    _processed_at  DateTime
)
ENGINE = ReplacingMergeTree(_processed_at)
ORDER BY jour;

-- Taux de réadmission à 30 jours, par service et par mois.
CREATE TABLE IF NOT EXISTS gold.readmission_par_service
(
    service_code           String,
    service_label          String,
    mois                   Date,
    nb_sejours             UInt32,
    nb_readmissions        UInt32,
    taux_readmission_pct   Float64,
    _processed_at          DateTime
)
ENGINE = ReplacingMergeTree(_processed_at)
ORDER BY (service_code, mois);

-- Surveillance des constantes : relevés en alerte par jour, ventilés par type.
-- Un même relevé peut violer plusieurs bornes : nb_alertes_total compte les
-- relevés en alerte, pas la somme des trois colonnes.
-- Grain (jour x service) et non (jour) seul : la surveillance ne concerne que
-- certains services, et le pilotage a besoin de savoir lequel décroche. Le
-- total journalier reste une simple somme sur les services.
CREATE TABLE IF NOT EXISTS gold.alertes_par_jour
(
    jour                  Date,
    service_code          String,
    service_label         String,
    nb_releves            UInt32,
    nb_alertes_fc         UInt32,
    nb_alertes_spo2       UInt32,
    nb_alertes_temp       UInt32,
    nb_alertes_total      UInt32,
    nb_sejours_concernes  UInt32,
    _processed_at         DateTime
)
ENGINE = ReplacingMergeTree(_processed_at)
ORDER BY (jour, service_code);

-- Répartition des admissions par tranche d'âge.
CREATE TABLE IF NOT EXISTS gold.admissions_par_age
(
    age_group      String,
    nb_admissions  UInt32,
    nb_patients    UInt32,
    _processed_at  DateTime
)
ENGINE = ReplacingMergeTree(_processed_at)
ORDER BY age_group;

-- ---------------------------------------------------- Recherche clinique
-- Les deux tables ci-dessous appliquent la règle RGPD des petits effectifs :
-- aucune cohorte de moins de 5 patients n'est matérialisée. La règle est donc
-- appliquée à l'écriture, et non à la lecture : elle n'est pas contournable,
-- même par quelqu'un qui aurait un accès direct à la table.

CREATE TABLE IF NOT EXISTS gold.prevalence_pathologie
(
    code_cim10      String,
    pathologie      String,
    taille_cohorte  UInt64,
    nb_sejours      UInt32,
    _processed_at   DateTime
)
ENGINE = ReplacingMergeTree(_processed_at)
ORDER BY code_cim10;

CREATE TABLE IF NOT EXISTS gold.cohorte_age_sexe
(
    age_group       String,
    sex             String,
    taille_cohorte  UInt64,
    _processed_at   DateTime
)
ENGINE = ReplacingMergeTree(_processed_at)
ORDER BY (age_group, sex);

-- ------------------------------------------------ Cloisonnement des droits
-- Deux rôles, deux périmètres disjoints. Aucun des deux n'a accès à silver :
-- les dashboards ne voient que des agrégats, jamais le grain patient.
--
-- final = 1 : applique automatiquement FINAL à la lecture des ReplacingMergeTree,
-- pour que Metabase ne voie jamais de doublon transitoire entre une réécriture
-- et le merge de fond de ClickHouse.

CREATE USER IF NOT EXISTS pilotage_user IDENTIFIED WITH plaintext_password BY 'pilotage_pwd' SETTINGS final = 1;
CREATE USER IF NOT EXISTS recherche_user IDENTIFIED WITH plaintext_password BY 'recherche_pwd' SETTINGS final = 1;

GRANT SELECT ON gold.dms_par_service TO pilotage_user;
GRANT SELECT ON gold.urgences_par_jour TO pilotage_user;
GRANT SELECT ON gold.readmission_par_service TO pilotage_user;
GRANT SELECT ON gold.alertes_par_jour TO pilotage_user;
GRANT SELECT ON gold.admissions_par_age TO pilotage_user;

GRANT SELECT ON gold.prevalence_pathologie TO recherche_user;
GRANT SELECT ON gold.cohorte_age_sexe TO recherche_user;
