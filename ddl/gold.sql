-- Gold : une table par indicateur métier (voir diagrams/gold.puml)
--
-- Chaque table contient le KPI déjà agrégé, prêt à être affiché sans calcul :
-- les dashboards lisent, ils n'agrègent pas. Le grain de chaque table est son
-- axe d'analyse (service x mois, jour, tranche d'age...).
--
-- Toutes en ReplacingMergeTree(_processed_at) ordonnées sur leur grain : le
-- moteur garantit qu'une clé n'apparaît jamais deux fois, quel que soit le
-- déroulé de l'écriture.
--
-- Le recalcul se fait par TRUNCATE puis réécriture intégrale (cf.
-- scripts/insert-to-gold.py) : ReplacingMergeTree écrase les lignes de même
-- clé mais ne supprime pas, une clé disparue de Silver resterait donc affichée
-- avec ses anciennes valeurs. Le prix est une brève fenêtre, à chaque cycle,
-- pendant laquelle un dashboard voit la table vide.

CREATE DATABASE IF NOT EXISTS gold;

-- Découpage en tranches d'âge, défini une seule fois et partagé par les deux
-- KPI qui l'utilisent (admissions_par_age, cohorte_age_sexe) : ils ne peuvent
-- donc pas diverger. Le bornage vit ici, en Gold, et non en Silver : c'est un
-- choix de présentation, pas une propriété de la donnée. Silver stocke l'âge
-- révolu à l'événement, changer les bornes ne demande qu'un recalcul de Gold.
-- OR REPLACE et non IF NOT EXISTS : ClickHouse ne compare pas les corps, il
-- ne regarde que le nom. Avec IF NOT EXISTS, modifier les bornes ci-dessous ne
-- se propagerait jamais au serveur -- le fichier et la fonction vivante
-- divergeraient en silence.
CREATE OR REPLACE FUNCTION age_group AS (a) -> multiIf(
    a < 10, '0-9',
    a <= 19, '10-19',
    a <= 29, '20-29',
    a <= 39, '30-39',
    a <= 49, '40-49',
    a <= 59, '50-59',
    a <= 69, '60-69',
    a <= 79, '70-79',
    a <= 89, '80-89',
    a <= 99, '90-99',
    '100+'
);
-- 0-9 / 10- 19 / 20-29 / 30-39 / 40-49 / 50-59 / 60-69 / 70-79 / 80-89 / 90 - 99



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

-- Taux de réadmission à 30 jours, par mois, tous services confondus.
-- Le taux est recalculé au grain mois et non moyenné depuis un grain plus
-- fin : une moyenne de taux par service donnerait un poids identique à un
-- service de 20 séjours et à un service de 2 000.
CREATE TABLE IF NOT EXISTS gold.readmission_par_mois
(
    mois                   Date,
    nb_sejours             UInt32,
    nb_readmissions        UInt32,
    taux_readmission_pct   Float64,
    _processed_at          DateTime
)
ENGINE = ReplacingMergeTree(_processed_at)
ORDER BY mois;

-- Surveillance des constantes : relevés en alerte par jour, ventilés par type.
-- Un même relevé peut violer plusieurs bornes : nb_alertes_total compte les
-- relevés en alerte, pas la somme des trois colonnes.
-- Grain (jour) seul : tous services confondus. nb_sejours_concernes est donc
-- recalculé à ce grain et non sommé -- un séjour transféré d'un service à un
-- autre dans la même journée ne doit compter qu'une fois.
CREATE TABLE IF NOT EXISTS gold.alertes_par_jour
(
    jour                  Date,
    nb_releves            UInt32,
    nb_alertes_fc         UInt32,
    nb_alertes_spo2       UInt32,
    nb_alertes_temp       UInt32,
    nb_alertes_total      UInt32,
    nb_sejours_concernes  UInt32,
    _processed_at         DateTime
)
ENGINE = ReplacingMergeTree(_processed_at)
ORDER BY jour;

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



-- NOUVELLES KPIS

CREATE TABLE IF NOT EXISTS gold.activite_dms_par_categorie_service
(
    categorie             String,
    mois                  Date,
    nb_sejours_termines   UInt32,
    nb_sejours_en_cours   UInt32,
    dms_jours             Float64,
    _processed_at         DateTime
)
ENGINE = ReplacingMergeTree(_processed_at)
ORDER BY (categorie, mois);

CREATE TABLE IF NOT EXISTS gold.actes_par_service
(
    service_code          String,
    mois                  Date,
    nb_actes              UInt32,
    _processed_at         DateTime
)
ENGINE = ReplacingMergeTree(_processed_at)
ORDER BY (service_code, mois);


-- Actes realises par code CCAM et par mois.
-- Il n'existe pas de "type d'acte" dans les sources : ni ccam.csv ni
-- actes.parquet n'en portent. Le seul axe disponible est le code lui-meme,
-- accompagne de son libelle -- meme montage que prevalence_pathologie
-- (code_cim10 + pathologie). Le libelle est denormalise ici pour que le
-- dashboard n'ait aucune jointure a faire.
CREATE TABLE IF NOT EXISTS gold.actes_par_code_ccam
(
    code_ccam             String,
    libelle               String,
    mois                  Date,
    nb_actes              UInt32,
    nb_sejours_concernes  UInt32,
    _processed_at         DateTime
)
ENGINE = ReplacingMergeTree(_processed_at)
ORDER BY (code_ccam, mois);


CREATE TABLE IF NOT EXISTS gold.densite_actes_par_lits
(
    service_code          String,
    mois                  Date,
    nb_actes              UInt32,
    capacite_lits         UInt32,
    densite_actes_par_lits Float64,
    _processed_at         DateTime
)
ENGINE = ReplacingMergeTree(_processed_at)
ORDER BY (service_code, mois);

-- Recettes T2A : somme des tarifs des actes realises, par service et par mois.
-- UInt64 et non Float64 : tarif_euros est un entier en euros (cf.
-- bronze.ccam), une somme d'entiers reste un entier. Un flottant
-- reintroduirait au niveau agrege ce qu'on evite au niveau unitaire.
CREATE TABLE IF NOT EXISTS gold.montant_facture_par_service
(
    service_code          String,
    mois                  Date,
    nb_actes              UInt32,
    montant_facture_euros UInt64,
    _processed_at         DateTime
)
ENGINE = ReplacingMergeTree(_processed_at)
ORDER BY (service_code, mois);






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
GRANT SELECT ON gold.readmission_par_mois TO pilotage_user;
GRANT SELECT ON gold.alertes_par_jour TO pilotage_user;
GRANT SELECT ON gold.admissions_par_age TO pilotage_user;
GRANT SELECT ON gold.activite_dms_par_categorie_service TO pilotage_user;
GRANT SELECT ON gold.actes_par_service TO pilotage_user;
GRANT SELECT ON gold.actes_par_code_ccam TO pilotage_user;
GRANT SELECT ON gold.densite_actes_par_lits TO pilotage_user;
GRANT SELECT ON gold.montant_facture_par_service TO pilotage_user;

GRANT SELECT ON gold.prevalence_pathologie TO recherche_user;
GRANT SELECT ON gold.cohorte_age_sexe TO recherche_user;
