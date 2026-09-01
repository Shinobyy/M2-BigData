-- silver.dim_patient <- bronze.patients
--
-- Controles : date de naissance renseignee et non future, sexe normalisable.
-- Doublons (retour quotidien du meme patient) -> version la plus recente.
--
-- Aucune colonne d'age ici : l'age est une mesure de l'EVENEMENT, il est porte
-- par les faits (voir le commentaire de la table dans ddl/silver.sql).

INSERT INTO silver.dim_patient
SELECT patient_id, birth_date, sex, region_code, _ingested_at
FROM (
    SELECT
        patient_id, birth_date, upper(sex) AS sex, region_code,
        _ingested_at
    FROM bronze.patients
    WHERE isNotNull(birth_date) AND birth_date <= today()
      AND upper(sex) IN ('M', 'F')
      AND _ingested_at > {watermark}
    ORDER BY _ingested_at DESC
    LIMIT 1 BY patient_id
)
WHERE (patient_id, birth_date, sex, region_code) NOT IN (
    SELECT patient_id, birth_date, sex, region_code
    FROM silver.dim_patient FINAL
)
