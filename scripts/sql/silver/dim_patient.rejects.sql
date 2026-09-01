-- Lignes de bronze.patients ecartees par les controles de dim_patient.sql.

INSERT INTO silver._rejets (source_table, regle, cle, details, _ingested_at)
SELECT
    'bronze.patients',
    multiIf(
        isNull(birth_date), 'date de naissance manquante',
        birth_date > today(), 'date de naissance dans le futur',
        'sexe non normalisable (attendu M ou F)'
    ),
    patient_id,
    concat('birth_date=', toString(birth_date), ' sex=', sex),
    _ingested_at
FROM bronze.patients
WHERE _ingested_at > {watermark}
  AND NOT (isNotNull(birth_date) AND birth_date <= today() AND upper(sex) IN ('M', 'F'))
