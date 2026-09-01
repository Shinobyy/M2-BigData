-- Lignes de bronze.sejours ecartees par les controles de fact_sejours.sql.
-- L'ordre du multiIf donne la premiere cause rencontree, pas toutes.

INSERT INTO silver._rejets (source_table, regle, cle, details, _ingested_at)
SELECT
    'bronze.sejours',
    multiIf(
        isNull(admission_ts), 'date d''admission manquante',
        isNotNull(discharge_ts) AND discharge_ts < admission_ts,
            'incoherence temporelle (sortie avant admission)',
        patient_id NOT IN (SELECT patient_id FROM silver.dim_patient FINAL),
            'patient inconnu du referentiel',
        'service inconnu du referentiel'
    ),
    stay_id,
    concat('patient_id=', patient_id, ' service_code=', service_code,
           ' admission=', toString(admission_ts),
           ' sortie=', ifNull(toString(discharge_ts), 'NULL')),
    _ingested_at
FROM bronze.sejours
WHERE _ingested_at > {watermark}
  AND NOT (
    isNotNull(admission_ts)
    AND (discharge_ts IS NULL OR discharge_ts >= admission_ts)
    AND patient_id IN (SELECT patient_id FROM silver.dim_patient FINAL)
    AND service_code IN (SELECT service_code FROM silver.dim_service FINAL)
  )
