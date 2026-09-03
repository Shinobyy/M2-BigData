-- Actes dont le code CCAM est absent du referentiel.

INSERT INTO silver._rejets (source_table, regle, cle, details, _ingested_at)
SELECT 'bronze.actes', 'code CCAM inconnu du referentiel',
       concat(stay_id, '/', code_ccam),
       concat('acte_ts=', toString(acte_ts)), _ingested_at
FROM bronze.actes
WHERE _ingested_at > {watermark}
  AND code_ccam NOT IN (SELECT code_ccam FROM bronze.ccam)
