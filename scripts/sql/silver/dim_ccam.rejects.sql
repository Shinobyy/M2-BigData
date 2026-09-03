INSERT INTO silver._rejets (source_table, regle, cle, details, _ingested_at)
SELECT 'bronze.ccam', 'code ou libelle vide', code_ccam,
       concat('libelle=', libelle), _ingested_at
FROM bronze.ccam
WHERE _ingested_at > {watermark}
  AND NOT (code_ccam != '' AND libelle != '')
