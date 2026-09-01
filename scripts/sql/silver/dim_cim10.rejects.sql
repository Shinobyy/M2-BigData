-- Lignes de bronze.cim10 ecartees par les controles de dim_cim10.sql.

INSERT INTO silver._rejets (source_table, regle, cle, details, _ingested_at)
SELECT 'bronze.cim10', 'code ou libelle vide', code_cim10,
       concat('libelle=', libelle), _ingested_at
FROM bronze.cim10
WHERE _ingested_at > {watermark}
  AND NOT (code_cim10 != '' AND libelle != '')
