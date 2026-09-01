-- Diagnostics dont le code CIM-10 est absent du referentiel.

INSERT INTO silver._rejets (source_table, regle, cle, details, _ingested_at)
SELECT 'bronze.diagnostics', 'code CIM-10 inconnu du referentiel',
       concat(stay_id, '/', d.code_cim10),
       concat('type=', d.type), _ingested_at
FROM bronze.diagnostics
ARRAY JOIN diagnostics AS d
WHERE _ingested_at > {watermark}
  AND d.code_cim10 NOT IN (SELECT code_cim10 FROM silver.dim_cim10 FINAL)
