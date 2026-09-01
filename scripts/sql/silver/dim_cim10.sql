-- silver.dim_cim10 <- bronze.cim10
-- Controle : code et libelle non vides.

INSERT INTO silver.dim_cim10
SELECT code_cim10, libelle, _ingested_at
FROM (
    SELECT code_cim10, libelle, _ingested_at
    FROM bronze.cim10
    WHERE code_cim10 != '' AND libelle != ''
      AND _ingested_at > {watermark}
    ORDER BY _ingested_at DESC
    LIMIT 1 BY code_cim10
)
WHERE (code_cim10, libelle) NOT IN (
    SELECT code_cim10, libelle FROM silver.dim_cim10 FINAL
)
