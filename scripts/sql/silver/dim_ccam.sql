INSERT INTO silver.dim_ccam
SELECT code_ccam, libelle, tarif_euros, _ingested_at
FROM (
    SELECT code_ccam, libelle, tarif_euros, _ingested_at
    FROM bronze.ccam
    WHERE code_ccam != '' AND libelle != ''
      AND _ingested_at > {watermark}
    ORDER BY _ingested_at DESC
    LIMIT 1 BY code_ccam
)
WHERE (code_ccam, libelle, tarif_euros) NOT IN (
    SELECT code_ccam, libelle, tarif_euros FROM silver.dim_ccam FINAL
)
