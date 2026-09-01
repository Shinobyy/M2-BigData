-- silver.dim_service <- bronze.services
-- Controle : code et libelle non vides.

INSERT INTO silver.dim_service
SELECT service_code, service_label, _ingested_at
FROM (
    SELECT service_code, service_label, _ingested_at
    FROM bronze.services
    WHERE service_code != '' AND service_label != ''
      AND _ingested_at > {watermark}
    ORDER BY _ingested_at DESC
    LIMIT 1 BY service_code
)
WHERE (service_code, service_label) NOT IN (
    SELECT service_code, service_label FROM silver.dim_service FINAL
)
