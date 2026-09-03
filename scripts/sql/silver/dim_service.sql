INSERT INTO silver.dim_service
SELECT service_code, service_label, categorie, pole, capacite_lits, _ingested_at
FROM (
    SELECT
        s.service_code AS service_code,
        s.service_label AS service_label,
        d.categorie AS categorie,
        d.pole AS pole,
        d.capacite_lits AS capacite_lits,
        greatest(s._ingested_at, d._ingested_at) AS _ingested_at
    FROM (
        SELECT service_code, service_label, _ingested_at
        FROM bronze.services
        WHERE service_code != '' AND service_label != ''
        ORDER BY _ingested_at DESC
        LIMIT 1 BY service_code
    ) AS s
    LEFT JOIN (
        SELECT service_code, categorie, pole, capacite_lits, _ingested_at
        FROM bronze.description_service
        ORDER BY _ingested_at DESC
        LIMIT 1 BY service_code
    ) AS d ON s.service_code = d.service_code
)
WHERE (service_code, service_label, categorie, pole, capacite_lits) NOT IN (
    SELECT service_code, service_label, categorie, pole, capacite_lits
    FROM silver.dim_service FINAL
)
