INSERT INTO gold.actes_par_service
SELECT service_code, mois, nb_actes, now() AS _processed_at
FROM (
    SELECT
        service_code,
        toStartOfMonth(acte_ts) AS mois,
        count() AS nb_actes
    FROM silver.fact_actes FINAL
    GROUP BY service_code, mois
)
