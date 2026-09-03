INSERT INTO gold.densite_actes_par_lits
SELECT service_code, mois, nb_actes, capacite_lits,
       ifNull(round(nb_actes / nullIf(capacite_lits, 0), 2), 0) AS densite,
       now() AS _processed_at
FROM (
    SELECT
        a.service_code AS service_code,
        toStartOfMonth(a.acte_ts) AS mois,
        count() AS nb_actes,
        any(d.capacite_lits) AS capacite_lits
    FROM silver.fact_actes AS a FINAL
    JOIN silver.dim_service AS d FINAL ON a.service_code = d.service_code
    GROUP BY service_code, mois
)
