INSERT INTO gold.montant_facture_par_service
SELECT service_code, mois, nb_actes, montant_facture_euros,
       now() AS _processed_at
FROM (
    SELECT
        a.service_code AS service_code,
        toStartOfMonth(a.acte_ts) AS mois,
        count() AS nb_actes,
        sum(c.tarif_euros) AS montant_facture_euros
    FROM silver.fact_actes AS a FINAL
    JOIN silver.dim_ccam AS c FINAL ON a.code_ccam = c.code_ccam
    GROUP BY service_code, mois
)
