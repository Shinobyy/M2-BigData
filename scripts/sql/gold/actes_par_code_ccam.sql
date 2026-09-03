INSERT INTO gold.actes_par_code_ccam
SELECT code_ccam, libelle, mois, nb_actes, nb_sejours_concernes,
       now() AS _processed_at
FROM (
    SELECT
        a.code_ccam AS code_ccam,
        c.libelle AS libelle,
        toStartOfMonth(a.acte_ts) AS mois,
        count() AS nb_actes,
        uniqExact(a.stay_id) AS nb_sejours_concernes
    FROM silver.fact_actes AS a FINAL
    JOIN silver.dim_ccam AS c FINAL ON a.code_ccam = c.code_ccam
    GROUP BY code_ccam, libelle, mois
)
