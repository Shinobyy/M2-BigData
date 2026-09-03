INSERT INTO gold.activite_dms_par_categorie_service
SELECT categorie, mois, nb_sejours_termines, nb_sejours_en_cours, dms_jours,
       now() AS _processed_at
FROM (
    SELECT
        d.categorie AS categorie,
        toStartOfMonth(f.admission_ts) AS mois,
        countIf(f.discharge_ts IS NOT NULL) AS nb_sejours_termines,
        countIf(f.discharge_ts IS NULL) AS nb_sejours_en_cours,
        round(avgIf(f.duree_sejour_h, f.duree_sejour_h IS NOT NULL) / 24, 2) AS dms_jours
    FROM silver.fact_sejours AS f FINAL
    JOIN silver.dim_service AS d FINAL ON f.service_code = d.service_code
    GROUP BY categorie, mois
)
