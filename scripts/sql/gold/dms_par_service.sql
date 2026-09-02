-- gold.dms_par_service <- silver.fact_sejours + dim_service
-- Grain : service x mois.
--
-- Les sejours en cours sont exclus de la moyenne (duree inconnue) mais comptes
-- a part, dans nb_sejours_en_cours.

INSERT INTO gold.dms_par_service
SELECT service_code, service_label, mois, nb_sejours_termines,
       nb_sejours_en_cours, dms_jours, now() AS _processed_at
FROM (
    SELECT
        f.service_code AS service_code,
        d.service_label AS service_label,
        toStartOfMonth(f.admission_ts) AS mois,
        countIf(f.discharge_ts IS NOT NULL) AS nb_sejours_termines,
        countIf(f.discharge_ts IS NULL) AS nb_sejours_en_cours,
        round(avgIf(f.duree_sejour_h, f.duree_sejour_h IS NOT NULL) / 24, 2) AS dms_jours
    FROM silver.fact_sejours AS f FINAL
    JOIN silver.dim_service AS d FINAL ON f.service_code = d.service_code
    GROUP BY service_code, service_label, mois
)
