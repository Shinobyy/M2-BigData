-- gold.readmission_par_service <- silver.fact_sejours + dim_service
-- Grain : service x mois.
--
-- readmission_30j est deja calcule au grain du sejour en silver (lagInFrame
-- sur l'historique complet du patient) : ici on ne fait que l'agreger.

INSERT INTO gold.readmission_par_service
SELECT service_code, service_label, mois, nb_sejours, nb_readmissions,
       taux_readmission_pct, now() AS _processed_at
FROM (
    SELECT
        f.service_code AS service_code,
        d.service_label AS service_label,
        toStartOfMonth(f.admission_ts) AS mois,
        count() AS nb_sejours,
        sum(f.readmission_30j) AS nb_readmissions,
        round(100 * avg(f.readmission_30j), 2) AS taux_readmission_pct
    FROM silver.fact_sejours AS f FINAL
    JOIN silver.dim_service AS d FINAL ON f.service_code = d.service_code
    GROUP BY service_code, service_label, mois
)
WHERE (service_code, mois, nb_sejours, nb_readmissions, taux_readmission_pct) NOT IN (
    SELECT service_code, mois, nb_sejours, nb_readmissions, taux_readmission_pct
    FROM gold.readmission_par_service FINAL
)
