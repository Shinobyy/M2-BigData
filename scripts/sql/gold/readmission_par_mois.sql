-- gold.readmission_par_mois <- silver.fact_sejours
-- Grain : mois d'admission, tous services confondus.
--
-- readmission_30j est deja calcule au grain du sejour en silver (lagInFrame
-- sur l'historique complet du patient, decès exclus) : ici on ne fait que
-- l'agreger.
--
-- Le taux est recalcule a ce grain, et non moyenne depuis un grain plus fin :
-- avg() sur des taux par service donnerait le meme poids a un service de 20
-- sejours et a un service de 2 000.
--
-- Plus de jointure sur dim_service : le grain ne porte plus le service.

INSERT INTO gold.readmission_par_mois
SELECT mois, nb_sejours, nb_readmissions, taux_readmission_pct,
       now() AS _processed_at
FROM (
    SELECT
        toStartOfMonth(f.admission_ts) AS mois,
        count() AS nb_sejours,
        sum(f.readmission_30j) AS nb_readmissions,
        round(100 * avg(f.readmission_30j), 2) AS taux_readmission_pct
    FROM silver.fact_sejours AS f FINAL
    -- Denominateur restreint aux sejours CLOS : un sejour en cours n'a pas
    -- encore eu l'occasion d'etre suivi d'une readmission, le compter au
    -- denominateur diluerait le taux. Convention standard du KPI.
    WHERE f.discharge_ts IS NOT NULL
    GROUP BY mois
)
