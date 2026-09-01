-- gold.prevalence_pathologie <- silver.fact_diagnostics + dim_cim10
-- Grain : code CIM-10.
--
-- RGPD petits effectifs : le HAVING filtre A L'ECRITURE, donc aucune cohorte
-- de moins de 5 patients n'est materialisee dans l'entrepot. La regle n'est
-- pas contournable, meme avec un acces direct a la table.

INSERT INTO gold.prevalence_pathologie
SELECT code_cim10, pathologie, taille_cohorte, nb_sejours, now() AS _processed_at
FROM (
    SELECT
        f.code_cim10 AS code_cim10,
        c.libelle AS pathologie,
        uniqExact(f.patient_id) AS taille_cohorte,
        uniqExact(f.stay_id) AS nb_sejours
    FROM silver.fact_diagnostics AS f FINAL
    JOIN silver.dim_cim10 AS c FINAL ON f.code_cim10 = c.code_cim10
    GROUP BY code_cim10, pathologie
    HAVING taille_cohorte >= 5
)
WHERE (code_cim10, taille_cohorte, nb_sejours) NOT IN (
    SELECT code_cim10, taille_cohorte, nb_sejours FROM gold.prevalence_pathologie FINAL
)
