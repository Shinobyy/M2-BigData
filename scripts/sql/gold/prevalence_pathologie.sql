INSERT INTO gold.prevalence_pathologie
SELECT code_cim10, pathologie, taille_cohorte, nb_sejours, now() AS _processed_at
FROM (

    SELECT
        f.code_cim10 AS code_cim10,
        c.libelle AS pathologie,
        count(DISTINCT f.patient_id) AS taille_cohorte,
        uniqExact(f.stay_id) AS nb_sejours
    FROM silver.fact_diagnostics AS f FINAL
    JOIN silver.dim_cim10 AS c ON f.code_cim10 = c.code_cim10
    GROUP BY code_cim10, pathologie
    -- HAVING taille_cohorte >= 5

)
