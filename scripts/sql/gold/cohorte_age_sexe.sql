-- gold.cohorte_age_sexe <- silver.fact_sejours (age) + dim_patient (sexe)
-- Grain : tranche d'age A L'INCLUSION x sexe.
--
-- L'age retenu est celui a la PREMIERE admission connue du patient (argMin sur
-- admission_ts). C'est la convention de la recherche clinique -- le "Table 1"
-- d'un article decrit la population a l'entree dans l'etude, pas a la date de
-- publication -- et c'est la seule definition qui ne derive pas avec le temps.
--
-- Consequence assumee : la cohorte porte sur les patients ayant au moins un
-- sejour, et non sur l'ensemble des patients connus du referentiel.
--
-- RGPD petits effectifs : le HAVING filtre A L'ECRITURE, donc aucune cohorte
-- de moins de 5 patients n'est materialisee. La regle n'est pas contournable,
-- meme avec un acces direct a la table.

INSERT INTO gold.cohorte_age_sexe
SELECT age_group, sex, taille_cohorte, now() AS _processed_at
FROM (
    SELECT
        age_group(i.age_inclusion) AS age_group,
        p.sex AS sex,
        uniqExact(i.patient_id) AS taille_cohorte
    FROM (
        SELECT patient_id,
               argMin(age_at_admission, admission_ts) AS age_inclusion
        FROM silver.fact_sejours FINAL
        GROUP BY patient_id
    ) AS i
    JOIN silver.dim_patient AS p FINAL ON i.patient_id = p.patient_id
    GROUP BY age_group, sex
    HAVING taille_cohorte >= 5
)
