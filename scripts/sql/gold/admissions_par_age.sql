-- gold.admissions_par_age <- silver.fact_sejours
-- Grain : tranche d'age A L'ADMISSION.
--
-- age_group() est une fonction SQL definie dans ddl/gold.sql : le decoupage en
-- tranches vit dans l'entrepot, partage par les deux KPI qui l'utilisent, pour
-- qu'ils ne puissent pas diverger.
--
-- Un patient hospitalise a 17 ans puis a 19 ans compte dans deux tranches :
-- c'est le comportement voulu, mais il implique que la somme des nb_patients
-- peut depasser le nombre de patients distincts.

INSERT INTO gold.admissions_par_age
SELECT age_group, nb_admissions, nb_patients, now() AS _processed_at
FROM (
    SELECT
        age_group(age_at_admission) AS age_group,
        count() AS nb_admissions,
        uniqExact(patient_id) AS nb_patients
    FROM silver.fact_sejours FINAL
    GROUP BY age_group
)
WHERE (age_group, nb_admissions, nb_patients) NOT IN (
    SELECT age_group, nb_admissions, nb_patients FROM gold.admissions_par_age FINAL
)
