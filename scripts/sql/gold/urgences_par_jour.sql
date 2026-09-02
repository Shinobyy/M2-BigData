-- gold.urgences_par_jour <- silver.fact_sejours
-- Grain : jour d'admission.

INSERT INTO gold.urgences_par_jour
SELECT jour, nb_passages, now() AS _processed_at
FROM (
    SELECT toDate(admission_ts) AS jour, count() AS nb_passages
    FROM silver.fact_sejours FINAL
    WHERE service_code = 'URGENCES'
    GROUP BY jour
)
