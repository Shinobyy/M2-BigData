-- silver.fact_diagnostics <- bronze.diagnostics (grain : 1 diagnostic)
--
-- Table de faits sans mesure ("factless fact table"), aplatie depuis la
-- structure imbriquee du JSON via ARRAY JOIN : 1 ligne bronze -> N lignes
-- silver. Controle : le code CIM-10 doit exister au referentiel.

INSERT INTO silver.fact_diagnostics
SELECT stay_id, code_cim10, patient_id, service_code, type,
       age_at_diagnostic, _ingested_at
FROM (
    SELECT
        b.stay_id AS stay_id, d.code_cim10 AS code_cim10,
        s.patient_id AS patient_id, s.service_code AS service_code,
        d.type AS type,
        -- Le grain (sejour x code) n'a pas de date propre : la date de
        -- reference est l'admission du sejour, deja ramenee par la jointure
        -- qui denormalise patient_id et service_code.
        s.age_at_admission AS age_at_diagnostic,
        b._ingested_at AS _ingested_at
    FROM bronze.diagnostics b
    ARRAY JOIN b.diagnostics AS d
    JOIN silver.fact_sejours AS s FINAL ON b.stay_id = s.stay_id
    WHERE d.code_cim10 IN (SELECT code_cim10 FROM silver.dim_cim10 FINAL)
      AND b._ingested_at > {watermark}
    ORDER BY b._ingested_at DESC
    LIMIT 1 BY b.stay_id, d.code_cim10
)
WHERE (stay_id, code_cim10, patient_id, service_code, type,
       age_at_diagnostic) NOT IN (
    SELECT stay_id, code_cim10, patient_id, service_code, type,
           age_at_diagnostic
    FROM silver.fact_diagnostics FINAL
)
