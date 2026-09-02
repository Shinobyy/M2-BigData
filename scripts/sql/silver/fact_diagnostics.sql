-- silver.fact_diagnostics <- bronze.diagnostics (grain : 1 diagnostic)
--
-- Table de faits sans mesure ("factless fact table"), aplatie depuis la
-- structure imbriquee du JSON via ARRAY JOIN : 1 ligne bronze -> N lignes
-- silver. Controle : le code CIM-10 doit exister au referentiel.
--
-- La jointure sur silver.fact_sejours (deja insere : voir TABLE_ORDER) sert de
-- controle d'integrite referentielle et ramene patient_id, service_code et
-- l'age, deja calcules la-bas. Le grain (sejour x code) n'a pas de date
-- propre : l'age de reference est celui de l'admission.
--
-- Pas de deduplication ici : la cle de tri de la table cible est exactement le
-- grain (stay_id, code_cim10), le ReplacingMergeTree(_ingested_at) garde donc
-- la version la plus recente tout seul.

INSERT INTO silver.fact_diagnostics
SELECT 
  b.stay_id,
  d.code_cim10,
  s.patient_id,
  s.service_code,
  d.type,
  toUInt8(age('year', p.birth_date, s.admission_ts)) AS age_at_diagnostic,
  b._ingested_at
FROM bronze.diagnostics AS b
ARRAY JOIN b.diagnostics AS d
JOIN bronze.sejours AS s ON b.stay_id = s.stay_id
JOIN silver.dim_patient as p ON s.patient_id = p.patient_id
WHERE b._ingested_at > {watermark}
  AND d.code_cim10 IN (SELECT code_cim10 FROM silver.dim_cim10 FINAL)