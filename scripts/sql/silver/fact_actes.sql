-- silver.fact_actes <- bronze.actes (grain : 1 acte)
--
-- Controle : le code CCAM doit exister au referentiel.
--
-- Aucune lecture de silver : le sejour et le referentiel CCAM sont relus
-- depuis bronze. patient_id et service_code sont denormalises depuis
-- bronze.sejours, jamais empruntes a un autre fait.
--
-- Pas de deduplication de la jointure : la cle de tri de la table cible est
-- exactement le grain (stay_id, code_ccam, acte_ts), le
-- ReplacingMergeTree(_ingested_at) garde la version la plus recente tout seul.

INSERT INTO silver.fact_actes
SELECT a.stay_id, a.code_ccam, a.acte_ts, s.patient_id, s.service_code,
       a._ingested_at
FROM bronze.actes AS a
JOIN bronze.sejours AS s ON a.stay_id = s.stay_id
WHERE a._ingested_at > {watermark}
  AND a.code_ccam IN (SELECT code_ccam FROM bronze.ccam)
