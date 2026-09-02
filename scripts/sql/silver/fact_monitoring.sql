-- silver.fact_monitoring <- bronze.monitoring (grain : 1 releve)
--
-- Controle : plages physiologiques. Flux volumineux et purement additif ->
-- watermark seul, pas d'anti-jointure qui couterait cher sur ce volume.
-- patient_id / service_code sont denormalises depuis fact_sejours.
--
-- Attention a ne pas confondre deux jeux de bornes :
--   plausibilite physiologique (ci-dessous, WHERE) -> erreur de mesure, ecarte
--   seuil d'alerte clinique (is_alerte_*)          -> mesure valide, conservee

INSERT INTO silver.fact_monitoring
SELECT
    m.stay_id, m.ts, s.patient_id, s.service_code,
    m.heart_rate, m.spo2, m.temp_c,
    if(m.heart_rate > 120 OR m.heart_rate < 50, 1, 0) AS is_alerte_fc,
    if(m.spo2 < 92, 1, 0) AS is_alerte_spo2,
    if(m.temp_c > 38.5, 1, 0) AS is_alerte_temp,
    m._ingested_at
FROM bronze.monitoring m
JOIN bronze.sejours AS s ON m.stay_id = s.stay_id
WHERE m.heart_rate BETWEEN 20 AND 250
  AND m.spo2 BETWEEN 50 AND 100
  AND m.temp_c BETWEEN 30 AND 45
  AND m._ingested_at > {watermark}
