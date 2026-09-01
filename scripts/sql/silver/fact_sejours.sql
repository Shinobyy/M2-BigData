-- silver.fact_sejours <- bronze.sejours (grain : 1 sejour)
--
-- Controles : admission renseignee, coherence temporelle, integrite
-- referentielle. Un sejour sans date de sortie est legitime (patient encore
-- hospitalise), il est conserve.
--
-- readmission_30j a besoin de tout l'historique du patient : on reprend donc
-- l'integralite des sejours des patients concernes par une nouveaute, et pas
-- seulement les sejours nouvellement deposes.

INSERT INTO silver.fact_sejours
SELECT
    stay_id, patient_id, service_code, admission_ts, discharge_ts,
    if(isNull(discharge_ts), NULL, dateDiff('hour', admission_ts, discharge_ts)) AS duree_sejour_h,
    admission_mode, discharge_mode,
    if(
        sortie_precedente IS NOT NULL
        AND dateDiff('day', sortie_precedente, admission_ts) <= 30,
        1, 0
    ) AS readmission_30j,
    -- Age a la date de l'admission, et non a la date du calcul : un patient
    -- admis a 12 ans puis a 42 ans doit compter dans deux tranches
    -- differentes, pas deux fois dans la meme.
    toUInt8(age('year', birth_date, admission_ts)) AS age_at_admission,
    _ingested_at,
    now() AS _processed_at
FROM (
    SELECT
        *,
        lagInFrame(discharge_ts) OVER (PARTITION BY patient_id ORDER BY admission_ts) AS sortie_precedente
    FROM (
        SELECT b.stay_id AS stay_id, b.patient_id AS patient_id,
               b.service_code AS service_code, b.admission_ts AS admission_ts,
               b.discharge_ts AS discharge_ts, b.admission_mode AS admission_mode,
               b.discharge_mode AS discharge_mode, b._ingested_at AS _ingested_at,
               p.birth_date AS birth_date
        -- La jointure sur dim_patient sert a deux choses a la fois : ramener
        -- birth_date (pour l'age a l'admission) et faire office de controle
        -- d'integrite referentielle, un sejour dont le patient est inconnu ne
        -- trouvant pas de contrepartie.
        FROM bronze.sejours AS b
        JOIN silver.dim_patient AS p FINAL ON b.patient_id = p.patient_id
        WHERE isNotNull(b.admission_ts)
          AND (b.discharge_ts IS NULL OR b.discharge_ts >= b.admission_ts)
          AND b.service_code IN (SELECT service_code FROM silver.dim_service FINAL)
          AND b.patient_id IN (
            SELECT DISTINCT patient_id FROM bronze.sejours
            WHERE _ingested_at > {watermark}
          )
        ORDER BY b._ingested_at DESC
        LIMIT 1 BY b.stay_id
    )
)
WHERE (stay_id, patient_id, service_code, admission_ts, discharge_ts,
       duree_sejour_h, admission_mode, discharge_mode, readmission_30j,
       age_at_admission) NOT IN (
    SELECT stay_id, patient_id, service_code, admission_ts, discharge_ts,
           duree_sejour_h, admission_mode, discharge_mode, readmission_30j,
           age_at_admission
    FROM silver.fact_sejours FINAL
)
