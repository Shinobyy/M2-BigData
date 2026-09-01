-- gold.alertes_par_jour <- silver.fact_monitoring + dim_service
-- Grain : jour de la mesure x service.
--
-- nb_alertes_total compte les releves EN ALERTE, pas la somme des trois
-- colonnes de detail : un meme releve peut violer plusieurs seuils.
--
-- Le jour est celui de la mesure (m.ts), pas celui du depot : un depot peut
-- contenir des mesures posterieures a sa propre date.

INSERT INTO gold.alertes_par_jour
SELECT jour, service_code, service_label, nb_releves, nb_alertes_fc,
       nb_alertes_spo2, nb_alertes_temp, nb_alertes_total,
       nb_sejours_concernes, now() AS _processed_at
FROM (
    SELECT
        toDate(m.ts) AS jour,
        m.service_code AS service_code,
        d.service_label AS service_label,
        count() AS nb_releves,
        countIf(m.is_alerte_fc = 1) AS nb_alertes_fc,
        countIf(m.is_alerte_spo2 = 1) AS nb_alertes_spo2,
        countIf(m.is_alerte_temp = 1) AS nb_alertes_temp,
        countIf(m.is_alerte_fc = 1 OR m.is_alerte_spo2 = 1 OR m.is_alerte_temp = 1) AS nb_alertes_total,
        uniqExactIf(m.stay_id, m.is_alerte_fc = 1 OR m.is_alerte_spo2 = 1 OR m.is_alerte_temp = 1) AS nb_sejours_concernes
    FROM silver.fact_monitoring m
    JOIN silver.dim_service AS d FINAL ON m.service_code = d.service_code
    GROUP BY jour, service_code, service_label
)
WHERE (jour, service_code, nb_releves, nb_alertes_fc, nb_alertes_spo2,
       nb_alertes_temp, nb_alertes_total, nb_sejours_concernes) NOT IN (
    SELECT jour, service_code, nb_releves, nb_alertes_fc, nb_alertes_spo2,
           nb_alertes_temp, nb_alertes_total, nb_sejours_concernes
    FROM gold.alertes_par_jour FINAL
)
