-- gold.alertes_par_jour <- silver.fact_monitoring
-- Grain : jour de la mesure, tous services confondus.
--
-- nb_alertes_total compte les releves EN ALERTE, pas la somme des trois
-- colonnes de detail : un meme releve peut violer plusieurs seuils.
--
-- nb_sejours_concernes est recalcule au grain jour, et non somme depuis un
-- grain plus fin : un sejour transfere d'un service a un autre dans la meme
-- journee ne doit compter qu'une fois.
--
-- Le jour est celui de la mesure (m.ts), pas celui du depot : un depot peut
-- contenir des mesures posterieures a sa propre date.
--
-- Plus de jointure sur dim_service : le grain ne porte plus le service, et
-- service_code est deja controle en amont, a l'insertion de fact_monitoring.

INSERT INTO gold.alertes_par_jour
SELECT jour, nb_releves, nb_alertes_fc, nb_alertes_spo2, nb_alertes_temp,
       nb_alertes_total, nb_sejours_concernes, now() AS _processed_at
FROM (
    SELECT
        toDate(m.ts) AS jour,
        count() AS nb_releves,
        countIf(m.is_alerte_fc = 1) AS nb_alertes_fc,
        countIf(m.is_alerte_spo2 = 1) AS nb_alertes_spo2,
        countIf(m.is_alerte_temp = 1) AS nb_alertes_temp,
        countIf(m.is_alerte_fc = 1 OR m.is_alerte_spo2 = 1 OR m.is_alerte_temp = 1) AS nb_alertes_total,
        uniqExactIf(m.stay_id, m.is_alerte_fc = 1 OR m.is_alerte_spo2 = 1 OR m.is_alerte_temp = 1) AS nb_sejours_concernes
    FROM silver.fact_monitoring m
    GROUP BY jour
)