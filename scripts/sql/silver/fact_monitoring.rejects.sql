-- Releves de bronze.monitoring hors plage physiologique.

INSERT INTO silver._rejets (source_table, regle, cle, details, _ingested_at)
SELECT
    'bronze.monitoring',
    -- Un releve peut violer plusieurs bornes : on ne liste que celles
    -- reellement en cause. concat_ws ne convient pas ici (il propage le NULL
    -- et conserve les separateurs des chaines vides), d'ou le filtrage
    -- explicite du tableau.
    arrayStringConcat(arrayFilter(x -> x != '', [
        if(NOT (heart_rate BETWEEN 20 AND 250), 'FC hors plage 20-250', ''),
        if(NOT (spo2 BETWEEN 50 AND 100), 'SpO2 hors plage 50-100', ''),
        if(NOT (temp_c BETWEEN 30 AND 45), 'temperature hors plage 30-45', '')
    ]), ' + '),
    concat(stay_id, '@', toString(ts)),
    concat('heart_rate=', toString(heart_rate),
           ' spo2=', toString(spo2),
           ' temp_c=', toString(temp_c)),
    _ingested_at
FROM bronze.monitoring
WHERE _ingested_at > {watermark}
  AND NOT (
    heart_rate BETWEEN 20 AND 250
    AND spo2 BETWEEN 50 AND 100
    AND temp_c BETWEEN 30 AND 45
  )
