-- Lignes de bronze.services ecartees par les controles de dim_service.sql.

INSERT INTO silver._rejets (source_table, regle, cle, details, _ingested_at)
SELECT 'bronze.services', 'code ou libelle vide', service_code,
       concat('service_label=', service_label), _ingested_at
FROM bronze.services
WHERE _ingested_at > {watermark}
  AND NOT (service_code != '' AND service_label != '')
