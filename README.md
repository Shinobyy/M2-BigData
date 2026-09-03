# Modélisation des données

Diagrammes PlantUML correspondants : [diagrams/bronze.puml](diagrams/bronze.puml), [diagrams/silver.puml](diagrams/silver.puml), [diagrams/gold.puml](diagrams/gold.puml)

## Bronze

Données brutes, telles qu'ingérées depuis les sources (`source-filestorage/`), sans validation ni transformation.

**patients**
- patient_id
- birth_date
- sex
- region_code

**sejours**
- stay_id
- patient_id
- service_code
- admission_ts
- discharge_ts
- admission_mode
- discharge_mode

**diagnostics**
- stay_id
- diagnostics : Diagnostic[] *(tableau imbriqué, fidèle à la source)*
  - code_cim10
  - type

**monitoring**
- stay_id
- ts
- heart_rate
- spo2
- temp_c

**services**
- service_code
- service_label

**cim10**
- code_cim10
- libelle

## Silver

Données nettoyées, typées, dédupliquées et à l'intégrité référentielle vérifiée (patient_id/service_code/code_cim10 valides).

**patients**
- patient_id
- birth_date
- sex
- region_code

**sejours**
- stay_id
- patient_id
- service_code
- admission_ts
- discharge_ts
- admission_mode
- discharge_mode

**diagnostic** *(aplati depuis Bronze via ARRAY JOIN : 1 ligne Bronze -> N lignes Silver)*
- stay_id
- code_cim10
- type

**monitoring**
- stay_id
- ts
- heart_rate
- spo2
- temp_c

**services**
- service_code
- service_label

**cim10**
- code_cim10
- libelle

## Gold

Modèle en étoile / fact constellation : plusieurs facts partagent des dimensions communes. `stay_id` est une **dimension dégénérée** partagée par les 3 facts (pas de FK d'un fact vers un autre) : un incident sur un fact n'invalide pas les autres.

### Dimensions

**dim_patient**
- patient_id
- birth_date
- sex
- region_code
- age_group

**dim_service**
- service_code
- service_label

**dim_cim10**
- code_cim10
- libelle

### Facts

**fact_sejours** — grain = 1 séjour
- stay_id
- patient_id (FK dim_patient)
- service_code (FK dim_service)
- admission_ts
- discharge_ts
- duree_sejour_h
- admission_mode
- discharge_mode
- readmission_30j

**fact_monitoring** — grain = 1 relevé
- stay_id
- ts
- patient_id *(dénormalisé depuis silver_sejours à l'ETL, évite un join vers fact_sejours)*
- service_code *(dénormalisé, idem)*
- heart_rate
- spo2
- temp_c
- is_alerte_fc
- is_alerte_spo2
- is_alerte_temp

**fact_diagnostics** — grain = 1 diagnostic
- stay_id
- code_cim10 (FK dim_cim10)
- patient_id *(dénormalisé)*
- service_code *(dénormalisé)*
- type

> ⚠️ Ne jamais joindre deux facts entre eux directement sur `stay_id` (fan-out : duplication des lignes). Toujours agréger chaque fact séparément au grain `stay_id` avant de les combiner.

### Vues de pilotage hospitalier

Ce sont des requêtes agrégées sur les facts + dimensions ci-dessus, pas des tables physiques séparées.

**Durée Moyenne de Séjour (DMS) par service**
```sql
SELECT service_label, avg(duree_sejour_h) AS dms
FROM fact_sejours
JOIN dim_service USING (service_code)
GROUP BY service_label;
```

**Activité des urgences : passages par jour**
```sql
SELECT service_code, toDate(admission_ts) AS jour, count() AS number_of_passages
FROM fact_sejours
WHERE service_code = 'URG'
GROUP BY service_code, jour;
```

**Taux de réadmission à 30 jours (qualité des soins)**
```sql
SELECT toStartOfMonth(admission_ts) AS mois,
       avg(readmission_30j) AS readmission_rate
FROM fact_sejours
GROUP BY mois;
```

**Surveillance des constantes : relevés en alerte / jour**
```sql
SELECT
    toDate(ts) AS jour,
    countIf(is_alerte_fc) AS nb_alertes_fc,
    countIf(is_alerte_spo2) AS nb_alertes_spo2,
    countIf(is_alerte_temp) AS nb_alertes_temp,
    countIf(is_alerte_fc OR is_alerte_spo2 OR is_alerte_temp) AS nb_alertes_total
FROM fact_monitoring
GROUP BY jour;
```

**Répartition des admissions par tranche d'âge**
```sql
SELECT age_group, count() AS number_of_admissions
FROM fact_sejours
JOIN dim_patient USING (patient_id)
GROUP BY age_group
ORDER BY age_group;
```

**Toute autre vue d'activité pertinente**
