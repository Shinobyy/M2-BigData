# Entrepôt de données de santé (EDS) — CHU

Chaîne complète de collecte, pseudonymisation, modélisation et restitution des données
d'un CHU, sur une architecture médaillon (Bronze → Silver → Gold) portée par ClickHouse,
restituée dans Metabase avec deux rôles aux périmètres disjoints.

Le CHU dépose ses fichiers chaque jour dans `source-filestorage/`, arborescence
`<type>/<AAAA-MM-JJ>/<fichier>`. Le dépôt est monté en lecture seule : le pipeline ne
peut pas le modifier.

---

## Démarrage

```bash
cp scripts/.env.example scripts/.env    # renseigner PSEUDO_SALT et METABASE_API_KEY
docker compose up -d clickhouse metabase postgres

# Le DDL n'est câblé à aucun script : à jouer une fois, à la main.
for f in ddl/bronze.sql ddl/silver.sql ddl/gold.sql; do
  docker compose exec -T clickhouse clickhouse-client --multiquery < "$f"
done

docker compose up -d pipeline
docker compose run --rm --no-deps -e METABASE_URL=http://metabase:3000 \
  pipeline python provision-metabase.py
```

Metabase sur <http://localhost:3000>, ClickHouse sur `:8123` (HTTP) et `:9000` (natif).

> **Toute modification d'un `.py` ou d'un `.sql` exige un `docker compose build pipeline`.**
> Le [Dockerfile](scripts/Dockerfile) fait `COPY *.py` et `COPY sql`, et `compose.yaml` ne
> monte pas `scripts/` : le code est figé dans l'image au build. C'est la cause d'erreur
> la plus fréquente sur ce projet — un correctif qui « ne prend pas » vient presque
> toujours de là.

---

## Architecture

| diagramme | objet |
|---|---|
| [architecture.puml](diagrams/architecture.puml) | composants, conteneurs, flux d'exécution |
| [lineage.puml](diagrams/lineage.puml) | lignage des tables sur les trois couches |
| [bronze](diagrams/bronze.puml) · [silver](diagrams/silver.puml) · [gold](diagrams/gold.puml) | modèle détaillé de chaque couche |
| [silver-vs-gold.puml](diagrams/silver-vs-gold.puml) | contraste grain fin ↔ indicateur |

Les `.png` et `.svg` sont générés depuis les `.puml` :

```bash
plantuml -tsvg -tpng diagrams/*.puml
```

### Le pipeline

`main.py` enchaîne quatre étapes, séquentiellement, un cycle par jour :

| étape | rôle |
|---|---|
| [sync-to-lake.py](scripts/sync-to-lake.py) | copie `source-filestorage/` → `lake/` **en pseudonymisant** |
| [insert-to-bronze.py](scripts/insert-to-bronze.py) | `lake/` → tables Bronze, puis archivage du fichier |
| [insert-to-silver.py](scripts/insert-to-silver.py) | Bronze → Silver : contrôles qualité, déduplication |
| [insert-to-gold.py](scripts/insert-to-gold.py) | Silver → Gold : agrégation en indicateurs |

Toutes les transformations Bronze → Silver → Gold sont des `INSERT ... SELECT` exécutés
par ClickHouse. Python n'est qu'un pilote : il copie des fichiers et poste des requêtes
HTTP. Le SQL vit dans [scripts/sql/](scripts/sql/), un fichier par table cible, si bien
que chaque requête est copiable telle quelle dans une console pour être déboguée.

La seule exception est `sync-to-lake.py`, qui réécrit les CSV ligne à ligne — c'est là
qu'on retire `nir`/`nom`/`prenom` et qu'on remplace `patient_id` par
`SHA256(id + sel)[:16]`.

---

## Les couches

### Bronze — miroir des fichiers déposés

`MergeTree`, append-only, aucune transformation. Une table par fichier source, nommée
d'après lui : `actes.parquet` → `bronze.actes`.

`patients` · `sejours` · `diagnostics` · `monitoring` · `actes` · `services` ·
`description_service` · `cim10` · `ccam`

Plus `_ingestion_log` : une ligne par fichier ingéré (table cible, chemin source,
horodatage, statut).

La structure imbriquée du JSON est conservée telle quelle dans
`Array(Tuple(code_cim10 String, type String))` — l'aplatissement a lieu en Silver.

### Silver — étoile au grain fin, nettoyée et contrôlée

**Dimensions** — `dim_patient` · `dim_service` · `dim_cim10` · `dim_ccam`

`dim_service` porte une hiérarchie à trois niveaux d'agrégation croissants
(`service_label` → `categorie` → `pole`) ainsi que `capacite_lits`. Elle est conformée
depuis **deux** fichiers sources (`services.csv` et `description_service.csv`) par une
`LEFT JOIN` : un service décrit dans l'un mais pas dans l'autre doit rester dans la
dimension, faute de quoi le contrôle d'intégrité de `fact_sejours` rejetterait tous ses
séjours en silence.

**Faits** — `fact_sejours` · `fact_monitoring` · `fact_diagnostics` · `fact_actes`

`stay_id` est une **dimension dégénérée** partagée par les quatre faits, jamais une clé
étrangère d'un fait vers un autre. `patient_id` et `service_code` sont dénormalisés sur
chaque fait pour qu'aucun n'ait besoin d'en interroger un autre.

> ⚠️ Ne jamais joindre deux faits entre eux sur `stay_id` : le fan-out duplique les
> lignes. Agréger chaque fait séparément à son grain avant de combiner.

**Tables techniques** — `_watermarks` (reprise incrémentale) · `_rejets` (toute ligne
écartée, avec la règle et les valeurs en cause)

### Gold — une table par indicateur

Les dashboards **lisent**, ils n'agrègent pas. Chaque table contient le KPI déjà calculé
à son grain d'analyse.

**Pilotage hospitalier** (`pilotage_user`)

| table | grain |
|---|---|
| `dms_par_service` | service × mois |
| `activite_dms_par_categorie_service` | catégorie × mois |
| `urgences_par_jour` | jour |
| `readmission_par_mois` | mois |
| `alertes_par_jour` | jour |
| `admissions_par_age` | tranche d'âge |
| `actes_par_service` | service × mois |
| `actes_par_code_ccam` | code CCAM × mois |
| `densite_actes_par_lits` | service × mois |
| `montant_facture_par_service` | service × mois |

**Recherche clinique** (`recherche_user`)

| table | grain |
|---|---|
| `prevalence_pathologie` | code CIM-10 |
| `cohorte_age_sexe` | tranche d'âge × sexe |

La définition, le grain et la justification de chaque indicateur — ainsi que les
décisions de calcul qui en déterminent la valeur — sont documentés dans le dossier de
rendu, section « Indicateurs ».

---

## Incrémentalité

Trois mécanismes distincts, parce que les trois étages n'ont pas les mêmes contraintes :

| étage | mécanisme |
|---|---|
| Filestorage → Lake | watermark par type de dépôt, dans `state/last_ingested.log` |
| Lake → Bronze | archivage du fichier après succès, dans `lake/archive/` |
| Bronze → Silver | watermark + `LIMIT 1 BY` + anti-jointure |
| Silver → Gold | recalcul complet : `TRUNCATE` puis réécriture intégrale |

Gold n'a pas de watermark : un agrégat mensuel change dès qu'un jour de ce mois arrive.
Le `TRUNCATE` n'est pas une facilité — `ReplacingMergeTree` écrase par clé mais ne
supprime pas, donc une clé disparue de Silver resterait affichée avec ses anciens
chiffres.

`dim_service` fait exception et ignore volontairement le watermark : ses deux fichiers
sources n'arrivent pas ensemble, et un watermark assis sur `bronze.services` bloquerait
à jamais l'enrichissement venu de `description_service.csv`.

---

## Cloisonnement

Deux comptes ClickHouse aux périmètres strictement disjoints, définis dans
[ddl/gold.sql](ddl/gold.sql). Aucun des deux n'a de droit sur `silver` ni sur `bronze` :
les dashboards ne voient que des agrégats, jamais le grain patient.

```bash
# doit répondre « Not enough privileges »
curl -s -u pilotage_user:pilotage_pwd http://localhost:8123/ \
  --data-binary "SELECT count() FROM silver.fact_sejours"
```

Le réglage `SETTINGS final = 1` applique `FINAL` d'office à la lecture, pour que Metabase
ne voie jamais un doublon transitoire d'un `ReplacingMergeTree`.

---

## Exploitation

### Ordonnancement

Un cycle par jour (`LOOP_INTERVAL: 86400`), en Python pur, sans cron ni orchestrateur.
La boucle sait échouer : après cinq cycles ratés d'affilée le processus sort en code 1,
et `restart: on-failure:3` laisse le conteneur en `Exited (1)`. Entre deux tentatives le
délai double : 5, 10, 20 puis 40 minutes.

Cette forme garantit par construction qu'aucun cycle n'en chevauche un autre, que Gold
n'est jamais calculé sur un Silver incomplet, et que la reprise après interruption est
gratuite — les watermarks vivant hors du conteneur. Un orchestrateur type Airflow
apporterait la reprise par tâche et le backfill ; pour quatre étapes strictement
séquentielles, ce serait ajouter un scheduler, une base de métadonnées et un serveur web
pour faire ce qu'une boucle fait déjà.

### Rejouer une étape

```bash
docker compose run --rm --no-deps pipeline python insert-to-gold.py
```

### Reconstruction complète

⚠️ **Les cinq éléments doivent tomber ensemble.** Oublier Bronze suffit à tout
invalider : Silver le relit et en reproduit fidèlement les doublons.

```bash
docker compose stop pipeline
CH() { curl -s -u default:clickhouse "http://localhost:8123/" --data-binary "$1"; }

for t in patients sejours diagnostics monitoring actes services description_service cim10 ccam; do
  CH "TRUNCATE TABLE bronze.$t"
done
for t in dim_patient dim_service dim_cim10 dim_ccam fact_sejours fact_monitoring \
         fact_diagnostics fact_actes _watermarks _rejets; do
  CH "TRUNCATE TABLE silver.$t"
done

rm -f state/last_ingested.log     # watermark de synchronisation
rm -rf lake/archive               # sinon rien n'est recopié

docker compose run --rm --no-deps pipeline python main.py   # UN seul cycle
```

Gold n'est pas dans la liste : `insert-to-gold.py` la tronque de lui-même à chaque
passage.

---

## Limites connues

- **`silver.fact_monitoring` est un `MergeTree` nu.** C'est la seule table du projet où
  rejouer une ingestion duplique les lignes ; les trois autres faits sont en
  `ReplacingMergeTree` sur leur grain et absorbent le cas. La clé de tri `(stay_id, ts)`
  étant déjà le grain, la bascule fermerait le sujet.
- **Le watermark de synchronisation est une date maximale par type.** Un dépôt daté
  avant ce maximum est ignoré, avec un avertissement mais sans reprise possible.
- **Le seuil RGPD des petits effectifs est désactivé sur `prevalence_pathologie`.** Le
  `HAVING taille_cohorte >= 5` est commenté : deux cohortes sous le seuil sont
  matérialisées. À réactiver avant toute mise à disposition réelle.
- **`description_service.csv` est incomplet** : `NEURO` n'y figure pas, d'où une catégorie
  vide et une densité d'actes nulle pour ce service.
- **Les contrôles référentiels des faits ne suivent pas la même règle** :
  `fact_diagnostics` contrôle son code contre `silver.dim_cim10`, `fact_actes` contre
  `bronze.ccam`. Les deux fonctionnent, mais l'asymétrie mérite d'être tranchée.
- **Le pipeline n'a pas de supervision active.** L'échec est désormais visible
  (`Exited (1)`), mais rien n'alerte personne. Une sonde sur la fraîcheur de
  `bronze._ingestion_log` couvrirait l'essentiel du risque.

---

## Arborescence

```
FICHE-SUJET.pdf         sujet initial
SUJET-EVOLUTION-*.pdf   sujet d'évolution (actes, CCAM, hiérarchie de services)
ddl/                    DDL des trois couches (à jouer à la main)
diagrams/               6 diagrammes PlantUML + rendus
lake/                   copie de travail pseudonymisée (+ archive/)
scripts/
  main.py               orchestrateur
  sync-to-lake.py       pseudonymisation
  insert-to-*.py        pilotes Bronze / Silver / Gold
  provision-metabase.py cartes et connexions, via l'API REST
  sql/silver/           un fichier par table cible (+ .rejects.sql)
  sql/gold/             un fichier par indicateur
source-filestorage/     dépôt du CHU, monté en lecture seule
state/                  watermark de synchronisation
```
