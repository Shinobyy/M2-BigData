import csv
import hashlib
import io
import os
import re
import shutil
from dotenv import load_dotenv

load_dotenv()

SOURCE_PATH = os.getenv("SOURCE_PATH", "../source-filestorage")
LAKE_PATH = os.getenv("LAKE_PATH", "../lake")
STATE_PATH = os.getenv("STATE_PATH", "../state")
PSEUDO_SALT = os.getenv("PSEUDO_SALT")

# Watermark par type de dépôt : dernière date de dossier traitée. Évite de
# reparcourir et de recopier les dépôts déjà ingérés, indépendamment du contenu
# de lake/ (qui peut être purgé pour cause de rétention).
LAST_INGESTED_FILE = os.path.join(STATE_PATH, "last_ingested.log")
LAST_INGESTED_LINE = re.compile(r"^\[(\d{4}-\d{2}-\d{2})\]\s+(\S+)\s*$")
DATE_DIR = re.compile(r"^\d{4}-\d{2}-\d{2}$")

if not PSEUDO_SALT:
    raise RuntimeError("PSEUDO_SALT must be set in .env (secret salt for patient_id hashing)")

# Fichiers contenant du patient_id et/ou des identifiants directs : transformés
# en flux (streaming, pas pandas) au moment de la copie vers le lake.
# Les autres fichiers (diagnostics.json, monitoring.parquet, referentiels/*.csv)
# n'ont pas de donnée identifiante : copie brute telle quelle.
PII_FILES = {"patients.csv", "sejours.csv"}


def pseudonymize(patient_id):
    return hashlib.sha256(f"{patient_id}{PSEUDO_SALT}".encode()).hexdigest()[:16]


def sync_patients_csv(src_path, dest_path):
    with open(src_path, "r", newline="") as src:
        reader = csv.DictReader(src)
        # Seules ces colonnes passent au lake : nir/nom/prenom sont des
        # identifiants directs, ils ne doivent jamais quitter le filestorage source.
        fieldnames = ["patient_id", "birth_date", "sex", "region_code"]
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=fieldnames)
        writer.writeheader()
        for row in reader:
            writer.writerow({
                "patient_id": pseudonymize(row["patient_id"]),
                "birth_date": row["birth_date"],
                "sex": row["sex"],
                "region_code": row["region_code"],
            })
    with open(dest_path, "w", newline="") as dest:
        dest.write(buffer.getvalue())


def sync_sejours_csv(src_path, dest_path):
    with open(src_path, "r", newline="") as src:
        reader = csv.DictReader(src)
        fieldnames = reader.fieldnames
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=fieldnames)
        writer.writeheader()
        for row in reader:
            row["patient_id"] = pseudonymize(row["patient_id"])
            writer.writerow(row)
    with open(dest_path, "w", newline="") as dest:
        dest.write(buffer.getvalue())


def sync_file(src_path, dest_path):
    file_name = os.path.basename(src_path)
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    if file_name == "patients.csv":
        sync_patients_csv(src_path, dest_path)
    elif file_name == "sejours.csv":
        sync_sejours_csv(src_path, dest_path)
    else:
        # monitoring.parquet est volumineux : copie d'octets, jamais parsé en Python.
        shutil.copy2(src_path, dest_path)


def list_source_files(directory):
    files = []
    for root, dirs, filenames in os.walk(directory):
        dirs[:] = [d for d in dirs if d != "archive"]
        for filename in filenames:
            files.append(os.path.join(root, filename))
    return files


def read_last_ingested():
    if not os.path.exists(LAST_INGESTED_FILE):
        return {}
    watermarks = {}
    with open(LAST_INGESTED_FILE) as f:
        for line in f:
            match = LAST_INGESTED_LINE.match(line)
            if match:
                watermarks[match.group(2)] = match.group(1)
    return watermarks


def write_last_ingested(watermarks):
    os.makedirs(STATE_PATH, exist_ok=True)
    lines = [f"[{date}]   {type_name}" for type_name, date in sorted(watermarks.items())]
    with open(LAST_INGESTED_FILE, "w") as f:
        f.write("\n".join(lines) + "\n")


def split_relative_path(relative_path):
    """<type>/<AAAA-MM-JJ>/<fichier> -> (type, date). (None, None) si la forme diffère."""
    parts = relative_path.split(os.sep)
    if len(parts) >= 3 and DATE_DIR.match(parts[1]):
        return parts[0], parts[1]
    return None, None


def main():
    print("Syncing source-filestorage (read-only) -> lake ...")
    watermarks = read_last_ingested()
    synced = 0
    seen_dates = {}

    for src_path in sorted(list_source_files(SOURCE_PATH)):
        relative_path = os.path.relpath(src_path, SOURCE_PATH)
        type_name, file_date = split_relative_path(relative_path)

        if type_name is None:
            print(f"WARNING {relative_path} : chemin hors format <type>/<date>/<fichier>, ignoré")
            continue

        dest_path = os.path.join(LAKE_PATH, relative_path)
        archived_path = os.path.join(LAKE_PATH, "archive", relative_path)

        # La décision de traiter ou non repose uniquement sur le watermark : un
        # dossier de date déjà ingéré ne l'est jamais deux fois, même si lake/ a
        # été purgé entre-temps. Le disque ne sert qu'à savoir s'il faut alerter.
        last_date = watermarks.get(type_name)
        if last_date and file_date <= last_date:
            already_in_lake = os.path.exists(dest_path) or os.path.exists(archived_path)
            if not already_in_lake:
                # Fichier jamais passé par le lake alors que sa date est déjà
                # dépassée : dépôt tardif, ou lake/ purgé sans remise à zéro du
                # watermark. Dans les deux cas il est ignoré, mais pas en silence.
                print(
                    f"WARNING {relative_path} ignoré : date {file_date} déjà traitée "
                    f"pour '{type_name}' (watermark {last_date}) et fichier absent du lake"
                )
            continue

        sync_file(src_path, dest_path)
        print(f"Synced {relative_path}")
        synced += 1
        seen_dates[type_name] = max(seen_dates.get(type_name, ""), file_date)

    # Mise à jour en fin de passe seulement : avancer le watermark en cours de
    # route ferait sauter les fichiers suivants du même dossier de date
    # (referentiels/ en contient deux).
    if seen_dates:
        for type_name, file_date in seen_dates.items():
            watermarks[type_name] = max(watermarks.get(type_name, ""), file_date)
        write_last_ingested(watermarks)
        print(f"Watermarks mis à jour : {', '.join(f'{t}={d}' for t, d in sorted(seen_dates.items()))}")

    print(f"Sync complete: {synced} new file(s)")


if __name__ == "__main__":
    main()
