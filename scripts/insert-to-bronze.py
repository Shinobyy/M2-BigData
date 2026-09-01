import requests
import os
from dotenv import load_dotenv

load_dotenv()

clickhouse_host = os.getenv("CLICKHOUSE_HOST", "localhost")
clickhouse_port = os.getenv("CLICKHOUSE_PORT", "8123")
clickhouse_user = os.getenv("CLICKHOUSE_USER", "default")
clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "")
lake_path = os.getenv("LAKE_PATH", "/lake")

FORMAT_BY_EXT = {
    "csv": "CSV",
    "parquet": "Parquet",
    "json": "JSONEachRow",
}

def list_files_in_directory(directory):
    files = []
    for root, dirs, filenames in os.walk(directory):
        # Ignorer le dossier archive pour ne pas retraiter les fichiers déjà archivés.
        dirs[:] = [d for d in dirs if d != "archive"]
        for filename in filenames:
            files.append(os.path.join(root, filename))
    return files

def log_ingestion(table_name, source_file, status):
    requests.post(
        f"http://{clickhouse_host}:{clickhouse_port}/",
        params={"query": "INSERT INTO bronze._ingestion_log (table_name, source_file, status) FORMAT JSONEachRow"},
        data=f'{{"table_name": "{table_name}", "source_file": "{source_file}", "status": "{status}"}}',
        auth=(clickhouse_user, clickhouse_password),
    )

def insert_file(file_path):
    file_name = os.path.basename(file_path)
    table_name, ext = os.path.splitext(file_name)
    ext = ext.lstrip(".")
    relative_path = os.path.relpath(file_path, lake_path)
    fmt = {"csv": "CSVWithNames", "parquet": "Parquet", "json": "JSONEachRow"}.get(ext)
    if fmt is None:
        print(f"Unknown format for {file_name}, skipping")
        return False

    with open(file_path, "rb") as f:
        response = requests.post(
            f"http://{clickhouse_host}:{clickhouse_port}/",
            params={"query": f"INSERT INTO bronze.{table_name} FORMAT {fmt}"},
            data=f,
            auth=(clickhouse_user, clickhouse_password),
        )
    if response.status_code == 200:
        print(f"Inserted {file_name} into bronze.{table_name}")
        log_ingestion(table_name, relative_path, "success")
        return True
    print(f"Failed on {file_name}: {response.status_code} {response.text}")
    log_ingestion(table_name, relative_path, "failed")
    return False


# passe le fichier de base : source-filestorage/diagnostics/2026-08-27/diagnostics.json
# vers source-filestorage/archive/diagnostics/2026-08-27/diagnostics.json
def archive_file(file_path):
    relative_path = os.path.relpath(file_path, lake_path)
    archive_path = os.path.join(lake_path, "archive", relative_path)
    os.makedirs(os.path.dirname(archive_path), exist_ok=True)
    os.rename(file_path, archive_path)

def clear_bronze_table(table_name):
    response = requests.post(
        f"http://{clickhouse_host}:{clickhouse_port}/",
        params={"query": f"TRUNCATE TABLE bronze.{table_name}"},
        auth=(clickhouse_user, clickhouse_password),
    )
    if response.status_code == 200:
        print(f"Cleared bronze.{table_name}")
    else:
        print(f"Failed to clear bronze.{table_name}: {response.status_code} {response.text}")

BRONZE_TABLES = ["patients", "sejours", "diagnostics", "monitoring", "services", "cim10"]

def clear_bronze():
    for table in BRONZE_TABLES:
        clear_bronze_table(table)

def main():
    print("Starting insertion of files into ClickHouse...")
    for file_path in list_files_in_directory(lake_path):
        if insert_file(file_path):
            archive_file(file_path)
            print(f"Archived {file_path} to archive directory.")

if __name__ == "__main__":
    import sys
    if "--clear" in sys.argv:
        clear_bronze()
    else:
        main()