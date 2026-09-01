import os
import subprocess
import sys
import time
from datetime import datetime

PIPELINE = [
    "sync-to-lake.py",
    "insert-to-bronze.py",
    "insert-to-silver.py",
    "insert-to-gold.py",
]

def log(message):
    print(f"[{datetime.now().isoformat(timespec='seconds')}] {message}")

def run_step(script_name):
    log(f"Starting {script_name}")
    result = subprocess.run([sys.executable, script_name])
    if result.returncode != 0:
        log(f"FAILED {script_name} (exit code {result.returncode})")
        return False
    log(f"Done {script_name}")
    return True

def run_pipeline():
    for script_name in PIPELINE:
        if not run_step(script_name):
            log("Stopping pipeline early due to failure")
            return False
    log("Pipeline complete")
    return True

def run_forever(interval_seconds):
    # Boucle séquentielle : le cycle suivant ne démarre qu'une fois le précédent
    # terminé, donc aucun chevauchement possible (contrairement à cron, où un
    # cycle qui déborde de l'intervalle en croiserait un autre).
    log(f"Loop mode: un cycle toutes les {interval_seconds}s")
    while True:
        run_pipeline()
        time.sleep(interval_seconds)


if __name__ == "__main__":
    # Mode boucle (conteneur) : LOOP_INTERVAL défini, ou --loop N.
    # Sinon un seul cycle, et code de sortie non nul en cas d'échec pour que
    # cron ou un superviseur détecte l'incident sans parser les logs.
    interval = os.getenv("LOOP_INTERVAL")
    if "--loop" in sys.argv:
        interval = sys.argv[sys.argv.index("--loop") + 1]
    if interval:
        run_forever(int(interval))
    else:
        sys.exit(0 if run_pipeline() else 1)
