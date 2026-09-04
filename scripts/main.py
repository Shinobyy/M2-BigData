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

# Nombre d'échecs consécutifs toléré avant d'abandonner. Sortir en erreur est
# le SEUL moyen de rendre une panne visible de l'extérieur : tant que le
# processus vit, `docker compose ps` affiche « Up » et rien ne distingue un
# pipeline sain d'un pipeline cassé qui boucle dans le vide.
MAX_CONSECUTIVE_FAILURES = int(os.getenv("MAX_CONSECUTIVE_FAILURES", "5"))

# Backoff exponentiel après un échec. Attendre l'intervalle nominal (un jour)
# serait absurde pour une panne transitoire, et réessayer toutes les 30 s
# martèlerait ClickHouse. 5, 10, 20 puis 40 min : une indisponibilité passagère
# (redémarrage de ClickHouse, disque saturé puis libéré) a le temps de se
# résorber avant que le pipeline n'abandonne.
# Le plafond ne mord que si MAX_CONSECUTIVE_FAILURES est relevé.
BACKOFF_BASE_SECONDS = 300
BACKOFF_MAX_SECONDS = 3600


def run_forever(interval_seconds):
    """Boucle jusqu'à MAX_CONSECUTIVE_FAILURES échecs d'affilée, puis rend False.

    Boucle séquentielle : le cycle suivant ne démarre qu'une fois le précédent
    terminé, donc aucun chevauchement possible (contrairement à cron, où un
    cycle qui déborde de l'intervalle en croiserait un autre).
    """
    log(f"Loop mode: un cycle toutes les {interval_seconds}s "
        f"(abandon après {MAX_CONSECUTIVE_FAILURES} échecs consécutifs)")
    failures = 0

    while True:
        started_at = time.monotonic()
        succeeded = run_pipeline()

        if succeeded:
            if failures:
                log(f"Rétabli après {failures} échec(s) consécutif(s)")
            failures = 0
            # Délai mesuré depuis le DÉBUT du cycle : sinon la période réelle
            # vaut durée + intervalle, et dérive avec la charge.
            delay = max(0, interval_seconds - (time.monotonic() - started_at))
        else:
            failures += 1
            if failures >= MAX_CONSECUTIVE_FAILURES:
                log(f"FATAL {failures} échecs consécutifs, arrêt du processus")
                return False
            delay = min(BACKOFF_BASE_SECONDS * 2 ** (failures - 1),
                        BACKOFF_MAX_SECONDS)
            log(f"Échec {failures}/{MAX_CONSECUTIVE_FAILURES}, "
                f"nouvelle tentative dans {delay:.0f}s")

        time.sleep(delay)


if __name__ == "__main__":
    # Mode boucle (conteneur) : LOOP_INTERVAL défini, ou --loop N.
    # Sinon un seul cycle, et code de sortie non nul en cas d'échec pour que
    # cron ou un superviseur détecte l'incident sans parser les logs.
    interval = os.getenv("LOOP_INTERVAL")
    if "--loop" in sys.argv:
        interval = sys.argv[sys.argv.index("--loop") + 1]
    if interval:
        # run_forever ne rend la main qu'en abandonnant : le code de sortie non
        # nul fait tomber le conteneur, ce que `restart: on-failure` et la
        # supervision peuvent voir. Sans ça, une panne durable reste invisible.
        sys.exit(0 if run_forever(int(interval)) else 1)
    else:
        sys.exit(0 if run_pipeline() else 1)
