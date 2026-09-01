FROM python:3.12-slim

# Utilisateur non-root aligné sur l'UID/GID de l'hôte : les fichiers écrits dans
# lake/ (volume monté) restent manipulables côté hôte, et le conteneur ne tourne
# pas en root.
RUN groupadd -g 1000 pipeline && useradd -u 1000 -g 1000 -m pipeline

# Sans ça, Python bufferise stdout hors TTY : les logs de main.py (dont les
# messages d'échec) n'apparaîtraient pas dans `docker compose logs`.
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py ./
# Les transformations Bronze->Silver->Gold sont du SQL, pas du Python : elles
# vivent dans des fichiers .sql chargés à l'exécution.
COPY sql ./sql

USER pipeline

CMD ["python", "main.py"]
