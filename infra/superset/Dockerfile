FROM apache/superset:latest

USER root
# Driver PostgreSQL installé dans le venv Superset (pas de pip dans le venv, on cible manuellement)
RUN /usr/local/bin/pip install --no-cache-dir \
    --target=/app/.venv/lib/python3.10/site-packages \
    psycopg2-binary

# Config personnalisée
COPY superset_config.py /app/superset_config.py
ENV SUPERSET_CONFIG_PATH=/app/superset_config.py

USER superset
