FROM apache/airflow:2.10.4

# pip doit tourner en user airflow (pas root) — doc officielle Airflow Docker
COPY --chown=airflow:root requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
