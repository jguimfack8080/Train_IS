FROM apache/airflow:2.9.3

# Python‑Abhängigkeiten als Benutzer 'airflow' installieren (empfohlen)
USER airflow
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt