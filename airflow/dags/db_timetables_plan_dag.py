from __future__ import annotations

import sys
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure include path is available
INCLUDE_PATH = "/opt/airflow/include"
if INCLUDE_PATH not in sys.path:
    sys.path.append(INCLUDE_PATH)

from utils.timetables import ingest_plan
from utils.date_utils import to_tz


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
}


def run_plan_window(**context):
    """Importe les horaires planifiés pour:
    - toutes les heures du jour courant (00–23)
    - et les premières heures du lendemain (00–05)

    La détermination se base sur la `logical_date` Airflow (Europe/Berlin).
    """
    logical_date = context["logical_date"]
    berlin_dt = to_tz(logical_date)

    rows_total = 0
    api_success_total = 0
    stations_count = None

    # Heures du jour courant (00–23)
    for hour in range(0, 24):
        dt_hour = berlin_dt.replace(hour=hour, minute=0, second=0, microsecond=0)
        metrics = ingest_plan(dt_hour)
        if not isinstance(metrics, dict):
            metrics = {}
        rows_total += (metrics.get("rows_inserted") or 0)
        api_success_total += (metrics.get("api_success") or 0)
        stations_count = metrics.get("stations", stations_count)

    # Heures du lendemain (00–05)
    next_day = berlin_dt.add(days=1)
    for hour in range(0, 6):
        dt_hour = next_day.replace(hour=hour, minute=0, second=0, microsecond=0)
        metrics = ingest_plan(dt_hour)
        if not isinstance(metrics, dict):
            metrics = {}
        rows_total += (metrics.get("rows_inserted") or 0)
        api_success_total += (metrics.get("api_success") or 0)
        stations_count = metrics.get("stations", stations_count)

    return {
        "rows_inserted_total": rows_total,
        "api_success_total": api_success_total,
        "stations": stations_count,
    }


with DAG(
    dag_id="db_timetables_plan_import",
    schedule_interval="0 1 * * *",  # Tous les jours à 01:00
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/Berlin"),
    catchup=False,
    default_args=default_args,
    tags=["deutsche-bahn", "timetables", "plan"],
) as dag:
    PythonOperator(task_id="import_plan_day_and_next_morning", python_callable=run_plan_window, provide_context=True)