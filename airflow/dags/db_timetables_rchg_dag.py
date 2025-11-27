from __future__ import annotations

import sys
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure include path is available
INCLUDE_PATH = "/opt/airflow/include"
if INCLUDE_PATH not in sys.path:
    sys.path.append(INCLUDE_PATH)

from utils.timetables import task_ingest_rchg


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="db_timetables_rchg_import",
    schedule_interval="*/10 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Berlin"),
    catchup=False,
    default_args=default_args,
    tags=["deutsche-bahn", "timetables", "rchg"],
) as dag:
    PythonOperator(task_id="import_rchg_xml", python_callable=task_ingest_rchg, provide_context=True)