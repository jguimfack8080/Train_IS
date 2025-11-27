from datetime import datetime
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Ensure include path is available
INCLUDE_PATH = "/opt/airflow/include"
if INCLUDE_PATH not in sys.path:
    sys.path.append(INCLUDE_PATH)

from utils.api_client import fetch_db_stations_all
from utils.db import (
    insert_json,
    log_api_call,
    copy_stations_stg_to_psa,
    purge_stg_stations,
    log_process_start,
    log_process_end,
)
import uuid


DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
}


def _fetch_and_store_stations(**context):
    limit = 100  # sinnvolle Seitengröße
    start_ts = datetime.utcnow()
    batch_id = f"db_stations_{start_ts.strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
    process_id = log_process_start(process_name="db_stations_import", batch_id=batch_id)
    data, status_code, elapsed_ms, meta = fetch_db_stations_all(limit=limit, searchstring="*")

    # Insert raw payload into STG
    insert_json(table="stg.db_stations_raw", payload=data, batch_id=batch_id)

    # Log API call in METADATA
    log_api_call(
        source="deutsche_bahn",
        endpoint="station-data/v2/stations",
        params={
            "limit": limit,
            "searchstring": "*",
            "pagination": True,
            "page_count": meta.get("page_count"),
            "total": meta.get("total"),
        },
        status_code=status_code,
        response_time_ms=elapsed_ms,
        result_count=(len(data) if isinstance(data, list) else 1),
        called_at=start_ts,
    )

    # XCom: batch_id und process_id an Folgetasks übergeben
    context["ti"].xcom_push(key="batch_id", value=batch_id)
    context["ti"].xcom_push(key="process_id", value=process_id)


def _copy_to_psa(**context):
    # Persistente Kopie STG → PSA (Stationen)
    copy_stations_stg_to_psa()


def _purge_stg(**context):
    # STG nach erfolgreicher Kopie bereinigen
    purge_stg_stations()
    # Prozess-Log erfolgreich abschließen
    ti = context["ti"]
    process_id = ti.xcom_pull(task_ids="fetch_and_store_stations", key="process_id")
    if process_id:
        log_process_end(process_id=process_id, status="success", message="Kopie nach PSA und STG‑Bereinigung abgeschlossen")


with DAG(
    dag_id="db_stations_import",
    default_args=DEFAULT_ARGS,
    description="Importiert Bahnstationen (Rohdaten) in STG und loggt den API-Call",
    schedule_interval="@monthly",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    fetch_and_store = PythonOperator(
        task_id="fetch_and_store_stations",
        python_callable=_fetch_and_store_stations,
        provide_context=True,
    )
    copy_to_psa = PythonOperator(
        task_id="copy_stations_to_psa",
        python_callable=_copy_to_psa,
        provide_context=True,
    )
    purge_stg = PythonOperator(
        task_id="purge_stg_stations",
        python_callable=_purge_stg,
        provide_context=True,
    )

    fetch_and_store >> copy_to_psa >> purge_stg