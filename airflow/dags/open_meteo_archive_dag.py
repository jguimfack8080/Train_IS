from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Ensure include path is available
INCLUDE_PATH = "/opt/airflow/include"
if INCLUDE_PATH not in sys.path:
    sys.path.append(INCLUDE_PATH)

from utils.api_client import fetch_open_meteo_archive
from utils.db import (
    insert_json,
    log_api_call,
    get_bremen_station_coords,
    copy_weather_archive_stg_to_psa,
    purge_stg_weather_archive,
    log_process_start,
    log_process_end,
    get_latest_history_batch_id,
    is_batch_processed,
    insert_history_verticalized_to_dwh,
    get_last_archive_end_date,
)
import uuid


DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
}


def _fetch_and_store_weather_archive(**context):
    # Fin standard: J-2 (Open‑Meteo publie l’historique avec 1–2 jours de lag)
    today = datetime.utcnow().date()
    end_date = (today - timedelta(days=2)).isoformat()

    # Fenêtre roulante:
    # - Premier run: start = ARCHIVE_START_DATE (ex: 2025‑01‑01)
    # - Runs suivants: start = dernier end_date utilisé
    last_end = get_last_archive_end_date()
    start_date = last_end if last_end else os.getenv("ARCHIVE_START_DATE", "2025-01-01")

    # Stundenparameter (über Env variabel)
    hourly_params = os.getenv(
        "OPEN_METEO_ARCHIVE_HOURLY",
        "temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,"
        "precipitation,rain,snowfall,snow_depth,weather_code,pressure_msl,surface_pressure,"
        "cloud_cover,cloud_cover_low,cloud_cover_mid,cloud_cover_high,wind_speed_10m",
    )

    # Stationskoordinaten aus DWH (nur Bremen)
    coords = get_bremen_station_coords()
    if not coords:
        # Keine Koordinaten; Logging und sauber beenden
        log_api_call(
            source="open_meteo",
            endpoint="archive",
            params={
                "reason": "no_station_coords",
                "start_date": start_date,
                "end_date": end_date,
            },
            status_code=204,
            response_time_ms=0,
            result_count=0,
            called_at=datetime.utcnow(),
        )
        return

    start_ts = datetime.utcnow()
    batch_id = f"open_meteo_archive_{start_ts.strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
    process_id = log_process_start(process_name="open_meteo_archive_import", batch_id=batch_id)
    # Appel API avec uniquement les coordonnées de Brême issues de get_bremen_station_coords()
    data, status_code, elapsed_ms = fetch_open_meteo_archive(coords, start_date, end_date, hourly_params)

    # Rohpayload in STG speichern
    insert_json(table="stg.weather_history_raw", payload=data, batch_id=batch_id)

    # API‑Call in METADATA protokollieren
    # Calcul précis du nombre de résultats (liste directe ou sous clé 'results')
    if isinstance(data, list):
        result_count = len(data)
    elif isinstance(data, dict) and isinstance(data.get("results"), list):
        result_count = len(data.get("results"))
    else:
        result_count = 1

    log_api_call(
        source="open_meteo",
        endpoint="archive",
        params={
            "start_date": start_date,
            "end_date": end_date,
            "hourly": hourly_params,
            "station_count": len(coords),
            # Journaliser les coordonnées utilisées, comme pour le forecast
            "latitude": ",".join([str(round(c[0], 6)) for c in coords]),
            "longitude": ",".join([str(round(c[1], 6)) for c in coords]),
        },
        status_code=status_code,
        response_time_ms=elapsed_ms,
        result_count=result_count,
        called_at=start_ts,
    )
    # XCom
    context["ti"].xcom_push(key="batch_id", value=batch_id)
    context["ti"].xcom_push(key="process_id", value=process_id)


def _copy_archive_to_psa(**context):
    copy_weather_archive_stg_to_psa()


def _purge_stg_archive(**context):
    purge_stg_weather_archive()
    ti = context["ti"]
    process_id = ti.xcom_pull(task_ids="fetch_and_store_weather_archive", key="process_id")
    if process_id:
        log_process_end(process_id=process_id, status="success", message="Kopie nach PSA und STG‑Bereinigung abgeschlossen")

def transform_history_task(**context):
    batch_id = get_latest_history_batch_id()
    # Idempotence: si déjà traité, consigner et quitter
    if batch_id and is_batch_processed("weather_history_transform_dwh", batch_id):
        process_id = log_process_start("weather_history_transform_dwh", batch_id)
        log_process_end(process_id, "skipped", f"Batch déjà transformé: {batch_id}")
        return
    process_id = log_process_start("weather_history_transform_dwh", batch_id)
    try:
        inserted = insert_history_verticalized_to_dwh(batch_id=batch_id)
        msg = f"Archiv vertikalisiert: eingefügte Zeilen={inserted} (Batch={batch_id})"
        log_process_end(process_id, "success", msg)
    except Exception as e:
        log_process_end(process_id, "error", f"Fehler: {e}")
        raise


with DAG(
    dag_id="open_meteo_archive_import",
    default_args=DEFAULT_ARGS,
    description="Importiert historische Wetterdaten und loggt den API‑Call",
    schedule_interval="0 3 * * *",  # täglich um 03:00, Fenster J-2
    start_date=days_ago(1),
    catchup=False,
) as dag:

    fetch_and_store_archive = PythonOperator(
        task_id="fetch_and_store_weather_archive",
        python_callable=_fetch_and_store_weather_archive,
        provide_context=True,
    )
    copy_to_psa = PythonOperator(
        task_id="copy_weather_archive_to_psa",
        python_callable=_copy_archive_to_psa,
        provide_context=True,
    )
    purge_stg = PythonOperator(
        task_id="purge_stg_weather_archive",
        python_callable=_purge_stg_archive,
        provide_context=True,
    )
    transform_history = PythonOperator(
        task_id="transform_history_to_dwh",
        python_callable=transform_history_task,
        provide_context=True,
    )

    fetch_and_store_archive >> copy_to_psa >> purge_stg >> transform_history