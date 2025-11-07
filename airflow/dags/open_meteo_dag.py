from datetime import datetime
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

INCLUDE_PATH = "/opt/airflow/include"
if INCLUDE_PATH not in sys.path:
    sys.path.append(INCLUDE_PATH)

from utils.api_client import fetch_open_meteo_forecast
from utils.db import (
    insert_json,
    log_api_call,
    get_bremen_station_coords,
    copy_weather_stg_to_psa,
    purge_stg_weather,
    log_process_start,
    log_process_end,
    get_latest_forecast_batch_id,
    is_batch_processed,
    insert_forecast_verticalized_to_dwh,
)
import uuid


DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
}

HOURLY_PARAMS = (
    "temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,"
    "precipitation_probability,precipitation,rain,showers,snowfall,snow_depth,"
    "weather_code,pressure_msl,surface_pressure,cloud_cover,cloud_cover_low,"
    "cloud_cover_mid,cloud_cover_high,visibility,wind_speed_10m,"
    "evapotranspiration,vapour_pressure_deficit"
)


def _fetch_weather_for_coords(**context):
    coords = get_bremen_station_coords(limit=None)
    # Fallback: Bremen Zentrum (falls keine Koordinaten in DWH vorhanden)
    if not coords:
        coords = [(53.0793, 8.8017)]

    # Batches von bis zu 200 Koordinaten
    max_batch = 200
    batches = [coords[i : i + max_batch] for i in range(0, len(coords), max_batch)]
    start_ts = datetime.utcnow()
    batch_id = f"open_meteo_{start_ts.strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
    process_id = log_process_start(process_name="open_meteo_import", batch_id=batch_id)
    context["ti"].xcom_push(key="batch_id", value=batch_id)
    context["ti"].xcom_push(key="process_id", value=process_id)

    for batch in batches:
        start_ts = datetime.utcnow()
        payload, status_code, elapsed_ms = fetch_open_meteo_forecast(
            coords=batch, hourly_params=HOURLY_PARAMS
        )
        insert_json(table="stg.weather_forecast_raw", payload=payload, batch_id=batch_id)
        log_api_call(
            source="open_meteo",
            endpoint="v1/forecast",
            params={"coords": batch, "hourly": HOURLY_PARAMS},
            status_code=status_code,
            response_time_ms=elapsed_ms,
            result_count=(1 if payload else 0),
            called_at=start_ts,
        )


def _copy_weather_to_psa(**context):
    copy_weather_stg_to_psa()


def _purge_stg_weather(**context):
    purge_stg_weather()
    ti = context["ti"]
    process_id = ti.xcom_pull(task_ids="fetch_open_meteo_weather", key="process_id")
    if process_id:
        log_process_end(process_id=process_id, status="success", message="Kopie nach PSA und STG‑Bereinigung abgeschlossen")

def transform_forecast_task(**context):
    batch_id = get_latest_forecast_batch_id()
    # Idempotence: si déjà traité, consigner et quitter
    if batch_id and is_batch_processed("weather_forecast_transform_dwh", batch_id):
        process_id = log_process_start("weather_forecast_transform_dwh", batch_id)
        log_process_end(process_id, "skipped", f"Batch déjà transformé: {batch_id}")
        return
    process_id = log_process_start("weather_forecast_transform_dwh", batch_id)
    try:
        inserted = insert_forecast_verticalized_to_dwh(batch_id=batch_id)
        msg = f"Vorhersage vertikalisiert: eingefügte Zeilen={inserted} (Batch={batch_id})"
        log_process_end(process_id, "success", msg)
    except Exception as e:
        log_process_end(process_id, "error", f"Fehler: {e}")
        raise


with DAG(
    dag_id="open_meteo_forecast_import",
    default_args=DEFAULT_ARGS,
    description="Importiert Wetterdaten (Roh) in STG, basierend auf Stationskoordinaten",
    schedule_interval="0 */6 * * *",  # toutes les 6 heures: 00,06,12,18
    start_date=days_ago(1),
    catchup=False,
) as dag:

    fetch_weather = PythonOperator(
        task_id="fetch_open_meteo_weather",
        python_callable=_fetch_weather_for_coords,
        provide_context=True,
    )
    copy_to_psa = PythonOperator(
        task_id="copy_weather_to_psa",
        python_callable=_copy_weather_to_psa,
        provide_context=True,
    )
    purge_stg = PythonOperator(
        task_id="purge_stg_weather",
        python_callable=_purge_stg_weather,
        provide_context=True,
    )
    transform_forecast = PythonOperator(
        task_id="transform_forecast_to_dwh",
        python_callable=transform_forecast_task,
        provide_context=True,
    )

    fetch_weather >> copy_to_psa >> purge_stg >> transform_forecast