import json
import psycopg2.extras
from .db_conn import get_connection
from .db_batch import _get_latest_batch_id


def _fetch_psa_entries(table: str, batch_id: str) -> list[tuple[int, dict]]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT id, payload FROM {table} WHERE batch_id = %s",
                (batch_id,),
            )
            rows = cur.fetchall()
            return [(int(r[0]), r[1]) for r in rows]


def _load_stations_for_mapping() -> list[tuple[str, float, float]]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT station_id, latitude, longitude
                FROM dwh.v_stations
                WHERE latitude IS NOT NULL
                  AND longitude IS NOT NULL
                  AND eva_number IS NOT NULL
                  AND name LIKE 'Brem%'
                  AND REPLACE(ds100, '"', '') LIKE 'HB%'
                """
            )
            return [(str(sid), float(lat), float(lon)) for sid, lat, lon in cur.fetchall()]


def _match_station_id(lat: float | None, lon: float | None, stations: list[tuple[str, float, float]]) -> str | None:
    if lat is None or lon is None or not stations:
        return None
    tol = 1e-4
    for sid, s_lat, s_lon in stations:
        if abs(lat - s_lat) < tol and abs(lon - s_lon) < tol:
            return sid
    best_sid = None
    best_d = None
    for sid, s_lat, s_lon in stations:
        d = (lat - s_lat) ** 2 + (lon - s_lon) ** 2
        if best_d is None or d < best_d:
            best_d = d
            best_sid = sid
    return best_sid


def insert_forecast_verticalized_to_dwh(batch_id: str | None = None) -> int:
    table = "psa.weather_forecast_raw"
    if batch_id is None:
        batch_id = _get_latest_batch_id(table)
    if batch_id is None:
        return 0

    stations = _load_stations_for_mapping()
    entries = _fetch_psa_entries(table, batch_id)

    rows_to_insert = []
    for psa_id, payload in entries:
        try:
            lat = float(payload.get("latitude")) if payload.get("latitude") is not None else None
            lon = float(payload.get("longitude")) if payload.get("longitude") is not None else None
            station_id = _match_station_id(lat, lon, stations)
            hourly = payload.get("hourly")
            if not isinstance(hourly, dict):
                continue
            times = hourly.get("time", [])
            n = len(times)
            for i in range(n):
                payload_row = {
                    "psa_id": psa_id,
                    "station_id": station_id,
                    "batch_id": batch_id,
                    "latitude": lat,
                    "longitude": lon,
                    "time": times[i],
                }
                for key, arr in hourly.items():
                    if key == "time":
                        continue
                    if isinstance(arr, list) and i < len(arr):
                        val = arr[i]
                        if val is not None:
                            payload_row[key] = val
                rows_to_insert.append((json.dumps(payload_row), batch_id))
        except Exception:
            continue

    if not rows_to_insert:
        return 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO psa.weather_forecast_vertical_raw (payload, batch_id) VALUES %s",
                rows_to_insert,
            )
    return len(rows_to_insert)


def insert_history_verticalized_to_dwh(batch_id: str | None = None) -> int:
    table = "psa.weather_history_raw"
    if batch_id is None:
        batch_id = _get_latest_batch_id(table)
    if batch_id is None:
        return 0

    stations = _load_stations_for_mapping()
    entries = _fetch_psa_entries(table, batch_id)

    rows_to_insert = []
    for psa_id, payload in entries:
        try:
            if isinstance(payload, list):
                objs = payload
            elif isinstance(payload, dict) and isinstance(payload.get("results"), list):
                objs = payload.get("results")
            else:
                objs = [payload]
            for obj in objs:
                lat = float(obj.get("latitude")) if obj.get("latitude") is not None else None
                lon = float(obj.get("longitude")) if obj.get("longitude") is not None else None
                station_id = _match_station_id(lat, lon, stations)
                hourly = obj.get("hourly")
                if not isinstance(hourly, dict):
                    continue
                times = hourly.get("time", [])
                n = len(times)
                for i in range(n):
                    payload_row = {
                        "psa_id": psa_id,
                        "station_id": station_id,
                        "batch_id": batch_id,
                        "latitude": lat,
                        "longitude": lon,
                        "time": times[i],
                    }
                    for key, arr in hourly.items():
                        if key == "time":
                            continue
                        if isinstance(arr, list) and i < len(arr):
                            val = arr[i]
                            if val is not None:
                                payload_row[key] = val
                    rows_to_insert.append((json.dumps(payload_row), batch_id))
        except Exception:
            continue

    if not rows_to_insert:
        return 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO psa.weather_history_vertical_raw (payload, batch_id) VALUES %s",
                rows_to_insert,
            )
    return len(rows_to_insert)
