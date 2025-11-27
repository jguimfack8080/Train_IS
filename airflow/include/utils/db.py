import os
import json
import psycopg2
import psycopg2.extras


def _conn_params():
    return dict(
        host=os.getenv("DATA_DB_HOST", "localhost"),
        port=int(os.getenv("DATA_DB_PORT", "5432")),
        dbname=os.getenv("DATA_DB_NAME", "train_dw"),
        user=os.getenv("DATA_DB_USER", "dw"),
        password=os.getenv("DATA_DB_PASSWORD", "dw"),
    )


def get_connection():
    return psycopg2.connect(**_conn_params())


def insert_json(table: str, payload, batch_id: str | None = None):
    # payload kann List oder Dict sein
    with get_connection() as conn:
        with conn.cursor() as cur:
            if isinstance(payload, list):
                if batch_id is not None:
                    values = [(json.dumps(item), batch_id) for item in payload]
                    psycopg2.extras.execute_values(
                        cur,
                        f"INSERT INTO {table} (payload, batch_id) VALUES %s",
                        values,
                    )
                else:
                    values = [(json.dumps(item),) for item in payload]
                    psycopg2.extras.execute_values(
                        cur,
                        f"INSERT INTO {table} (payload) VALUES %s",
                        values,
                    )
            else:
                if batch_id is not None:
                    cur.execute(
                        f"INSERT INTO {table} (payload, batch_id) VALUES (%s, %s)",
                        (json.dumps(payload), batch_id),
                    )
                else:
                    cur.execute(
                        f"INSERT INTO {table} (payload) VALUES (%s)",
                        (json.dumps(payload),),
                    )


def insert_text(table: str, texts: list[str] | str, batch_id: str | None = None):
    """Insère du texte brut (par exemple XML) tel quel dans la colonne `payload`.

    - Si `texts` est une liste de chaînes, effectue un insert en batch.
    - Si `texts` est une seule chaîne, insère une seule ligne.
    - Ne fait AUCUNE transformation (pas de JSON), conserve le contenu exactement.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            if isinstance(texts, list):
                if batch_id is not None:
                    values = [(t, batch_id) for t in texts]
                    psycopg2.extras.execute_values(
                        cur,
                        f"INSERT INTO {table} (payload, batch_id) VALUES %s",
                        values,
                    )
                else:
                    values = [(t,) for t in texts]
                    psycopg2.extras.execute_values(
                        cur,
                        f"INSERT INTO {table} (payload) VALUES %s",
                        values,
                    )
            else:
                if batch_id is not None:
                    cur.execute(
                        f"INSERT INTO {table} (payload, batch_id) VALUES (%s, %s)",
                        (texts, batch_id),
                    )
                else:
                    cur.execute(
                        f"INSERT INTO {table} (payload) VALUES (%s)",
                        (texts,),
                    )


def log_api_call(source: str, endpoint: str, params: dict, status_code: int, response_time_ms: int, result_count: int, called_at):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO metadata.api_call_log
                (source, endpoint, params, status_code, response_time_ms, result_count, called_at)
                VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s)
                """,
                (
                    source,
                    endpoint,
                    json.dumps(params),
                    status_code,
                    response_time_ms,
                    result_count,
                    called_at,
                ),
            )


# Entfernt: get_station_coords (nicht verwendet)


def get_bremen_station_coords(limit=None):
    """
    Gibt die Koordinaten (Breitengrad, Längengrad) von Stationen in Bremen zurück.
    Kriterien:
      - Latitude und Longitude nicht NULL
      - eva_number nicht NULL
      - Name beginnt mit 'Brem'
      - ds100 beginnt mit 'HB' (ohne Anführungszeichen)
    """
    query = """
        SELECT latitude, longitude
        FROM dwh.v_stations
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND eva_number IS NOT NULL
          AND name LIKE 'Brem%'
          AND REPLACE(ds100, '"', '') LIKE 'HB%'
    """
    
    if limit is not None:
        query += " LIMIT %s"

    with get_connection() as conn:
        with conn.cursor() as cur:
            if limit is not None:
                cur.execute(query, (limit,))
            else:
                cur.execute(query)
            rows = cur.fetchall()
            return [(float(lat), float(lon)) for lat, lon in rows]


def copy_stations_stg_to_psa():
    # Persistente Kopie der Stationen von STG nach PSA
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO psa.db_stations_raw (payload, batch_id, ingested_at)
                SELECT s.payload, s.batch_id, s.ingested_at
                FROM stg.db_stations_raw s
                """
            )


def purge_stg_stations():
    # Flüchtige Bereinigung der STG‑Daten nach erfolgreicher Kopie
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE stg.db_stations_raw")


def copy_weather_stg_to_psa():
    # Persistente Kopie der Wettervorhersagen von STG nach PSA
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO psa.weather_forecast_raw (payload, batch_id, ingested_at)
                SELECT w.payload, w.batch_id, w.ingested_at
                FROM stg.weather_forecast_raw w
                """
            )


def purge_stg_weather():
    # STG‑Bereinigung für Wettervorhersagen
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE stg.weather_forecast_raw")


def copy_weather_archive_stg_to_psa():
    # Persistente Kopie der Wetterarchive von STG nach PSA
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO psa.weather_history_raw (payload, batch_id, ingested_at)
                SELECT a.payload, a.batch_id, a.ingested_at
                FROM stg.weather_history_raw a
                """
            )


def purge_stg_weather_archive():
    # STG‑Bereinigung für Wetterarchive
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE stg.weather_history_raw")


def copy_timetables_plan_stg_to_psa():
    # Persistente Kopie der Timetables PLAN von STG nach PSA
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO psa.timetables_plan_raw (payload, batch_id, ingested_at)
                SELECT s.payload, s.batch_id, s.ingested_at
                FROM stg.timetables_plan_raw s
                """
            )


def purge_stg_timetables_plan():
    # STG‑Bereinigung für Timetables PLAN
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE stg.timetables_plan_raw")


def copy_timetables_fchg_stg_to_psa():
    # Persistente Kopie der Timetables FCHG von STG nach PSA
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO psa.timetables_fchg_raw (payload, batch_id, ingested_at)
                SELECT s.payload, s.batch_id, s.ingested_at
                FROM stg.timetables_fchg_raw s
                """
            )


def purge_stg_timetables_fchg():
    # STG‑Bereinigung für Timetables FCHG
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE stg.timetables_fchg_raw")


def copy_timetables_rchg_stg_to_psa():
    # Persistente Kopie der Timetables RCHG von STG nach PSA
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO psa.timetables_rchg_raw (payload, batch_id, ingested_at)
                SELECT s.payload, s.batch_id, s.ingested_at
                FROM stg.timetables_rchg_raw s
                """
            )


def purge_stg_timetables_rchg():
    # STG‑Bereinigung für Timetables RCHG
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE stg.timetables_rchg_raw")


def _get_latest_batch_id(table: str) -> str | None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT batch_id FROM %s WHERE batch_id IS NOT NULL ORDER BY id DESC LIMIT 1" % table)
            row = cur.fetchone()
            return row[0] if row else None


def get_latest_forecast_batch_id() -> str | None:
    return _get_latest_batch_id("psa.weather_forecast_raw")


def get_latest_history_batch_id() -> str | None:
    return _get_latest_batch_id("psa.weather_history_raw")


def get_last_archive_end_date() -> str | None:
    """
    Retourne le dernier end_date utilisé pour l'appel d'archive Open‑Meteo
    basé sur le log des API (`metadata.api_call_log`).
    On sélectionne la dernière entrée réussie (status 2xx) avec `endpoint='archive'`.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT params->>'end_date' AS end_date
                FROM metadata.api_call_log
                WHERE source = 'open_meteo'
                  AND endpoint = 'archive'
                  AND status_code BETWEEN 200 AND 299
                  AND params ? 'end_date'
                ORDER BY called_at DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            return row[0] if row and row[0] else None


# Entfernt: transform_*_batch_to_dwh (nur für Views, nicht verwendet)


# Entfernt: _fetch_psa_payloads (nur für Python‑Validierungen verwendet)


# Entfernt: validate_*_batch_python (nicht verwendet)


# ===============================
# Vertikalisierung und Einfügen ins DWH (physische Tabellen)
# ===============================

def _fetch_psa_entries(table: str, batch_id: str) -> list[tuple[int, dict]]:
    """Gibt (psa_id, payload) für einen gegebenen PSA‑Batch zurück."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT id, payload FROM {table} WHERE batch_id = %s",
                (batch_id,),
            )
            rows = cur.fetchall()
            return [(int(r[0]), r[1]) for r in rows]


def _load_stations_for_mapping() -> list[tuple[str, float, float]]:
    """Charge uniquement les stations de Brême (station_id, latitude, longitude) pour le mapping.

    Filtre aligné sur get_bremen_station_coords():
      - latitude/longitude non null
      - eva_number non null
      - name LIKE 'Brem%'
      - REPLACE(ds100, '"', '') LIKE 'HB%'
    """
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
    """
    Gibt die nächstgelegene station_id zurück. Entspricht eine Station exakt (Toleranz ~1e‑4), wird sie gewählt.
    Andernfalls wird die nächstgelegene nach euklidischer Distanz über (lat, lon) gewählt.
    """
    if lat is None or lon is None or not stations:
        return None
    tol = 1e-4
    for sid, s_lat, s_lon in stations:
        if abs(lat - s_lat) < tol and abs(lon - s_lon) < tol:
            return sid
    # ansonsten nächstgelegen
    best_sid = None
    best_d = None
    for sid, s_lat, s_lon in stations:
        d = (lat - s_lat) ** 2 + (lon - s_lon) ** 2
        if best_d is None or d < best_d:
            best_d = d
            best_sid = sid
    return best_sid


def insert_forecast_verticalized_to_dwh(batch_id: str | None = None) -> int:
    """
    Vertikalisiert Forecast‑Payloads in stündliche Zeilen und fügt JSON ein
    in dwh.weather_forecast_vertical_raw (payload JSONB, batch_id).
    Gibt die Anzahl der eingefügten Zeilen zurück.
    """
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
            # Vertikalisieren durch Iteration über Index der hourly‑Arrays
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
            # Fehlgebildetes Payload: überspringen und fortfahren
            continue

    if not rows_to_insert:
        return 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO dwh.weather_forecast_vertical_raw (payload, batch_id) VALUES %s",
                rows_to_insert,
            )
    return len(rows_to_insert)


def insert_history_verticalized_to_dwh(batch_id: str | None = None) -> int:
    """
    Vertikalisiert History‑Payloads in stündliche Zeilen und fügt JSON ein
    in dwh.weather_history_vertical_raw (payload JSONB, batch_id).
    Gibt die Anzahl der eingefügten Zeilen zurück.
    """
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
            # History: Payload kann eine Liste von Objekten, ein einzelnes Objekt
            # oder ein Dict mit Schlüssel 'results' (Liste von Objekten) sein
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
                "INSERT INTO dwh.weather_history_vertical_raw (payload, batch_id) VALUES %s",
                rows_to_insert,
            )
    return len(rows_to_insert)


def log_process_start(process_name: str, batch_id: str | None = None):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO metadata.process_log (process_name, status, message, batch_id)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """,
                (process_name, "running", "start", batch_id),
            )
            row = cur.fetchone()
            return int(row[0]) if row else None


def log_process_end(process_id: int, status: str, message: str | None = None):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE metadata.process_log
                SET status = %s, finished_at = NOW(), message = %s
                WHERE id = %s
                """,
                (status, message, int(process_id)),
            )


def is_batch_processed(process_name: str, batch_id: str) -> bool:
    """
    Vérifie si un batch a déjà été traité avec succès pour un process donné.
    Retourne True si une entrée avec status='success' existe dans metadata.process_log.
    """
    if batch_id is None:
        return False
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM metadata.process_log
                WHERE process_name = %s
                  AND batch_id = %s
                  AND status = 'success'
                ORDER BY finished_at DESC NULLS LAST
                LIMIT 1
                """,
                (process_name, batch_id),
            )
            return cur.fetchone() is not None