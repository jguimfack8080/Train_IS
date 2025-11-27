import os
import time
import requests


DB_STATIONS_URL = "https://apis.deutschebahn.com/db-api-marketplace/apis/station-data/v2/stations"


def fetch_db_stations(limit: int = 100, offset: int = 0, searchstring: str = "*"):
    client_id = os.getenv("DB_CLIENT_ID", "")
    api_key = os.getenv("DB_API_KEY", "")

    headers = {
        "DB-Client-ID": client_id,
        "DB-Api-Key": api_key,
        "accept": "application/json",
    }
    params = {"limit": limit, "offset": offset, "searchstring": searchstring}

    start = time.time()
    resp = requests.get(DB_STATIONS_URL, headers=headers, params=params, timeout=60)
    elapsed_ms = int((time.time() - start) * 1000)

    data = None
    try:
        data = resp.json()
    except Exception:
        data = {"error": "invalid_json", "text": resp.text[:1000]}

    return data, resp.status_code, elapsed_ms


def _extract_items_and_total(payload):
    # Unterstützt mehrere mögliche Antwortstrukturen
    items = []
    total = None

    if isinstance(payload, dict):
        candidates = [
            payload.get("data"),
            payload.get("stations"),
            payload.get("results"),
            payload.get("items"),
        ]
        for c in candidates:
            if isinstance(c, list):
                items = c
                break
        # Fallback: Manche APIs liefern direkt eine Liste unter einem anderen Schlüssel
        if not items and any(isinstance(v, list) for v in payload.values()):
            for v in payload.values():
                if isinstance(v, list):
                    items = v
                    break
        total = payload.get("total")
    elif isinstance(payload, list):
        items = payload
        total = len(items)

    return items, total


def fetch_db_stations_all(limit: int = 100, searchstring: str = "*"):
    # Paginierungsschleife: Offset erhöht sich in Schritten von limit bis total erreicht ist
    aggregated = []
    offset = 0
    page_count = 0
    total = None
    status_code = 200
    start = time.time()

    while True:
        payload, status_code, _ = fetch_db_stations(limit=limit, offset=offset, searchstring=searchstring)
        items, page_total = _extract_items_and_total(payload)

        if total is None and page_total is not None:
            total = int(page_total)

        if status_code != 200:
            # Bei HTTP‑Fehler abbrechen; Status wird zurückgegeben
            break

        if not items:
            # Keine Daten: Ende
            break

        aggregated.extend(items)
        page_count += 1
        offset += limit

        if total is not None and offset >= total:
            break

    elapsed_ms_total = int((time.time() - start) * 1000)
    meta = {"total": total, "page_count": page_count, "limit": limit}
    return aggregated, status_code, elapsed_ms_total, meta


def fetch_open_meteo_forecast(coords, hourly_params: str):
    # coords: list of (lat, lon)
    lat_list = ",".join([str(round(c[0], 6)) for c in coords])
    lon_list = ",".join([str(round(c[1], 6)) for c in coords])

    url = (
        "https://api.open-meteo.com/v1/forecast?"
        f"latitude={lat_list}&longitude={lon_list}&hourly={hourly_params}"
    )

    start = time.time()
    resp = requests.get(url, headers={"Accept": "application/json"}, timeout=60)
    elapsed_ms = int((time.time() - start) * 1000)

    data = None
    try:
        data = resp.json()
    except Exception:
        data = {"error": "invalid_json", "text": resp.text[:1000]}

    return data, resp.status_code, elapsed_ms


def fetch_open_meteo_archive(coords, start_date: str, end_date: str, hourly_params: str):
    # coords: Liste von (lat, lon); Datumsformat YYYY‑MM‑DD
    lat_list = ",".join([str(round(c[0], 6)) for c in coords])
    lon_list = ",".join([str(round(c[1], 6)) for c in coords])

    url = (
        "https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat_list}&longitude={lon_list}&start_date={start_date}&end_date={end_date}&hourly={hourly_params}"
    )

    start = time.time()
    resp = requests.get(url, headers={"Accept": "application/json"}, timeout=60)
    elapsed_ms = int((time.time() - start) * 1000)

    data = None
    try:
        data = resp.json()
    except Exception:
        data = {"error": "invalid_json", "text": resp.text[:1000]}

    return data, resp.status_code, elapsed_ms


# ===============================
# Deutsche Bahn Timetables (XML)
# ===============================

def _db_headers_xml():
    client_id = os.getenv("DB_CLIENT_ID", "")
    api_key = os.getenv("DB_API_KEY", "")
    return {
        # Le marketplace accepte généralement cet en-tête; aligné sur la doc fournie
        "DB-Client-Id": client_id,
        "DB-Api-Key": api_key,
        "accept": "application/xml",
    }


def fetch_db_timetables_plan(eva_no: str, date_yymmdd: str, hour_hh: str):
    """
    Appelle l'endpoint PLAN (XML) pour une gare EVA et une heure spécifique.
    Retourne le texte XML brut (stocké ensuite tel quel en JSON {"xml": ...}).
    """
    url = (
        "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/plan/"
        f"{eva_no}/{date_yymmdd}/{hour_hh}"
    )
    headers = _db_headers_xml()
    start = time.time()
    resp = requests.get(url, headers=headers, timeout=60)
    elapsed_ms = int((time.time() - start) * 1000)
    return resp.text, resp.status_code, elapsed_ms


def fetch_db_timetables_fchg(eva_no: str):
    """
    Appelle l'endpoint FCHG (XML) pour tous les changements connus à partir de maintenant.
    Retourne le texte XML brut.
    """
    url = (
        "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/fchg/"
        f"{eva_no}"
    )
    headers = _db_headers_xml()
    start = time.time()
    resp = requests.get(url, headers=headers, timeout=60)
    elapsed_ms = int((time.time() - start) * 1000)
    return resp.text, resp.status_code, elapsed_ms


def fetch_db_timetables_rchg(eva_no: str):
    """
    Appelle l'endpoint RCHG (XML) pour les changements récents.
    Retourne le texte XML brut.
    """
    url = (
        "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/rchg/"
        f"{eva_no}"
    )
    headers = _db_headers_xml()
    start = time.time()
    resp = requests.get(url, headers=headers, timeout=60)
    elapsed_ms = int((time.time() - start) * 1000)
    return resp.text, resp.status_code, elapsed_ms