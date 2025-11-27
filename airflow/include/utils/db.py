import json
import psycopg2.extras
import xml.etree.ElementTree as ET
import hashlib
from .db_conn import get_connection
from .db_insert import insert_json, insert_text
from .db_logs import log_api_call, log_process_start, log_process_end, is_batch_processed
from .db_batch import _get_latest_batch_id, get_latest_forecast_batch_id, get_latest_history_batch_id, get_last_archive_end_date
from .db_stg_psa import (
    copy_stations_stg_to_psa,
    purge_stg_stations,
    copy_weather_stg_to_psa,
    purge_stg_weather,
    copy_weather_archive_stg_to_psa,
    purge_stg_weather_archive,
    copy_timetables_plan_stg_to_psa,
    purge_stg_timetables_plan,
    copy_timetables_fchg_stg_to_psa,
    purge_stg_timetables_fchg,
    copy_timetables_rchg_stg_to_psa,
    purge_stg_timetables_rchg,
)
from .weather_transform import insert_forecast_verticalized_to_dwh, insert_history_verticalized_to_dwh


def get_bremen_station_coords(limit=None):
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


def insert_timetables_fchg_events_to_dwh(batch_id: str | None = None) -> int:
    table = "psa.timetables_fchg_raw"
    if batch_id is None:
        batch_id = _get_latest_batch_id(table)
    if batch_id is None:
        return 0

    def _ts_to_iso(s: str | None) -> str | None:
        if not s:
            return None
        s = str(s)
        if len(s) == 10 and s.isdigit():
            y = int("20" + s[0:2])
            m = int(s[2:4])
            d = int(s[4:6])
            hh = int(s[6:8])
            mm = int(s[8:10])
            return f"{y:04d}-{m:02d}-{d:02d}T{hh:02d}:{mm:02d}:00Z"
        return None

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, payload FROM psa.timetables_fchg_raw WHERE batch_id = %s",
                (batch_id,),
            )
            rows = cur.fetchall()

    to_insert = []
    for psa_id, xml_text in rows:
        try:
            root = ET.fromstring(xml_text)
        except Exception:
            continue
        station_name = root.attrib.get("station")
        eva_number = root.attrib.get("eva")
        for s in root.findall("s"):
            for m in s.findall("m"):
                msg_id = m.attrib.get("id")
                t = m.attrib.get("t")
                cat = m.attrib.get("cat")
                pr = m.attrib.get("pr")
                ts = _ts_to_iso(m.attrib.get("ts"))
                vf = _ts_to_iso(m.attrib.get("from"))
                vt = _ts_to_iso(m.attrib.get("to"))
                row = (
                    eva_number,
                    station_name,
                    ts,
                    msg_id,
                    t,
                    cat,
                    int(pr) if pr is not None and str(pr).isdigit() else None,
                    None,
                    vf,
                    vt,
                    None,
                    batch_id,
                )
                to_insert.append(row)
            for parent_tag in ("ar", "dp"):
                for node in s.findall(parent_tag):
                    ct = _ts_to_iso(node.attrib.get("ct"))
                    cp = node.attrib.get("cp")
                    for m in node.findall("m"):
                        msg_id = m.attrib.get("id")
                        t = m.attrib.get("t")
                        c = m.attrib.get("c")
                        ts = _ts_to_iso(m.attrib.get("ts"))
                        ev_time = ct or ts
                        row = (
                            eva_number,
                            station_name,
                            ev_time,
                            msg_id,
                            t,
                            None,
                            None,
                            int(c) if c is not None and str(c).isdigit() else None,
                            None,
                            None,
                            cp,
                            batch_id,
                        )
                        to_insert.append(row)

    if not to_insert:
        return 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO dwh.timetables_fchg_events
                (eva_number, station_name, event_time, message_id, type, category, priority, delay_minutes, valid_from, valid_to, platform_change, batch_id)
                VALUES %s
                """,
                to_insert,
            )
    return len(to_insert)





def insert_timetables_plan_events_to_dwh(batch_id: str | None = None) -> int:
    table = "psa.timetables_plan_raw"
    if batch_id is None:
        batch_id = _get_latest_batch_id(table)
    if batch_id is None:
        return 0

    def _ts_to_iso(s: str | None) -> str | None:
        if not s:
            return None
        s = str(s)
        if len(s) == 10 and s.isdigit():
            y = int("20" + s[0:2])
            m = int(s[2:4])
            d = int(s[4:6])
            hh = int(s[6:8])
            mm = int(s[8:10])
            return f"{y:04d}-{m:02d}-{d:02d}T{hh:02d}:{mm:02d}:00Z"
        return None

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, payload FROM psa.timetables_plan_raw WHERE batch_id = %s",
                (batch_id,),
            )
            rows = cur.fetchall()

    to_insert = []
    seen = set()
    for psa_id, xml_text in rows:
        try:
            root = ET.fromstring(xml_text)
        except Exception:
            continue
        station_name = root.attrib.get("station")
        for s in root.findall("s"):
            service_id = s.attrib.get("id")
            tl = s.find("tl")
            train_number = tl.attrib.get("n") if tl is not None else None
            train_category = tl.attrib.get("c") if tl is not None else None
            train_type = tl.attrib.get("t") if tl is not None else None
            train_direction = tl.attrib.get("f") if tl is not None else None
            for parent_tag in ("dp", "ar"):
                for node in s.findall(parent_tag):
                    event_type = parent_tag
                    event_time = _ts_to_iso(node.attrib.get("pt"))
                    platform = node.attrib.get("pp")
                    train_line_name = node.attrib.get("l")
                    route_path = node.attrib.get("ppth")
                    key = (
                        station_name,
                        service_id,
                        train_number,
                        train_category,
                        train_type,
                        train_direction,
                        event_type,
                        event_time,
                        platform,
                        train_line_name,
                        route_path,
                        batch_id,
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    to_insert.append(key)

    if not to_insert:
        return 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO dwh.timetables_plan_events
                (station_name, service_id, train_number, train_category, train_type, train_direction, event_type, event_time, platform, train_line_name, route_path, batch_id)
                VALUES %s
                """,
                to_insert,
            )
    return len(to_insert)


def insert_timetables_rchg_events_to_dwh(batch_id: str | None = None) -> int:
    table = "psa.timetables_rchg_raw"
    if batch_id is None:
        batch_id = _get_latest_batch_id(table)
    if batch_id is None:
        return 0

    def _ts_to_iso(s: str | None) -> str | None:
        if not s:
            return None
        s = str(s)
        if len(s) == 10 and s.isdigit():
            y = int("20" + s[0:2])
            m = int(s[2:4])
            d = int(s[4:6])
            hh = int(s[6:8])
            mm = int(s[8:10])
            return f"{y:04d}-{m:02d}-{d:02d}T{hh:02d}:{mm:02d}:00Z"
        return None

    def _now_iso() -> str:
        from datetime import datetime
        return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    import xml.etree.ElementTree as ET
    import hashlib

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, payload FROM psa.timetables_rchg_raw WHERE batch_id = %s",
                (batch_id,),
            )
            rows = cur.fetchall()

    to_insert = []
    seen = set()
    for psa_id, xml_text in rows:
        try:
            root = ET.fromstring(xml_text)
        except Exception:
            continue
        station_name = root.attrib.get("station")
        eva_number = root.attrib.get("eva")
        for s in root.findall("s"):
            event_id = s.attrib.get("id")
            # Messages au niveau <s>
            for m in s.findall("m"):
                msg_id = m.attrib.get("id")
                cat = m.attrib.get("cat")
                t = m.attrib.get("t")
                pr = m.attrib.get("pr")
                vf = _ts_to_iso(m.attrib.get("from"))
                vt = _ts_to_iso(m.attrib.get("to"))
                ts = _ts_to_iso(m.attrib.get("ts"))
                c = m.attrib.get("c")
                old_time = None
                new_time = None
                platform = None
                platform_change = None
                route = None
                ts_event = ts or _now_iso()
                parts = [
                        event_id or "",
                        eva_number or "",
                        station_name or "",
                        msg_id or "",
                        "m",
                        cat or "",
                        t or "",
                        str(pr) if pr is not None else "",
                        vf or "",
                        vt or "",
                        old_time or "",
                        new_time or "",
                        str(c) if c is not None else "",
                        platform or "",
                        platform_change or "",
                        route or "",
                        ts_event or "",
                    ]
                h = hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()
                if h in seen:
                    continue
                seen.add(h)
                row = (
                    event_id,
                    eva_number,
                    station_name,
                    msg_id,
                    "m",
                    cat,
                    t,
                    int(pr) if pr is not None and str(pr).isdigit() else None,
                    vf,
                    vt,
                    old_time,
                    new_time,
                    int(c) if c is not None and str(c).isdigit() else None,
                    platform,
                    platform_change,
                    route,
                    None,
                    ts_event,
                    batch_id,
                    h,
                )
                to_insert.append(row)
            # Noeuds ar/dp
            for parent_tag in ("dp", "ar"):
                for node in s.findall(parent_tag):
                    event_type = parent_tag
                    ct = _ts_to_iso(node.attrib.get("ct"))
                    pt = _ts_to_iso(node.attrib.get("pt"))
                    platform = node.attrib.get("pp")
                    platform_change = node.attrib.get("cp")
                    route = node.attrib.get("ppth")
                    train_line_name = node.attrib.get("l")
                    last_m = None
                    m_children = node.findall("m")
                    if m_children:
                        last_m = m_children[-1]
                    msg_id = last_m.attrib.get("id") if last_m is not None else None
                    cat = last_m.attrib.get("cat") if last_m is not None else None
                    t = last_m.attrib.get("t") if last_m is not None else None
                    c = last_m.attrib.get("c") if last_m is not None else None
                    vf = _ts_to_iso(last_m.attrib.get("from")) if last_m is not None else None
                    vt = _ts_to_iso(last_m.attrib.get("to")) if last_m is not None else None
                    ts = _ts_to_iso(last_m.attrib.get("ts")) if last_m is not None else None
                    ts_event = ct or ts or _now_iso()
                    parts = [
                        event_id or "",
                        eva_number or "",
                        station_name or "",
                        msg_id or "",
                        event_type,
                        cat or "",
                        t or "",
                        "",
                        vf or "",
                        vt or "",
                        pt or "",
                        ct or "",
                        str(c) if c is not None else "",
                        platform or "",
                        platform_change or "",
                        route or "",
                        ts_event or "",
                        train_line_name or "",
                    ]
                    h = hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()
                    if h in seen:
                        continue
                    seen.add(h)
                    row = (
                        event_id,
                        eva_number,
                        station_name,
                        msg_id,
                        event_type,
                        cat,
                        t,
                        None,
                        vf,
                        vt,
                        pt,
                        ct,
                        int(c) if c is not None and str(c).isdigit() else None,
                        platform,
                        platform_change,
                        route,
                        train_line_name,
                        ts_event,
                        batch_id,
                        h,
                    )
                    to_insert.append(row)

    if not to_insert:
        return 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO dwh.timetables_rchg_events
                (event_id, eva_number, station_name, message_id, event_type, category, change_type, priority, valid_from, valid_to, old_time, new_time, delay_minutes, platform, platform_change, route, train_line_name, timestamp_event, batch_id, event_hash)
                VALUES %s
                ON CONFLICT (event_hash) DO NOTHING
                """,
                to_insert,
            )
    return len(to_insert)
