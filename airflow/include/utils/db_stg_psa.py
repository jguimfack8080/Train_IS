from .db_conn import get_connection


def copy_stations_stg_to_psa():
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
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE stg.db_stations_raw")


def copy_weather_stg_to_psa():
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
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE stg.weather_forecast_raw")


def copy_weather_archive_stg_to_psa():
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
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE stg.weather_history_raw")


def copy_timetables_plan_stg_to_psa():
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
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE stg.timetables_plan_raw")


def copy_timetables_fchg_stg_to_psa():
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
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE stg.timetables_fchg_raw")


def copy_timetables_rchg_stg_to_psa():
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
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE stg.timetables_rchg_raw")

