from .db_conn import get_connection


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

