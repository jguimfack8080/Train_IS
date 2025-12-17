import json
from .db_conn import get_connection


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

