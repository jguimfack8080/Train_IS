import json
import psycopg2.extras
from .db_conn import get_connection


def insert_json(table: str, payload, batch_id: str | None = None):
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

