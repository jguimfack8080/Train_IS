import os
import psycopg2


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

