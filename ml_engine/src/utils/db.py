import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def get_db_connection():
    """
    Returns a psycopg2 connection to the data warehouse.
    """
    return psycopg2.connect(
        host=os.getenv("DATA_DB_HOST", "postgres"),
        port=os.getenv("DATA_DB_PORT", "5432"),
        dbname=os.getenv("DATA_DB_NAME", "train_dw"),
        user=os.getenv("DATA_DB_USER", "dw"),
        password=os.getenv("DATA_DB_PASSWORD", "dw")
    )

def get_db_engine():
    """
    Returns a sqlalchemy engine.
    """
    user = os.getenv("DATA_DB_USER", "dw")
    password = os.getenv("DATA_DB_PASSWORD", "dw")
    host = os.getenv("DATA_DB_HOST", "postgres")
    port = os.getenv("DATA_DB_PORT", "5432")
    dbname = os.getenv("DATA_DB_NAME", "train_dw")
    
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}")

def load_data(query):
    """
    Executes a SQL query and returns a pandas DataFrame.
    """
    conn = get_db_connection()
    try:
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()
