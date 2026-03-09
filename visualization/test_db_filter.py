import pandas as pd
import psycopg2
import os
from datetime import datetime

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DATA_DB_HOST", "localhost"),
            port=os.getenv("DATA_DB_PORT", "5433"),
            database=os.getenv("DATA_DB_NAME", "train_dw"),
            user=os.getenv("DATA_DB_USER", "dw"),
            password=os.getenv("DATA_DB_PASSWORD", "dw")
        )
        return conn
    except Exception as e:
        print(f"Erreur de connexion à la base de données : {e}")
        return None

def test_load_predictions(historical_mode=False):
    conn = get_db_connection()
    if not conn:
        print("Failed to connect to DB")
        return

    where_clauses = []
    
    if not historical_mode:
        where_clauses.append("p.scheduled_time >= (NOW() - INTERVAL '1 hour')")
        where_clauses.append("p.scheduled_time <= (NOW() + INTERVAL '24 hours')")
    
    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    query = f"""
    SELECT COUNT(*)
    FROM dwh.predictions p
    WHERE {where_sql};
    """
    
    print(f"Testing Mode: {'Historical' if historical_mode else 'Real-Time'}")
    print(f"Query Condition: {where_sql}")
    
    try:
        count = pd.read_sql(query, conn).iloc[0, 0]
        print(f"Rows found: {count}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    print("--- Testing Real-Time Mode ---")
    test_load_predictions(historical_mode=False)
    print("\n--- Testing Historical Mode ---")
    test_load_predictions(historical_mode=True)
