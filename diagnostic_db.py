import os
import pandas as pd
from sqlalchemy import create_engine, text

# Database connection
user = os.getenv("DATA_DB_USER", "dw")
password = os.getenv("DATA_DB_PASSWORD", "dw")
host = os.getenv("DATA_DB_HOST", "postgres")
port = os.getenv("DATA_DB_PORT", "5432")
dbname = os.getenv("DATA_DB_NAME", "train_dw")

engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}")

def run_diagnostic():
    stations = ['Bremen Hbf', 'Bremerhaven Hbf', 'Bremerhaven-Lehe', 'Bremerhaven-Wulsdorf']
    stations_str = "', '".join(stations)
    
    query_pred = f"""
    SELECT 
        'predictions' as source,
        station_name,
        DATE(scheduled_time) as date,
        COUNT(*) as count,
        MIN(scheduled_time) as min_time,
        MAX(scheduled_time) as max_time
    FROM dwh.predictions
    WHERE station_name IN ('{stations_str}')
      AND scheduled_time >= NOW() - INTERVAL '2 DAYS'
      AND scheduled_time <= NOW() + INTERVAL '2 DAYS'
    GROUP BY station_name, DATE(scheduled_time)
    ORDER BY station_name, date;
    """
    
    query_plan = f"""
    SELECT 
        'timetables' as source,
        station_name,
        DATE(scheduled_time) as date,
        COUNT(*) as count,
        MIN(scheduled_time) as min_time,
        MAX(scheduled_time) as max_time
    FROM dwh.timetables_plan_events
    WHERE station_name IN ('{stations_str}')
      AND scheduled_time >= NOW() - INTERVAL '2 DAYS'
      AND scheduled_time <= NOW() + INTERVAL '2 DAYS'
    GROUP BY station_name, DATE(scheduled_time)
    ORDER BY station_name, date;
    """
    
    query_plan = f"""
    SELECT 
        'timetables' as source,
        station_name,
        DATE(event_time) as date,
        COUNT(*) as count,
        MIN(event_time) as min_time,
        MAX(event_time) as max_time
    FROM dwh.timetables_plan_events
    WHERE station_name IN ('{stations_str}')
      AND event_time >= NOW() - INTERVAL '2 DAYS'
      AND event_time <= NOW() + INTERVAL '2 DAYS'
    GROUP BY station_name, DATE(event_time)
    ORDER BY station_name, date;
    """
    
    query_stations = f"""
    SELECT 
        name,
        ds100,
        eva_number
    FROM dwh.v_stations
    WHERE name IN ('{stations_str}')
    """
    
    print("Running diagnostic query...")
    with engine.connect() as conn:
        print("\n--- Predictions ---")
        result = conn.execute(text(query_pred))
        df_pred = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(df_pred.to_string())
        
        print("\n--- Timetables (Source) ---")
        result = conn.execute(text(query_plan))
        df_plan = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(df_plan.to_string())

        print("\n--- Stations (v_stations) ---")
        result = conn.execute(text(query_stations))
        df_stations = pd.DataFrame(result.fetchall(), columns=result.keys())
        print(df_stations.to_string())


    # Check NOW() time in DB
    with engine.connect() as conn:
        now_res = conn.execute(text("SELECT NOW()::timestamp, CURRENT_SETTING('TIMEZONE');"))
        now_row = now_res.fetchone()
        print(f"\nDB NOW(): {now_row[0]}")
        print(f"DB Timezone: {now_row[1]}")

if __name__ == "__main__":
    run_diagnostic()
