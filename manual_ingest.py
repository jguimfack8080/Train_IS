import sys
import pendulum
from datetime import datetime
import os

# Add airflow include path
sys.path.append("/opt/airflow/include")

# Set up logging to stdout
import logging
logging.basicConfig(level=logging.INFO)

from utils.timetables import ingest_plan, transform_plan_events_to_dwh
from utils.date_utils import to_tz

def run_manual_ingest():
    print("Starting manual ingestion for Today...")
    
    # Get today in Berlin time
    now = pendulum.now("Europe/Berlin")
    today_start = now.start_of("day")
    
    # Loop over 24 hours
    for hour in range(24):
        dt_hour = today_start.replace(hour=hour, minute=0, second=0, microsecond=0)
        print(f"Processing {dt_hour}...")
        
        try:
            # Ingest (creates a batch in PSA)
            # ingest_plan takes a datetime
            metrics = ingest_plan(dt_hour)
            print(f"  Ingested: {metrics}")
            
            # Transform (processes the latest batch, which is the one we just created)
            res = transform_plan_events_to_dwh()
            print(f"  Transformed: {res}")
            
        except Exception as e:
            print(f"  Error processing {dt_hour}: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    run_manual_ingest()
