import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

class DBConnection:
    def __init__(self):
        self.db_user = os.getenv("DATA_DB_USER", "dw")
        self.db_password = os.getenv("DATA_DB_PASSWORD", "dw")
        self.db_host = os.getenv("DATA_DB_HOST", "postgres")
        self.db_port = os.getenv("DATA_DB_PORT", "5432")
        self.db_name = os.getenv("DATA_DB_NAME", "train_dw")
        self.engine = self._create_engine()

    def _create_engine(self) -> Engine:
        url = f"postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        return create_engine(url)

    def get_training_data(self, limit: int = None, start_date: str = None) -> pd.DataFrame:
        query = """
        SELECT * 
        FROM dwh.v_training_dataset 
        WHERE current_delay IS NOT NULL
        """
        params = {}
        
        if start_date:
            query += " AND scheduled_time >= %(start_date)s"
            params['start_date'] = start_date

        query += " ORDER BY train_line_ride_id, scheduled_time ASC"
        
        if limit:
            query += f" LIMIT {limit}"
        
        return pd.read_sql(query, self.engine, params=params)

    def save_predictions(self, df: pd.DataFrame):
        # Use chunksize to avoid massive queries
        df.to_sql('predictions', self.engine, schema='dwh', if_exists='append', index=False, method='multi', chunksize=100)
