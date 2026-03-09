from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'predict_future_delays',
    default_args=default_args,
    description='Run LSTM prediction for future trains every 10 minutes',
    schedule_interval='*/10 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ml', 'prediction', 'lstm', 'future'],
) as dag:

    predict_task = BashOperator(
        task_id='run_future_prediction',
        bash_command='docker exec ml_engine python /app/predict_future.py'
    )
