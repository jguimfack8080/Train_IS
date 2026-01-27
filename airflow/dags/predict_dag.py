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
    'predict_delay_lstm',
    default_args=default_args,
    description='Run LSTM prediction every 15 minutes',
    schedule_interval='*/15 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ml', 'prediction', 'lstm'],
) as dag:

    predict_task = BashOperator(
        task_id='run_prediction',
        bash_command='docker exec ml_engine python predict.py'
    )
