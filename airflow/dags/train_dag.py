from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
}

with DAG(
    'train_delay_lstm',
    default_args=default_args,
    description='Train LSTM model every hour (rolling window)',
    schedule_interval='0 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ml', 'training', 'lstm'],
    doc_md="""
    # Training DAG
    
    Ce DAG entraîne le modèle LSTM toutes les heures sur une fenêtre glissante.
    
    ## Stratégie
    *   **Fréquence** : Toutes les heures (Hourly).
    *   **Données** : Dernières 24 heures de données.
    *   **Objectif** : Mise à jour continue du modèle pour s'adapter aux conditions récentes sans surcharger le système.
    """
) as dag:

    train_task = BashOperator(
        task_id='run_training',
        bash_command='docker exec ml_engine python train.py --hours 24',
        execution_timeout=timedelta(minutes=30)
    )
