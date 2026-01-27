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
    description='Train LSTM model weekly',
    schedule_interval='@weekly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ml', 'training', 'lstm'],
    doc_md="""
    # Training DAG
    
    Ce DAG entraîne le modèle LSTM sur l'historique complet des données.
    
    ## Gestion des Erreurs
    *   **Exit Code 137 (OOM)** : Si la tâche échoue avec ce code, c'est que le conteneur `ml_engine` manque de mémoire RAM.
        *   **Solution** : Le script `train.py` a été optimisé avec `gc.collect()` et `del`.
        *   Si cela persiste, réduire le volume de données dans `train.py` (paramètre `limit`).
    """
) as dag:

    train_task = BashOperator(
        task_id='run_training',
        bash_command='docker exec ml_engine python train.py',
        # Timeout augmenté à 24h car le dataset est volumineux (3.4M lignes)
        execution_timeout=timedelta(hours=24)
    )
