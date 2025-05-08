from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from pathlib import Path
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import utils  # Now this should work

_config = utils.get_config_data_refresh()

#test_name = _config['sql']['query_1']

pg_hook = PostgresHook(postgres_conn_id='Comp_Bio_Hub_Postgres', schema='public')


# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

# Create the DAG object
dag = DAG(
    'test_dag',
    default_args=default_args,
    schedule_interval='@once',
)

# Create the tasks
start = DummyOperator(
    task_id='start',
    dag=dag,
)

def refreshData():
    print("CONFIG CONTENT:", _config)

refresh_data_task = PythonOperator(
    task_id='refresh_data_task',
    python_callable=refreshData,
    dag=dag,
    execution_timeout=timedelta(seconds=900000),
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set task dependencies
start >> refresh_data_task >> end