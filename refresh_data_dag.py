from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import logging
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import refresh_data_methods

# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

# Create the DAG object
dag = DAG(
    'refresh_data_dag',
    default_args=default_args,
    schedule_interval='@once',
)

# Create the tasks
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Define the logger at the module level
logger = logging.getLogger(__name__)  # This logger will be used across all functions

def refreshData():
    #refresh_data_methods.refresh_secondary_dose_curve()
    #refresh_data_methods.refresh_s_prime()
    #refresh_data_methods.refresh_damaging_mutations()
    refresh_data_methods.refresh_mutations("PANCREAS", "INCREMENTAL")
    #refresh_pooled_delta_s_results(7300, "LUNG")


refresh_data_task = PythonOperator(
    task_id='refresh_data_task',
    python_callable=refreshData,
    dag=dag,
    execution_timeout=timedelta(seconds=900000))

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set task dependencies
start >> refresh_data_task >> end