from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

# Create the DAG object
dag = DAG(
    'reference_dag',
    default_args=default_args,
    schedule_interval='@once',
)

# Define a Python function to use in tasks
def print_hello():
    return 'Hello world!'

# Create the tasks
start = DummyOperator(
    task_id='start',
    dag=dag,
)

hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set task dependencies
start >> hello_task >> end

