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
    'dwh_test_dag',
    default_args=default_args,
    schedule_interval='@once',
)

# Define a Python function to use in tasks
def test_table_create():
    return 'I will create a test table in PostgreSQL data_warehouse DB!'

# Create the tasks
start = DummyOperator(
    task_id='start',
    dag=dag,
)

dwh_test_task = PythonOperator(
    task_id='dwh_test_task',
    python_callable=test_table_create,
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set task dependencies
start >> dwh_test_task >> end

