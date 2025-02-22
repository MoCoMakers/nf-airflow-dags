from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import traceback


test_table_create_sql = "CREATE TABLE IF NOT EXISTS new_table_here (name VARCHAR(255), description VARCHAR(255))"

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

def test_table_create():
    try:
        pg_hook = PostgresHook(postgres_conn_id='Comp_Bio_Hub_Postgres', schema='public')
        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()
        cursor.execute(test_table_create_sql)
        print("A new table has been created.")
        pg_conn.commit()
    except Exception as e:
        print("Error happened while creating a new table in database.")
        traceback.print_exc()     
    finally:
        cursor.close()

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