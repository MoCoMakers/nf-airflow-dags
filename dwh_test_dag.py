from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import psycopg2
import utils
import traceback


_config = utils.get_config_nf()

DB_HOST = _config['db']['postgres_host']
DB_NAME = _config['db']['postgres_name']
DB_USER = _config['db']['postgres_user']
DB_PASSWORD = _config['db']['postgres_password']

test_table_create_sql = _config['sql']['test_table_create']

pg_conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
)

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

