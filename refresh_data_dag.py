from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import traceback
from pathlib import Path
from datetime import datetime, timedelta
import csv
import pandas as pd



secondary_dose_curve_table_sql = """CREATE TABLE IF NOT EXISTS im_dep_sprime_secondary_dose_curve 
(broad_id VARCHAR(255), depmap_id VARCHAR(255), ccle_name VARCHAR(1000), screen_id VARCHAR(50),
upper_limit INTEGER, lower_limit FLOAT, slope FLOAT,
r2 FLOAT, auc FLOAT, ec50 FLOAT, ic50 FLOAT,
name VARCHAR(255), moa VARCHAR(1000), target VARCHAR(1000),
disease_area VARCHAR(1000), indication VARCHAR(1000),
smiles VARCHAR(1500), phase VARCHAR(255), passed_str_profiling boolean, row_name VARCHAR(255),
CONSTRAINT sprime_dose_curve_pk PRIMARY KEY (broad_id, depmap_id))"""

secondary_dose_curve_table_insert_sql = """INSERT INTO im_dep_sprime_secondary_dose_curve 
(broad_id, depmap_id, ccle_name, screen_id, upper_limit, lower_limit, 
slope, r2, auc, ec50, ic50, name, moa, target, disease_area, indication,
smiles, phase, passed_str_profiling, row_name) 
VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT(broad_id, depmap_id) DO NOTHING"""

omics_mutations_matrix_table_sql = """CREATE TABLE IF NOT EXISTS im_dep_sprime_damaging_mutations (cell_line VARCHAR(255), gene VARCHAR(255), value INTEGER)"""

omics_mutations_matrix_table_insert_sql = "INSERT INTO im_dep_sprime_damaging_mutations (cell_line, gene, value) values (%s, %s,%s) ON CONFLICT (cell_line, gene) DO NOTHING"

DEP_PRISM_PATH = "/home/gatlay/nf_streamlit/app/data/DepMap/Prism19Q4"
DEP_PUBLIC_PATH = "/home/gatlay/nf_streamlit/app/data/DepMap/Public24Q2"


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

def refreshData():
    print("Data refresh process started.")
    #refresh_data(DEP_PRISM_PATH, "secondary-screen-dose-small-set.csv") 
    refresh_data(DEP_PRISM_PATH, "secondary-screen-dose-response-curve-parameters.csv") 
    print("Data refresh process ended.")         

def refresh_data(file_path, file_name):
    input_folder = Path(file_path)
    try:
        pg_hook = PostgresHook(postgres_conn_id='Comp_Bio_Hub_Postgres', schema='public')
        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()
        cursor.execute(secondary_dose_curve_table_sql)
        pg_conn.commit()
        print("A new table has been created.")
        
        print(f"{file_name} is being processed.")
        # Read the CSV file
        csv_data = pd.read_csv(input_folder / file_name)

        rows = []
        for val in csv_data.values:
            rows.append(tuple(list(val.flatten())))
        
        for rows_batch in batch(rows, 1000):
            cursor.executemany(secondary_dose_curve_table_insert_sql, rows_batch)
            pg_conn.commit()
        
        print(f"Total number of rows inserted to DB = {len(rows)}")
    except Exception as e:
        print("Error happened while refreshing data in database.")
        traceback.print_exc()     
    finally:
        cursor.close()

def batch(iterable, n):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)] 

# Create the tasks
start = DummyOperator(
    task_id='start',
    dag=dag,
)

refresh_data_task = PythonOperator(
    task_id='refresh_data_task',
    python_callable=refreshData,
    dag=dag,
    execution_timeout=timedelta(seconds=10000),
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set task dependencies
start >> refresh_data_task >> end