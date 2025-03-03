from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import traceback
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd



secondary_dose_curve_table_sql = """CREATE TABLE IF NOT EXISTS im_dep_sprime_secondary_dose_curve 
(broad_id VARCHAR(255), depmap_id VARCHAR(255), ccle_name VARCHAR(1000), screen_id VARCHAR(50),
upper_limit INTEGER, lower_limit FLOAT, slope FLOAT,
r2 FLOAT, auc FLOAT, ec50 FLOAT, ic50 FLOAT,
name VARCHAR(255), moa VARCHAR(1000), target VARCHAR(1000),
disease_area VARCHAR(1000), indication VARCHAR(1000),
smiles VARCHAR(1500), phase VARCHAR(255), passed_str_profiling boolean, row_name VARCHAR(255),
CONSTRAINT sprime_dose_curve_pk PRIMARY KEY (broad_id, depmap_id, screen_id))"""

secondary_dose_curve_table_insert_sql = """INSERT INTO im_dep_sprime_secondary_dose_curve 
(broad_id, depmap_id, ccle_name, screen_id, upper_limit, lower_limit, 
slope, r2, auc, ec50, ic50, name, moa, target, disease_area, indication,
smiles, phase, passed_str_profiling, row_name) 
VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT(broad_id, depmap_id, screen_id) DO NOTHING"""

omics_mutations_matrix_table_sql = """CREATE TABLE IF NOT EXISTS im_dep_sprime_damaging_mutations (cell_line VARCHAR(255), gene VARCHAR(255), value INTEGER)"""

omics_mutations_matrix_table_insert_sql = "INSERT INTO im_dep_sprime_damaging_mutations (cell_line, gene, value) values (%s, %s,%s) ON CONFLICT (cell_line, gene) DO NOTHING"

DEP_PRISM_PATH = "/home/gatlay/nf_streamlit/app/data/DepMap/Prism19Q4"
DEP_PUBLIC_PATH = "/home/gatlay/nf_streamlit/app/data/DepMap/Public24Q2"

SEC_RESP_DOSE_CURVE = "secondary-screen-dose-response-curve-parameters.csv"


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
    refresh_data("SEC_RESP_DOSE_CURVE") 
             

def refresh_data(process_type):
    start_time = datetime.now()
    
    table_create_sql = None
    table_name = None
    data_insert_sql = None
    data_file_name = None
    file_path = None
    if process_type == "SEC_RESP_DOSE_CURVE":
        table_name = "im_dep_sprime_secondary_dose_curve"
        table_create_sql = secondary_dose_curve_table_sql
        data_insert_sql = secondary_dose_curve_table_insert_sql
        file_path = DEP_PRISM_PATH
    input_folder = Path(file_path)
    try:
        print(f"Data refresh process started for {table_name}. Table will be created if it doesn't exist.")
        pg_hook = PostgresHook(postgres_conn_id='Comp_Bio_Hub_Postgres', schema='public')
        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()
        cursor.execute(table_create_sql)
        pg_conn.commit()
        
        print(f"Started to load {data_file_name} content into memory.")
        # Read the CSV file
        csv_data = pd.read_csv(input_folder / data_file_name)

        rows = []
        for val in csv_data.values:
            rows.append(tuple(list(val.flatten())))
        
        print(f"Started inserting the data in {data_file_name} file into {table_name} table.")
        for rows_batch in batch(rows, 20000):
            cursor.executemany(data_insert_sql, rows_batch)
            pg_conn.commit()
            print(f"{len(rows_batch)} records done.") 
        
        print(f"Total number of rows inserted to DB = {len(rows)}")
    except Exception as e:
        print("Error happened while refreshing the data in database.")
        traceback.print_exc()
        pg_conn.rollback()    
    finally:
        pg_conn.commit()
        cursor.close()
        end_time = datetime.now()
        print(f"Duration to complete the refresh process for {table_name}: {end_time - start_time}")


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
    execution_timeout=timedelta(seconds=100000),
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set task dependencies
start >> refresh_data_task >> end