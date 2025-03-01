from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import traceback
from pathlib import Path


secondary_dose_curve_table_sql = """CREATE TABLE IF NOT EXISTS im_dep_sprime_secondary_dose_curve broad_id 
(depmap_id VARCHAR(255), ccle_name VARCHAR(255), screen_id	VARCHAR(50), 
upper_limit INTEGER, lower_limit NUMERIC, slope NUMERIC,
r2	NUMERIC, auc NUMERIC, 
ec50 NUMERIC, ic50 NUMERIC,
name VARCHAR(255), moa VARCHAR(255), target VARCHAR(255),
disease.area VARCHAR(255),	indication VARCHAR(255),
smiles VARCHAR(500), 
phase VARCHAR(255), 
passed_str_profiling boolean
row_name VARCHAR(255))"""

secondary_dose_curve_table_insert_sql = """INSERT INTO public.OA_FUNDERS 
(broad_id, depmap_id, ccle_name, screen_id, upper_limit, lower_limit, 
slope, r2, auc, ec50, ic50, name, moa, target, disease.area, indication,
smiles, phase, passed_str_profiling, row_name) 
VALUES (%%s,%%s,%%s, %%s,%%s,%%s, %%s,%%s,%%s, %%s,%%s,%%s, %%s,%%s,%%s, %%s,%%s,%%s, %%s,%%s)
ON CONFLICT (broad_id, depmap_id) DO NOTHING"""

FILE_PATH = "/home/gatlay/nf_streamlit/app/data/Prism19Q4"

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

def loadFileContent(fileName):
    data_lines = []
    input_folder = Path("FILE_PATH")
    
    with open(input_folder / fileName, 'r') as inputFile:
        for line in inputFile.readlines():
            data_lines.append(line.strip())

    print(f"Total number of rows within the file {file_name} = {len(data_lines)}")
            
    # res = dict()
    # for ele in data_lines:
    #     element_items = ele.split(';')
    #     if res.get(element_items[0]) is not None:
    #         res.get(element_items[0]).append(element_items[1])
    #     else:
    #         res[element_items[0]] = [element_items[1]]
    
    #_logger.info(f"Dictionary length ::: {len(res)}")
    #_logger.info(f"Value for 10.1021/acs.nanolett.0c00127 ::: {res['10.1021/acs.nanolett.0c00127']}")
    #return res

def refresh_data():
    try:
        pg_hook = PostgresHook(postgres_conn_id='Comp_Bio_Hub_Postgres', schema='public')
        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()
        cursor.execute(secondary_dose_curve_table_sql)
        pg_conn.commit()
        #Load the CSV data into the table
        loadFileContent("secondary-screen-dose-response-curve-parameters")
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

refresh_data_task = PythonOperator(
    task_id='refresh_data_task',
    python_callable=refresh_data,
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set task dependencies
start >> refresh_data_task >> end