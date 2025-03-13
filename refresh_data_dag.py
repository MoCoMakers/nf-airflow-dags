from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import traceback
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import time
import pickle


secondary_dose_curve_raw_table_sql = """CREATE TABLE IF NOT EXISTS im_dep_raw_secondary_dose_curve 
(broad_id VARCHAR(255), depmap_id VARCHAR(255), ccle_name VARCHAR(1000), screen_id VARCHAR(50),
upper_limit INTEGER, lower_limit FLOAT, slope FLOAT,
r2 FLOAT, auc FLOAT, ec50 FLOAT, ic50 FLOAT,
name VARCHAR(255), moa VARCHAR(1000), target VARCHAR(1000),
disease_area VARCHAR(1000), indication VARCHAR(1000),
smiles VARCHAR(1500), phase VARCHAR(255), passed_str_profiling boolean, row_name VARCHAR(255))"""

secondary_dose_curve_raw_insert_sql = """INSERT INTO im_dep_raw_secondary_dose_curve 
(broad_id, depmap_id, ccle_name, screen_id, upper_limit, lower_limit, 
slope, r2, auc, ec50, ic50, name, moa, target, disease_area, indication,
smiles, phase, passed_str_profiling, row_name) 
VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

secondary_dose_curve_raw_select = "select * from im_dep_raw_secondary_dose_curve"


omics_mutations_matrix_raw_table_sql = """CREATE TABLE IF NOT EXISTS im_dep_raw_damaging_mutations (gene VARCHAR(255), values BYTEA)"""
omics_mutations_matrix_raw_insert_sql = "INSERT INTO im_dep_raw_damaging_mutations (gene, values) values (%s, %s)"

im_sprime_solved_s_prime_table_sql = """CREATE TABLE IF NOT EXISTS im_sprime_solved_s_prime 
(broad_id VARCHAR(255), depmap_id VARCHAR(255), ccle_name VARCHAR(1000), screen_id VARCHAR(50),
upper_limit INTEGER, lower_limit FLOAT, slope FLOAT,
r2 FLOAT, auc FLOAT, ec50 FLOAT, ic50 FLOAT,
name VARCHAR(255), moa VARCHAR(1000), target VARCHAR(1000),
disease_area VARCHAR(1000), indication VARCHAR(1000),
smiles VARCHAR(1500), phase VARCHAR(255), passed_str_profiling boolean, row_name VARCHAR(255), eff FLOAT, eff_100 FLOAT, eff_ec50 FLOAT, s_prime FLOAT)"""

im_sprime_solved_s_prime_insert_sql = """INSERT INTO im_sprime_solved_s_prime 
(broad_id, depmap_id, ccle_name, screen_id, upper_limit, lower_limit, 
slope, r2, auc, ec50, ic50, name, moa, target, disease_area, indication,
smiles, phase, passed_str_profiling, row_name, eff, eff_100, eff_ec50, s_prime) 
VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""


DEP_PRISM_PATH = "/home/gatlay/nf_streamlit/app/data/DepMap/Prism19Q4"
DEP_PUBLIC_PATH = "/home/gatlay/nf_streamlit/app/data/DepMap/Public24Q2"

SEC_RESP_DOSE_CURVE = "secondary-screen-dose-response-curve-parameters.csv"
OMICS_MUTATIONS_MATRIX = "OmicsSomaticMutationsMatrixDamaging.csv"

pg_hook = PostgresHook(postgres_conn_id='Comp_Bio_Hub_Postgres', schema='public')


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
    save_sec_resp_dose_curve_data()
    save_omics_data()
    solve_S_Prime()
             

# Save the contents of the "dose-response-curve-parameters.csv" file to table "im_dep_raw_secondary_dose_curve"
def save_sec_resp_dose_curve_data():
    start_time = datetime.now()
    
    table_name = "im_dep_raw_secondary_dose_curve"
    table_create_sql = secondary_dose_curve_raw_table_sql
    data_insert_sql = secondary_dose_curve_raw_insert_sql
    file_path = DEP_PRISM_PATH
    data_file_name = SEC_RESP_DOSE_CURVE
        
    input_folder = Path(file_path)
    try:
        print(f"Data refresh process started for {table_name}.")
        pg_conn = pg_hook.get_conn()
        cursor = pg_conn.cursor()
        
        cursor.execute(table_create_sql)
        pg_conn.commit()
        
        chunksize = 20000
        total = 0
        for chunk in pd.read_csv(input_folder / data_file_name, chunksize=chunksize):
            start_time_insert = datetime.now()
            rows = []
            for row in chunk.values:
                rows.append(tuple(list(row.flatten())))
            cursor.executemany(data_insert_sql, rows)
            pg_conn.commit()
            end_time_insert = datetime.now()
            print(f"Duration to insert {len(chunk)} records: {end_time_insert - start_time_insert}")
            total = total + len(chunk.values)
            time.sleep(3)
        print(f"Total number of records inserted to {table_name} table = {total}") 
        
    except Exception as e:
        print(f"Error happened while refreshing {table_name} table.")
        traceback.print_exc()
        pg_conn.rollback()    
    finally:
        pg_conn.commit()
        cursor.close()
        end_time = datetime.now()
        print(f"Duration to complete the refresh process for {table_name}: {end_time - start_time}")

# Save the contents of the "OmicsSomaticMutationsMatrixDamaging.csv" file to table "im_dep_raw_damaging_mutations"
# row=(gene_name, values=[values in the column specific to the gene])
# Each value in values array correspond to a cell line.
# It will be assumed that cell line names are known and pre-ordered. Cell line names can be stored in a separate table.
def save_omics_data():
    table_name = "im_dep_raw_damaging_mutations"
    table_create_sql = omics_mutations_matrix_raw_table_sql
    data_insert_sql = omics_mutations_matrix_raw_insert_sql
    drop_table_sql = f"drop table if exists {table_name}"

    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    cursor.execute(drop_table_sql)
    pg_conn.commit()
    
    cursor.execute(table_create_sql)
    pg_conn.commit()
    print(f"DB table {table_name} has been created.")

    input_folder = Path(DEP_PUBLIC_PATH)
    filename = OMICS_MUTATIONS_MATRIX

    damaging_mutations = pd.read_csv(input_folder/filename)

    genes = damaging_mutations.columns.tolist()[1:]

    insert_rows = []
    for gene in genes:
        insert_rows.append((gene, pickle.dumps(damaging_mutations[gene])))
    cursor.executemany(data_insert_sql, insert_rows)
    pg_conn.commit()
    print(f"Total # of rows inserted into {table_name} table: {len(insert_rows)}")


# Solve S' for all entries in response-curve-parameters
def solve_S_Prime():
    table_name = "im_sprime_solved_s_prime"
    table_create_sql = im_sprime_solved_s_prime_table_sql
    data_insert_sql = im_sprime_solved_s_prime_insert_sql
    drop_table_sql = f"drop table if exists {table_name}"
    
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    cursor.execute(drop_table_sql)
    pg_conn.commit()
    
    cursor.execute(table_create_sql)
    pg_conn.commit()
    print(f"DB table {table_name} has been created.")

    cursor.execute(secondary_dose_curve_raw_select)
    secondary_raw_data = cursor.fetchall()
    
    for rows_batch in batch(secondary_raw_data, 10000):
        insert_rows = []
        for row in rows_batch:

            # Derive EFF (upper_limit - lower_limit) 
            #df['EFF'] = df['upper_limit'] - df['lower_limit']
            EFF = row[4]  - row[5]

            # Derive EFF*100
            #df['EFF*100'] = df['EFF'] * 100
            EFF_100 = EFF * 100

            # Derive EFF/EC50
            #df['EFF/EC50'] = df['EFF'] / df['ec50']
            EFF_EC50 = EFF / row[9]

            # Derive S'
            # ASINH((EFF*100)/EC50)
            #df["S'"] = np.arcsinh(df['EFF*100'] / df['ec50'])
            S_PRIME = np.arcsinh(EFF_100 / row[9])

            new_row_values = list(row)
            new_row_values.extend([EFF, EFF_100, EFF_EC50, S_PRIME])

            insert_rows.append(tuple(new_row_values))
        
        cursor.executemany(data_insert_sql, insert_rows)
        pg_conn.commit()
        print(f"Total # of rows inserted into {table_name} table: {len(insert_rows)}")


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