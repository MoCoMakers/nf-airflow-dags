from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import traceback

# SQL queries
extract_sql_1 = "SELECT * FROM public.mv_ligand_list"
extract_sql_2 = "SELECT * FROM public.ligand_physchem"
create_load_tables_1 = """
CREATE TABLE IF NOT EXISTS target_table_1
"""
create_load_tables_2 = """
CREATE TABLE IF NOT EXISTS target_table_2
"""
create_combined_table_sql = """
CREATE TABLE IF NOT EXISTS combined_table AS 
SELECT * FROM target_table_1 
UNION 
SELECT * FROM target_table_2
"""

# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

# Create the DAG object
dag = DAG(
    'dwh_test2_combine_sets_dag',
    default_args=default_args,
    schedule_interval='@once',
)
def generate_create_table_sql(table_name, target_table_name, postgres_conn_id):
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id, schema='public')
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
    """)
    columns = cursor.fetchall()
    pg_conn.close()

    create_table_sql = f"CREATE TABLE IF NOT EXISTS {target_table_name} ("
    create_table_sql += ", ".join([f"{col[0]} {col[1]}" for col in columns])
    create_table_sql += ")"
    return create_table_sql

def create_tables():
    try:
        # Generate SQL for target_table_1
        create_table_1_sql = generate_create_table_sql(
            table_name='mv_ligand_list', 
            target_table_name='target_table_1', 
            postgres_conn_id='guide2pharma'
        )
        
        # Generate SQL for target_table_2
        create_table_2_sql = generate_create_table_sql(
            table_name='ligand_physchem', 
            target_table_name='target_table_2', 
            postgres_conn_id='guide2pharma'
        )

        pg_hook_dest = PostgresHook(postgres_conn_id='Comp_Bio_Hub_Postgres', schema='public')
        pg_conn_dest = pg_hook_dest.get_conn()
        cursor_dest = pg_conn_dest.cursor()
        cursor_dest.execute(create_table_1_sql)
        cursor_dest.execute(create_table_2_sql)
        pg_conn_dest.commit()
        pg_conn_dest.close()
    except Exception as e:
        print("Error during table creation.")
        traceback.print_exc()

def extract_data():
    try:
        pg_hook_src = PostgresHook(postgres_conn_id='guide2pharma', schema='public')
        pg_conn_src = pg_hook_src.get_conn()
        cursor_src = pg_conn_src.cursor()
        cursor_src.execute(extract_sql_1)
        data_1 = cursor_src.fetchall()
        cursor_src.execute(extract_sql_2)
        data_2 = cursor_src.fetchall()
        pg_conn_src.close()
        return (data_1, data_2)
    except Exception as e:
        print("Error during data extraction.")
        traceback.print_exc()

def load_data(data_list):
    try:
        pg_hook_dest = PostgresHook(postgres_conn_id='Comp_Bio_Hub_Postgres', schema='public')
        pg_conn_dest = pg_hook_dest.get_conn()
        cursor_dest = pg_conn_dest.cursor()
        index = 1
        for data_set in data_list:
            data=data_set
            for row in data:
                insert_sql = f"INSERT INTO target_table_{str(index)} VALUES ({','.join(str(value) for value in row)})"
                cursor_dest.execute(insert_sql)
            index = index+1
        pg_conn_dest.commit()
        pg_conn_dest.close()
    except Exception as e:
        print("Error during data loading.")
        traceback.print_exc()

def combine_tables():
    try:
        pg_hook_dest = PostgresHook(postgres_conn_id='Comp_Bio_Hub_Postgres', schema='public')
        pg_conn_dest = pg_hook_dest.get_conn()
        cursor_dest = pg_conn_dest.cursor()
        cursor_dest.execute(create_combined_table_sql)
        pg_conn_dest.commit()
        pg_conn_dest.close()
    except Exception as e:
        print("Error during table combination.")
        traceback.print_exc()

# Create the tasks
start = DummyOperator(
    task_id='start',
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_data_task',
    python_callable=extract_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data_task',
    python_callable=load_data,
    dag=dag,
)

combine_task = PythonOperator(
    task_id='combine_tables_task',
    python_callable=combine_tables,
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set task dependencies
start >> extract_task >> load_task >> combine_task >> end
