from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import traceback
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import time
import numpy as np


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

secondary_dose_curve_raw_select = "select * from im_dep_raw_secondary_dose_curve where screen_id='HTSwithMTS010_Overlayed' and passed_str_profiling=true"

im_sprime_solved_s_prime_table_sql = """CREATE TABLE IF NOT EXISTS im_sprime_solved_s_prime 
(id integer primary key generated always as identity, broad_id VARCHAR(255), depmap_id VARCHAR(255), ccle_name VARCHAR(1000), screen_id VARCHAR(50),
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

im_sprime_solved_s_prime_select_sql = "select id, depmap_id, ccle_name from im_sprime_solved_s_prime"

im_dep_sprime_damaging_mutations_table_sql = """CREATE TABLE IF NOT EXISTS im_dep_sprime_damaging_mutations (cell_line VARCHAR(255), gene_id INTEGER, mutation_value INTEGER)"""
im_dep_sprime_damaging_mutations_insert_sql = "INSERT INTO im_dep_sprime_damaging_mutations (cell_line, gene_id, mutation_value) values (%s, %s,%s)"

im_omics_gene_table_sql = """CREATE TABLE IF NOT EXISTS im_omics_genes (id integer primary key generated always as identity, name VARCHAR(100))"""
im_omics_gene_insert_sql = "INSERT INTO im_omics_genes (name) values (%s)"
im_omics_gene_select_sql = "select * from im_omics_genes"

mutation_values_for_cell_lines = "select * from im_dep_sprime_damaging_mutations where cell_line in ({}) and mutation_value in (0, 2)"

mutation_values_all = "select * from im_dep_sprime_damaging_mutations"
im_sprime_s_prime_with_mutations_table_sql = """CREATE TABLE IF NOT EXISTS im_sprime_s_prime_with_mutations (s_prime_id INTEGER, cell_line VARCHAR(255), tissue VARCHAR(255), gene_id INTEGER, mutation_value INTEGER)"""
im_sprime_s_prime_with_mutations_insert_sql = "INSERT INTO im_sprime_s_prime_with_mutations (s_prime_id, cell_line, tissue, gene_id, mutation_value) values (%s,%s,%s,%s,%s)"


# Just have these columns in “fnl_sprime_pooled_delta_sprime” table for now:
# - name
# - ref_pooled_s_prime = mean of the cell lines matching the filters that have 0 out of 2 damaging mutations,
# - num_ref_lines
# - test_pooled_s_prime = mean of the cell lines matching the filters that have 2 out of 2 damaging mutations
# - num_test_lines
# - delta_s_prime = ref_pooled_s_prime - test_pooled_s_prime
fnl_sprime_pooled_delta_sprime_table_sql = """CREATE TABLE IF NOT EXISTS fnl_sprime_pooled_delta_sprime (name VARCHAR(255), ref_pooled_s_prime FLOAT, num_ref_lines INTEGER, test_pooled_s_prime FLOAT, num_test_lines INTEGER, delta_s_prime FLOAT, gene_id INTEGER, tissue VARCHAR(255))"""
fnl_sprime_pooled_delta_sprime_insert_sql = "INSERT INTO fnl_sprime_pooled_delta_sprime (name, ref_pooled_s_prime, num_ref_lines, test_pooled_s_prime, num_test_lines, delta_s_prime, gene_id, tissue) values (%s,%s,%s,%s,%s,%s,%s,%s)"


source_data_for_fnl_sprime_table = """select distinct s_prime.name, mut.cell_line, s_prime.s_prime, mut.mutation_value from im_sprime_s_prime_with_mutations mut 
left join im_sprime_solved_s_prime s_prime on s_prime.row_name=mut.cell_line
where mut.gene_id=%s and mut.tissue=%s and mut.mutation_value in (0, 2)
and s_prime.ccle_name like %s and s_prime.name in ({})"""

names_for_tissue_select = """select distinct name from im_sprime_solved_s_prime where ccle_name like %s"""

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

def fetch_data_from_db(select_sql):
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(select_sql)
    rows = cursor.fetchall()
    return rows

# row = (cell_line, gene_id, mutation_value)
def get_mutation_values_for_cell_lines(cell_lines):
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    formatted_cell_lines = ', '.join(f"'{w}'" for w in cell_lines)
    cursor.execute(mutation_values_for_cell_lines.format(formatted_cell_lines))
    rows = cursor.fetchall()
    return rows

def refresh_omic_genes():
    input_folder = Path(DEP_PUBLIC_PATH)
    data_file_name = OMICS_MUTATIONS_MATRIX  
    table_name = "im_omics_genes"
    drop_table_sql = f"drop table if exists {table_name}"

    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    try:
        cursor.execute(drop_table_sql)
        pg_conn.commit()

        cursor.execute(im_omics_gene_table_sql)
        pg_conn.commit()
        print(f"DB table {table_name} has been created.")

        damaging_mutations = pd.read_csv(input_folder/data_file_name)

        genes = damaging_mutations.columns.tolist()[1:]
        gene_tuples = [(x,) for x in genes]

        cursor.executemany(im_omics_gene_insert_sql, gene_tuples)
        pg_conn.commit()

        print(f"Genes length: {len(genes)}")
    except Exception as e:
        traceback.print_exc() 
        pg_conn.rollback()
    finally:
        pg_conn.commit()
        cursor.close()

def refreshData():
    refresh_secondary_dose_curve()
    refresh_s_prime()
    refresh_damaging_mutations()
    # 7300 = NF1 (4763)
    refresh_mutations_by_cell_line([7300])
    refresh_pooled_delta_s_results(7300, "LUNG")


# TASK_1:
#Load all dose-response-curve-parameters.csv from ~/nf_streamlit/app/data$
#Table name -> im_dep_raw_secondary_dose_curve
def refresh_secondary_dose_curve():
    start_time_main = datetime.now()
    
    table_name = "im_dep_raw_secondary_dose_curve"
    table_create_sql = secondary_dose_curve_raw_table_sql
    data_insert_sql = secondary_dose_curve_raw_insert_sql
    file_path = DEP_PRISM_PATH
    data_file_name = SEC_RESP_DOSE_CURVE
    drop_table_sql = f"drop table if exists {table_name}"
        
    input_folder = Path(file_path)
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    try:
        print(f"Data refresh process started for {table_name}.")
        cursor.execute(drop_table_sql)
        pg_conn.commit()

        cursor.execute(table_create_sql)
        pg_conn.commit()
        print(f"DB table {table_name} has been created.")
        
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
            print(f"Duration to insert {len(chunk)} records: {(end_time_insert - start_time_insert).seconds} seconds")
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
        end_time_main = datetime.now()
        print(f"Duration to complete the refresh process for {table_name}: {(end_time_main - start_time_main).seconds} seconds")

# TASK_2:
#Load OmicsSomaticMutationsMatrixDamaging.csv from ~/nf_streamlit/app/data
#Table name -> im_dep_sprime_damaging_mutations
def refresh_damaging_mutations():
    start_time_main = datetime.now()
    table_name = "im_dep_sprime_damaging_mutations"
    table_create_sql = im_dep_sprime_damaging_mutations_table_sql
    data_insert_sql = im_dep_sprime_damaging_mutations_insert_sql
    drop_table_sql = f"drop table if exists {table_name}"
    input_folder = Path(DEP_PUBLIC_PATH)
    data_file_name = OMICS_MUTATIONS_MATRIX

    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    try:
        print(f"Data refresh process started for {table_name}.")
        cursor.execute(drop_table_sql)
        pg_conn.commit()
        
        cursor.execute(table_create_sql)
        pg_conn.commit()
        print(f"DB table {table_name} has been created.")

        damaging_mutations = pd.read_csv(input_folder/data_file_name)

        csv_columns = damaging_mutations.columns
        genes = csv_columns.tolist()[1:]

        omic_gene_rows = fetch_data_from_db(im_omics_gene_select_sql)
        omic_gene_dic = {y: x for x, y in omic_gene_rows}

        chunksize = 10

        total_rows = 0
        for chunk in pd.read_csv(input_folder/data_file_name, chunksize=chunksize, delimiter=","):
            matrix = chunk.values
            insert_rows = []
            for row in matrix:
                cell_line = row[0]
                mutation_vals = np.delete(row,0)
                res = [(cell_line, omic_gene_dic.get(x), y) for x, y in zip(genes, mutation_vals)]
                insert_rows.extend(res)
            
            start_time = datetime.now()
            cursor.executemany(data_insert_sql, insert_rows)
            pg_conn.commit()
            end_time = datetime.now()
            print(f"Duration to insert {len(insert_rows)} rows: {(end_time - start_time).seconds} seconds")
            total_rows = total_rows + len(insert_rows)
            
        print(f"Total number of rows inserted to {table_name} = {total_rows}")
    except Exception as e:
        traceback.print_exc() 
        pg_conn.rollback()
    finally:
        pg_conn.commit()
        cursor.close()
        end_time_main = datetime.now()
        print(f"Duration to complete the refresh process for {table_name}: {(end_time_main - start_time_main).seconds} seconds")

# TASK_3:
#Solve S' for all entries in response-curve-parameters
#Table name -> im_sprime_solved_s_prime
def refresh_s_prime():
    start_time_main = datetime.now()
    table_name = "im_sprime_solved_s_prime"
    table_create_sql = im_sprime_solved_s_prime_table_sql
    data_insert_sql = im_sprime_solved_s_prime_insert_sql
    drop_table_sql = f"drop table if exists {table_name}"

    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    try:
        print(f"Data refresh process started for {table_name}.")
        
        cursor.execute(drop_table_sql)
        pg_conn.commit()
        
        cursor.execute(table_create_sql)
        pg_conn.commit()
        print(f"DB table {table_name} has been created.")

        secondary_raw_data = fetch_data_from_db(secondary_dose_curve_raw_select)
        
        total_rows = 0
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
            print(f"{len(insert_rows)} rows inserted into {table_name}")
            total_rows = total_rows + len(insert_rows)
        print(f"Total # of rows inserted into {table_name}: total_rows")
    except Exception as e:
        traceback.print_exc() 
        pg_conn.rollback()
    finally:
        pg_conn.commit()
        cursor.close()
        end_time_main = datetime.now()
        print(f"Duration to complete the refresh process for {table_name}: {(end_time_main - start_time_main).seconds} seconds")

#Task_4: Create a merged table that brings in Mutation Value by cell line (ACH-….)
#Table name -> im_sprime_s_prime_with_mutations
# This table will populate “All S' by Mutation and Tissue” section on web page.
# In this table, use the primary key of “im_sprime_solved_s_prime” table as foreign key. 
# This foreign key will give us the pre-calculated values. So we don’t need to explicitly save s’ calculations in this table. 
# We will fetch the full data by joining “im_sprime_s_prime_with_mutations” table with “im_sprime_solved_s_prime”. 
# If this join causes slowness in data retrieval then we will calculate and save all values in this table explicitly.
def refresh_mutations_by_cell_line(gene_id_list):
    start_time_main = datetime.now()
    table_name = "im_sprime_s_prime_with_mutations"
    table_create_sql = im_sprime_s_prime_with_mutations_table_sql
    data_insert_sql = im_sprime_s_prime_with_mutations_insert_sql
    drop_table_sql = f"drop table if exists {table_name}"

    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    try:
        print(f"Data refresh process started for {table_name}.")

        # 1) Drop existing table
        cursor.execute(drop_table_sql)
        pg_conn.commit()

        # 2) Create a new table
        cursor.execute(table_create_sql)
        pg_conn.commit()
        print(f"DB table {table_name} has been created.")


        # 3) Fetch s_prime_rows
        # (id, depmap_id, ccle_name)
        cursor.execute(im_sprime_solved_s_prime_select_sql)
        s_prime_rows = cursor.fetchall()

        tissue_names = []

        s_prime_tissue_dict = {}
        for s_prime_row in s_prime_rows:
            res = s_prime_row[2].split("_", 1)
            tissue_name = res[1] if len(res) > 1 else ""
            tissue_names.append(tissue_name)
            s_prime_tissue_dict[s_prime_row[0]] = tissue_name

        tissue_names = list(set(tissue_names))

        cell_lines = list(set([x[1] for x in s_prime_rows]))

        print(f"Total number of unique cell lines in solved_s_prime table: {len(cell_lines)}")
        print(f"Total number of unique tissues: {len(tissue_names)}")

    
        for cell_line_batch in batch(cell_lines, 5): 
            # 4) Fetch mutation values for all cell lines
            # row = (cell_line, gene_id, mutation_value)
            mutation_values_for_cell_lines = get_mutation_values_for_cell_lines(cell_line_batch)
            print(f"Total # of cell line mutation values for {cell_line_batch}: {len(mutation_values_for_cell_lines)}")
            cell_line_mutations_dict = {}
            for cell_line, gene_id, mutation_value in mutation_values_for_cell_lines:
                if cell_line in cell_line_mutations_dict.keys():
                    gene_mutation_values = cell_line_mutations_dict[cell_line]
                    gene_mutation_values.append((gene_id, mutation_value))
                    cell_line_mutations_dict[cell_line] = gene_mutation_values
                else:
                    cell_line_mutations_dict[cell_line] = [(gene_id, mutation_value)]

            print(f"cell_line_mutations_dict length: {len(cell_line_mutations_dict)}")

            # 5) Prepare insert rows for im_sprime_s_prime_with_mutations table
            # row = (s_prime_id, cell_line, tissue_name, gene_id, mutation_value)
            s_prime_with_mutations_rows = []

            # (id, depmap_id, ccle_name)
            for row in s_prime_rows:
                insert_rows = []
                # [(gene_id, mutation_value)..]
                res = row[2].split('_', 1)
                tissue = res[1] if len(res) > 1 else ""
                if row[1] in cell_line_batch:
                    mutation_values = cell_line_mutations_dict[row[1]]
                    for mut_val in mutation_values:
                        if mut_val[0] in gene_id_list:
                            insert_rows = [tuple([row[0], row[1], s_prime_tissue_dict[row[0]]]+list(mut_val))]
                            s_prime_with_mutations_rows.extend(insert_rows)

            print(f"Target cell lines: {cell_line_batch}")
            print(f"Target gene ids: {gene_id_list}")
            cursor.executemany(data_insert_sql, s_prime_with_mutations_rows)
            pg_conn.commit()
            print(f"Total number of rows inserted to {table_name} table: {len(s_prime_with_mutations_rows)}")
    except Exception as e:
        traceback.print_exc() 
        pg_conn.rollback()
    finally:
        pg_conn.commit()
        cursor.close()
        end_time_main = datetime.now()
        print(f"Duration to complete the refresh process for {table_name}: {(end_time_main - start_time_main).seconds} seconds")

# Task_5: Create the Pooled delta S' results table from 4 by applying these filters:
# 	- LUNG
# 	- NF1 (a single gene for now, later we will do all genes in LUNG)
# 	- HTSwithMTS010_Overlayed as the study (file should be 500MB)
# 	- Only include compounds where the size of ref lines >2 and size of test lines >2
# Table name -> fnl_sprime_pooled_delta_sprime
def refresh_pooled_delta_s_results(gene_id, tissue):
    start_time_main = datetime.now()
    table_name = "fnl_sprime_pooled_delta_sprime"
    table_create_sql = fnl_sprime_pooled_delta_sprime_table_sql
    data_insert_sql = fnl_sprime_pooled_delta_sprime_insert_sql
    drop_table_sql = f"drop table if exists {table_name}"

    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    try:
        # 1) Drop existing table
        cursor.execute(drop_table_sql)
        pg_conn.commit()

        # 2) Create a new table
        cursor.execute(table_create_sql)
        pg_conn.commit()
        print(f"DB table {table_name} has been created.")

        cursor.execute(names_for_tissue_select, ("%_"+tissue,))
        names_for_tissue = cursor.fetchall()

        print(f"names_for_tissue length = {len(names_for_tissue)}")

        pooled_delta_s_prime_dict = {}
        s_prime_name_vals_dict = {}
        for name_tissue_row_batch in batch(names_for_tissue, 100):
            s_prime_names = [row[0] for row in name_tissue_row_batch]
            formatted_s_prime_names = ', '.join(f"'{w.replace("'", "''")}'" for w in s_prime_names)
            cursor.execute(source_data_for_fnl_sprime_table.format(formatted_s_prime_names), (gene_id, tissue, "%_"+tissue))
            results = cursor.fetchall()
            print(f"results length = {len(results)}")
            
            # s_prime.name, mut.cell_line, s_prime.s_prime, mut.mutation_value
            for row in results:
                if row[0] in s_prime_name_vals_dict.keys():
                    current_val = s_prime_name_vals_dict[row[0]]
                    current_val.append(row)
                else:
                    s_prime_name_vals_dict[row[0]] = [row]
        print(f"s_prime_name_vals_dict length = {len(s_prime_name_vals_dict)}")
        
        for key, value in s_prime_name_vals_dict.items():
            ref_sprime_values = []
            test_sprime_values = []
            for v in value:
                s_prime = v[2]
                mutation_value = v[3]

                if mutation_value == 0:
                    ref_sprime_values.append(s_prime)
                else:
                    test_sprime_values.append(s_prime)

            if len(ref_sprime_values) > 0 and len(test_sprime_values) > 0:
                ref_pooled_s_prime = np.mean(ref_sprime_values) if len(ref_sprime_values) > 0 else 0
                test_pooled_s_prime = np.mean(test_sprime_values) if len(test_sprime_values) > 0 else 0

                pooled_delta_s_prime_dict[key] = (key, ref_pooled_s_prime, len(ref_sprime_values), test_pooled_s_prime, len(test_sprime_values), ref_pooled_s_prime - test_pooled_s_prime, gene_id, tissue)   

        print(f"pooled_delta_s_prime_dict length = {len(pooled_delta_s_prime_dict)}")
        insert_rows = list(pooled_delta_s_prime_dict.values())
        for rows_batch in batch(insert_rows, 250):
            cursor.executemany(data_insert_sql, rows_batch)
            pg_conn.commit()
        print(f"Total number of rows inserted to {table_name} table: {len(pooled_delta_s_prime_dict)}")
    except Exception as e:
        traceback.print_exc()
        pg_conn.rollback()
    finally:
        pg_conn.commit()
        cursor.close()
        end_time_main = datetime.now()
        print(f"Duration to complete the refresh process for {table_name}: {(end_time_main - start_time_main).seconds} seconds")


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
    execution_timeout=timedelta(seconds=900000),
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Set task dependencies
start >> refresh_data_task >> end