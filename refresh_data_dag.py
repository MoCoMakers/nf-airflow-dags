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
from scipy.stats import mannwhitneyu
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import utils  # Now this should work

_config = utils.get_config_data_refresh()

secondary_dose_curve_raw_table_sql = _config['sql']['secondary_dose_curve_raw_table_sql']

secondary_dose_curve_raw_insert_sql = _config['sql']['secondary_dose_curve_raw_insert_sql']

secondary_dose_curve_raw_select = _config['sql']['secondary_dose_curve_raw_select']

im_sprime_solved_s_prime_table_sql = _config['sql']['im_sprime_solved_s_prime_table_sql']

im_sprime_solved_s_prime_insert_sql = _config['sql']['im_sprime_solved_s_prime_insert_sql']

im_sprime_solved_s_prime_select_sql = _config['sql']['im_sprime_solved_s_prime_select_sql']

im_dep_sprime_damaging_mutations_table_sql = _config['sql']['im_dep_sprime_damaging_mutations_table_sql']
im_dep_sprime_damaging_mutations_insert_sql = _config['sql']['im_dep_sprime_damaging_mutations_insert_sql']

im_omics_gene_table_sql = _config['sql']['im_omics_gene_table_sql']
im_omics_gene_insert_sql = _config['sql']['im_omics_gene_insert_sql']
im_omics_gene_select_sql = _config['sql']['im_omics_gene_select_sql']

mutation_values_for_cell_lines = _config['sql']['mutation_values_for_cell_lines']

mutation_values_all = _config['sql']['mutation_values_all']
im_sprime_s_prime_with_mutations_table_sql = _config['sql']['im_sprime_s_prime_with_mutations_table_sql']
im_sprime_s_prime_with_mutations_insert_sql = _config['sql']['im_sprime_s_prime_with_mutations_insert_sql']

fnl_sprime_pooled_delta_sprime_table_sql = _config['sql']['fnl_sprime_pooled_delta_sprime_table_sql']

fnl_sprime_pooled_delta_sprime_insert_sql = _config['sql']['fnl_sprime_pooled_delta_sprime_insert_sql']

source_data_for_fnl_sprime_table = _config['sql']['source_data_for_fnl_sprime_table']

names_for_tissue_select = _config['sql']['names_for_tissue_select']

DEP_PRISM_PATH = _config['files_path']['DEP_PRISM_PATH']
DEP_PUBLIC_PATH = _config['files_path']['DEP_PUBLIC_PATH']

SEC_RESP_DOSE_CURVE = _config['files_path']['SEC_RESP_DOSE_CURVE']
OMICS_MUTATIONS_MATRIX = _config['files_path']['OMICS_MUTATIONS_MATRIX']

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
            formatted_s_prime_names = ', '.join(f"""'{w.replace("'", "''")}'""" for w in s_prime_names)
            cursor.execute(source_data_for_fnl_sprime_table.format(formatted_s_prime_names), (gene_id, tissue, "%_"+tissue))
            results = cursor.fetchall()
            print(f"results length = {len(results)}")
            
            # s_prime.name, mut.cell_line, s_prime.s_prime, s_prime.ec50, s_prime.auc, s_prime.moa, s_prime.target, mut.mutation_value
            for row in results:
                if row[0] in s_prime_name_vals_dict.keys():
                    current_val = s_prime_name_vals_dict[row[0]]
                    current_val.append(row)
                else:
                    s_prime_name_vals_dict[row[0]] = [row]
        print(f"s_prime_name_vals_dict length = {len(s_prime_name_vals_dict)}")
        
        # s_prime.name, mut.cell_line, s_prime.s_prime, s_prime.ec50, s_prime.auc, s_prime.moa, s_prime.target, mut.mutation_value
        for key, value in s_prime_name_vals_dict.items():
            ref_sprime_values = []
            test_sprime_values = []
            ref_auc_values = []
            test_auc_values = []
            ref_ec50_values = []
            test_ec50_values = []
            moa_values_set = set()
            target_values_set = set()

            for v in value:
                s_prime = v[2]
                ec50 = v[3]
                auc = v[4]
                moa = v[5]
                target = v[6]
                mutation_value = v[7]

                moa_values_set.add(moa)
                target_values_set.add(target)

                if mutation_value == 0:
                    ref_sprime_values.append(s_prime)
                    ref_ec50_values.append(ec50)
                    ref_auc_values.append(auc)
                else:
                    test_sprime_values.append(s_prime)
                    test_ec50_values.append(ec50)
                    test_auc_values.append(auc)

            if len(ref_sprime_values) > 0 and len(test_sprime_values) > 0:
                ref_pooled_s_prime = np.mean(ref_sprime_values) if len(ref_sprime_values) > 0 else 0
                test_pooled_s_prime = np.mean(test_sprime_values) if len(test_sprime_values) > 0 else 0
                ref_median_s_prime = np.median(ref_sprime_values) if len(ref_sprime_values) > 0 else 0
                test_median_s_prime = np.median(test_sprime_values) if len(test_sprime_values) > 0 else 0

                num_ref_lines = len(ref_sprime_values)
                num_test_lines = len(test_sprime_values) 
                delta_s_prime = ref_pooled_s_prime - test_pooled_s_prime

                ref_mad = median_absolute_deviation(ref_sprime_values) if len(ref_sprime_values) > 0 else 0
                test_mad = median_absolute_deviation(test_sprime_values) if len(test_sprime_values) > 0 else 0

                ref_pooled_auc = np.mean(ref_auc_values) if len(ref_auc_values) > 0 else 0
                test_pooled_auc = np.mean(test_auc_values) if len(test_auc_values) > 0 else 0

                ref_pooled_ec50 = np.mean(ref_ec50_values) if len(ref_ec50_values) > 0 else 0
                test_pooled_ec50 = np.mean(test_ec50_values) if len(test_ec50_values) > 0 else 0

                if len(ref_sprime_values) > 1:
                    ref_s_prime_variance = np.var(np.array(ref_sprime_values), ddof=1)
                else:
                    ref_s_prime_variance = 0.0  # or np.nan, depending on what you want

                if len(test_sprime_values) > 1:
                    test_s_prime_variance = np.var(np.array(test_sprime_values), ddof=1)
                else:
                    test_s_prime_variance = 0.0  # or np.nan, depending on what you want

                delta_auc = ref_pooled_auc - test_pooled_auc
                delta_ec50 = ref_pooled_ec50 - test_pooled_ec50

                
                delta_s_prime_median = ref_median_s_prime - test_median_s_prime

                moa = ','.join(str(s) for s in moa_values_set)
                target = ','.join(str(s) for s in target_values_set)
                

                # Calculate p-value using Mann-Whitney U test
                p_values = []
                stat, p_value = mannwhitneyu(ref_sprime_values, test_sprime_values, alternative='two-sided')
                p_values.append(p_value)
                    
                p_val_median_man_whit = p_values[0] if len(p_values) > 0 else 0

                # TODO
                group_sub = None
                
                sensitivity_score = 0
                sensitivity = 'Equivocal'
                if delta_s_prime < -0.5:
                    sensitivity_score = -1
                    sensitivity = 'Sensitive'
                else:
                    sensitivity_score = 1
                    sensitivity = 'Resistant'

                # name, ref_pooled_s_prime, ref_median_s_prime, ref_mad, ref_pooled_auc, ref_pooled_ec50, num_ref_lines, 
                # ref_s_prime_variance, test_pooled_s_prime, test_median_s_prime, test_mad, test_pooled_auc, test_pooled_ec50, 
                # num_test_lines, test_s_prime_variance, delta_s_prime, delta_auc, delta_ec50, 
                # delta_s_prime_median, p_val_median_man_whit, sensitivity_score, sensitivity, moa, 
                # target, group_sub, gene_id, tissue
                pooled_delta_s_prime_dict[key] = (key, ref_pooled_s_prime, ref_median_s_prime, ref_mad, ref_pooled_auc, ref_pooled_ec50, 
                num_ref_lines, ref_s_prime_variance, test_pooled_s_prime, test_median_s_prime, test_mad, test_pooled_auc, test_pooled_ec50, 
                num_test_lines, test_s_prime_variance, delta_s_prime, delta_auc, delta_ec50, 
                delta_s_prime_median, p_val_median_man_whit, sensitivity_score, sensitivity, moa, 
                target, group_sub, gene_id, tissue)  

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

def median_absolute_deviation(data):
        # Calculate the median of the data
        median = np.median(data)
        # Calculate the absolute deviations from the median
        abs_deviation = np.abs(data - median)
        # Compute the median of the absolute deviations
        mad = np.median(abs_deviation)
        return mad

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