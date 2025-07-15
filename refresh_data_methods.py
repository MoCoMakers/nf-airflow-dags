from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.log.logging_mixin import LoggingMixin
import traceback
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import time
import numpy as np
from collections import defaultdict
from scipy.stats import mannwhitneyu
import csv
import sys
import os
import logging
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import utils
import io

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

refresh_mutations_source_data_select = _config['sql']['refresh_mutations_source_data_select']

cell_damaging_mutations_select = _config['sql']['cell_damaging_mutations_select']
cell_lines_for_tissue_select = _config['sql']['cell_lines_for_tissue_select']

DEP_PRISM_PATH = _config['files_path']['dep_prism_path']
DEP_PUBLIC_PATH = _config['files_path']['dep_public_path']

SEC_RESP_DOSE_CURVE = _config['files_path']['sec_resp_dose_curve']
OMICS_MUTATIONS_MATRIX = _config['files_path']['omics_mutations_matrix']

pg_hook = PostgresHook(postgres_conn_id='COMP_BIO_HUB_NEW', schema='public')

# Define the logger at the module level
logger = logging.getLogger(__name__)  # This logger will be used across all functions

pg_conn = pg_hook.get_conn()


def fetch_data_from_db(select_sql, params=None):
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    if params:
        cursor.execute(select_sql, params)
    else:
        cursor.execute(select_sql)
    rows = cursor.fetchall()
    return rows

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
        logger.info(f"DB table {table_name} has been created.")

        damaging_mutations = pd.read_csv(input_folder/data_file_name)

        genes = damaging_mutations.columns.tolist()[1:]
        gene_tuples = [(x,) for x in genes]

        cursor.executemany(im_omics_gene_insert_sql, gene_tuples)
        pg_conn.commit()

        logger.info(f"Genes length: {len(genes)}")
    except Exception as e:
        traceback.print_exc() 
        pg_conn.rollback()
    finally:
        pg_conn.commit()
        cursor.close()



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
        logger.info(f"Data refresh process started for {table_name}.")
        cursor.execute(drop_table_sql)
        pg_conn.commit()

        cursor.execute(table_create_sql)
        pg_conn.commit()
        logger.info(f"DB table {table_name} has been created.")
        
        chunksize = 50000
        total = 0
        for chunk in pd.read_csv(input_folder / data_file_name, chunksize=chunksize):
            start_time_insert = datetime.now()
            rows = []
            for row in chunk.values:
                rows.append(tuple(list(row.flatten())))
            cursor.executemany(data_insert_sql, rows)
            pg_conn.commit()
            end_time_insert = datetime.now()
            logger.info(f"Duration to insert {len(chunk)} records: {(end_time_insert - start_time_insert).seconds} seconds")
            total = total + len(chunk.values)
            time.sleep(3)
        logger.info(f"Total number of records inserted to {table_name} table = {total}") 
        
    except Exception as e:
        logger.info(f"Error happened while refreshing {table_name} table.")
        traceback.print_exc()
        pg_conn.rollback()    
    finally:
        pg_conn.commit()
        cursor.close()
        end_time_main = datetime.now()
        logger.info(f"Duration to complete the refresh process for {table_name}: {(end_time_main - start_time_main).seconds} seconds")

# TASK_1:
#Load all dose-response-curve-parameters.csv from ~/nf_streamlit/app/data$
#Table name -> im_dep_raw_secondary_dose_curve
def refresh_secondary_dose_curve_copy_csv():
    start_time_main = datetime.now()
    
    table_name = "im_dep_raw_secondary_dose_curve"
    table_create_sql = secondary_dose_curve_raw_table_sql
    file_path = DEP_PRISM_PATH
    data_file_name = SEC_RESP_DOSE_CURVE
    drop_table_sql = f"drop table if exists {table_name}"
        
    input_folder = Path(file_path)
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    column_names = ['broad_id', 'depmap_id', 'ccle_name', 'screen_id', 'upper_limit', 'lower_limit', 
                    'slope', 'r2', 'auc', 'ec50', 'ic50', 'name', 'moa', 'target', 'disease_area', 
                    'indication', 'smiles', 'phase', 'passed_str_profiling', 'row_name']

    try:
        logger.info(f"Data refresh process started for {table_name}.")
        cursor.execute(drop_table_sql)
        pg_conn.commit()

        cursor.execute(table_create_sql)
        pg_conn.commit()
        logger.info(f"DB table {table_name} has been created.")
        
        df = pd.read_csv(input_folder/data_file_name)
        #column_names = df.columns.tolist()
        total_rows_inserted = 0
        logger.info(f"Number of rows in {data_file_name} = {len(df)}")
        # Create a StringIO object to write DataFrame as CSV
        
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)  # Rewind the StringIO object to the beginning
        #logger.info(f"Chunk has been copied to CSV.")


        # Use COPY FROM with the StringIO object
        with pg_conn.cursor() as cursor:
            cursor.copy_expert(
                f"COPY {table_name} ({', '.join(column_names)}) FROM STDIN WITH CSV",
                csv_buffer
            )
            pg_conn.commit()
        logger.info(f"Total number of records inserted to {table_name} table = {len(df)}") 
        
    except Exception as e:
        logger.info(f"Error happened while refreshing {table_name} table.")
        traceback.print_exc()
        pg_conn.rollback()    
    finally:
        pg_conn.commit()
        cursor.close()
        end_time_main = datetime.now()
        logger.info(f"Duration to complete the refresh process for {table_name}: {(end_time_main - start_time_main).seconds} seconds")


# TASK_2:
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
        logger.info(f"Data refresh process started for {table_name}.")
        
        cursor.execute(drop_table_sql)
        pg_conn.commit()
        
        cursor.execute(table_create_sql)
        pg_conn.commit()
        logger.info(f"DB table {table_name} has been created.")

        secondary_raw_data = fetch_data_from_db(secondary_dose_curve_raw_select)
        
        total_rows = 0
        for rows_batch in utils.batch(secondary_raw_data, 50000):
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
            logger.info(f"{len(insert_rows)} rows inserted into {table_name}.")
            total_rows = total_rows + len(insert_rows)
        logger.info(f"Total # of rows inserted into {table_name}: {total_rows}")
    except Exception as e:
        traceback.print_exc() 
        pg_conn.rollback()
    finally:
        pg_conn.commit()
        cursor.close()
        end_time_main = datetime.now()
        logger.info(f"Duration to complete the refresh process for {table_name}: {(end_time_main - start_time_main).seconds} seconds")


# TASK_2:
#Solve S' for all entries in response-curve-parameters
#Table name -> im_sprime_solved_s_prime
def refresh_s_prime_csv():
    start_time_main = datetime.now()
    table_name = "im_sprime_solved_s_prime"
    table_create_sql = im_sprime_solved_s_prime_table_sql
    drop_table_sql = f"drop table if exists {table_name}"

    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    try:
        logger.info(f"Data refresh process started for {table_name}.")
        
        cursor.execute(drop_table_sql)
        pg_conn.commit()
        
        cursor.execute(table_create_sql)
        pg_conn.commit()
        logger.info(f"DB table {table_name} has been created.")

        secondary_raw_data = fetch_data_from_db(secondary_dose_curve_raw_select)

        column_names = ['broad_id', 'depmap_id', 'ccle_name', 'screen_id', 'upper_limit', 'lower_limit', 
                        'slope', 'r2', 'auc', 'ec50', 'ic50', 'name', 'moa', 'target', 'disease_area', 'indication',
                        'smiles', 'phase', 'passed_str_profiling', 'row_name', 'eff', 'eff_100', 'eff_ec50', 's_prime']
        
        total_rows = 0
        for rows_batch in utils.batch(secondary_raw_data, 10000):
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
            
            df = pd.DataFrame(insert_rows, columns=column_names)
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, header=False)
            csv_buffer.seek(0)  # Rewind the StringIO object to the beginning

            # Use COPY FROM with the StringIO object
            with pg_conn.cursor() as cursor:
                cursor.copy_expert(
                    f"COPY {table_name} ({', '.join(column_names)}) FROM STDIN WITH CSV",
                    csv_buffer
                )
            pg_conn.commit()
            total_rows = total_rows + len(insert_rows)
            logger.info("Batch completed.")
        logger.info(f"Total # of rows inserted into {table_name}: {total_rows}")
    except Exception as e:
        traceback.print_exc() 
        pg_conn.rollback()
    finally:
        pg_conn.commit()
        cursor.close()
        end_time_main = datetime.now()
        logger.info(f"Duration to complete the refresh process for {table_name}: {(end_time_main - start_time_main).seconds} seconds")


# TASK_3:
#Load OmicsSomaticMutationsMatrixDamaging.csv from ~/nf_streamlit/app/data
#Table name -> im_dep_sprime_damaging_mutations
def refresh_damaging_mutations():
    start_time_main = datetime.now()
    table_name = "im_dep_sprime_damaging_mutations"
    table_create_sql = im_dep_sprime_damaging_mutations_table_sql
    #data_insert_sql = im_dep_sprime_damaging_mutations_insert_sql
    drop_table_sql = f"drop table if exists {table_name}"
    input_folder = Path(DEP_PUBLIC_PATH)
    data_file_name = OMICS_MUTATIONS_MATRIX

    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    try:
        logger.info(f"Data refresh process started for '{table_name}'.")
        cursor.execute(drop_table_sql)
        pg_conn.commit()
        
        cursor.execute(table_create_sql)
        pg_conn.commit()
        
        logger.info(f"Database table '{table_name}' has been created successfully.")

        gene_rows = fetch_data_from_db(im_omics_gene_select_sql)
        gene_name_id_dict = {name: id for id, name in gene_rows}

        # Step 1: Load CSV
        logger.info(f"Step 1: Load {data_file_name} file")
        df = pd.read_csv(input_folder/data_file_name)

        # Step 2: Rename first column to 'cell_line'
        logger.info("Step 2: Rename first column to 'cell_line'")
        df.rename(columns={df.columns[0]: "cell_line"}, inplace=True)

        # Step 3: Clean header names and map to gene_id
        logger.info("Step 3: Clean header names and map to gene_id")
        clean_column_map = {}
        for gene_name in df.columns[1:]:  # Skip 'cell_line'
            if gene_name in gene_name_id_dict.keys():
                clean_column_map[gene_name] = gene_name_id_dict[gene_name]
            else:
                raise ValueError(f"Gene name '{gene_name}' not found in gene_name_to_id dictionary.")
            
        
        # Step 4: Melt the dataframe to long format
        logger.info("Step 4: Melt the dataframe to long format")
        df_long = df.melt(id_vars="cell_line", var_name="gene_col", value_name="mutation_value")

        # Step 5: Convert 'mutation_value' to integer
        logger.info("Step 5: Convert mutation_value to integer")
        df_long["mutation_value"] = df_long["mutation_value"].astype(float).astype(int)

        # Step 6: Map gene_col to gene_id
        logger.info("Step 6: Map gene_col to gene_id")
        df_long["gene_id"] = df_long["gene_col"].map(clean_column_map)

        # Step 7: (Optional) Filter rows to include only specific mutation values
        logger.info("Step 7: (Optional) Filter rows to include only specific mutation values")
        df_long = df_long[df_long["mutation_value"].isin([0, 2])]

        # Step 8: Final DataFrame for DB insertion
        logger.info("Step 8: Final DataFrame for DB insertion")
        
        df_final = df_long[["cell_line", "gene_id", "mutation_value"]].copy()
        
        logger.info(f"Total number of rows that will be inserted into '{table_name}' table =  {df_final.shape[0]}")

        # Step 9: Write to CSV buffer
        logger.info("Step 9: Write to CSV buffer")
        csv_buffer = io.StringIO()
        df_final.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)

        # Step 10: Insert into PostgreSQL
        logger.info("Step 10: Insert into PostgreSQL")
        with pg_conn.cursor() as cursor:
            cursor.copy_expert(
                f"COPY {table_name} (cell_line, gene_id, mutation_value) FROM STDIN WITH CSV",
                csv_buffer
            )
        pg_conn.commit()

    except Exception as e:
        traceback.print_exc() 
        pg_conn.rollback()
    finally:
        pg_conn.commit()
        cursor.close()
        end_time_main = datetime.now()
        logger.info(f"Completed refresh process for table '{table_name}' in {(end_time_main - start_time_main).seconds} seconds.")


def refresh_s_prime_mutations_for_tissue(tissue, gene_id_start, gene_id_max, gene_id_increment):
    start_time = datetime.now()
    logger.info(f"refresh_s_prime_mutations started for tissue={tissue}")

    table_name = "im_sprime_s_prime_with_mutations"

    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    try:
        start_id = gene_id_start
        logger.info(f"gene_id_start={gene_id_start}, gene_id_max={gene_id_max}, gene_id_increment={gene_id_increment}")
        table_columns = ['cell_line', 'gene_id', 'mutation_value', 's_prime_id', 'tissue']
        while start_id <= gene_id_max:
            end_id = min(start_id + gene_id_increment - 1, gene_id_max)
            logger.info(f"Started for gene ids between [{start_id} - {end_id}].")
            sprime_mutation_rows = fetch_data_from_db(refresh_mutations_source_data_select, (tissue, start_id, end_id, f"%_{tissue}"))
            save_in_chunks_list(tissue, table_name, table_columns, sprime_mutation_rows)
            start_id = start_id + gene_id_increment
    except Exception as e:
        traceback.print_exc() 
    finally:
        cursor.close()
        end_time = datetime.now()
        logger.info(f"Completed in {(end_time - start_time).seconds} seconds")


def refresh_s_prime_mutations(load_type):
    gene_id_start = 1
    gene_id_max = 18916
    gene_id_increment = 10
    # Access and split the list
    tissues = _config['data']['tissues']
    tissue_list = tissues.split(', ')
    logger.info(f"Tissues: {tissue_list}, type: {type(tissue_list)}")

    table_name = "im_sprime_s_prime_with_mutations"

    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    try:
        # If load type is not incremental, table will be recreated.
        if load_type == "INCREMENTAL":
            logger.info(f"Data load type '{load_type}' detected; the database table(s) will not be recreated.")
                
        # INITIAL
        else:
            logger.info(f"Data load type '{load_type}' detected; the database table(s) will be recreated.")
                
            # 1) Drop existing table(s)
            cursor.execute(f"drop table if exists {table_name}")
            pg_conn.commit()

            # 2) Create a new table
            cursor.execute(im_sprime_s_prime_with_mutations_table_sql)
            pg_conn.commit()
            logger.info(f"DB table {table_name} has been created.")
        for tissue in tissue_list:
            refresh_s_prime_mutations_for_tissue(tissue, gene_id_start, gene_id_max, gene_id_increment)
    except Exception as e:
        traceback.print_exc() 
    finally:
        cursor.close()

def refresh_pooled_s_prime_mutations(load_type):
    gene_id_start = 1
    gene_id_max = 18916
    gene_id_increment = 10
    # Access and split the list
    tissues = _config['data']['tissues']
    tissue_list = tissues.split(', ')
    logger.info(f"Tissues: {tissue_list}, type: {type(tissue_list)}")

    table_name = "fnl_sprime_pooled_delta_sprime"

    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    try:
        # If load type is not incremental, table will be recreated.
        if load_type == "INCREMENTAL":
            logger.info(f"Data load type '{load_type}' detected; the database table(s) will not be recreated.")
                
        # INITIAL
        else:
            logger.info(f"Data load type '{load_type}' detected; the database table(s) will be recreated.")
                
            # 1) Drop existing table(s)
            cursor.execute(f"drop table if exists {table_name}")
            pg_conn.commit()

            # 2) Create a new table
            cursor.execute(fnl_sprime_pooled_delta_sprime_table_sql)
            pg_conn.commit()
            logger.info(f"DB table {table_name} has been created.")
        
        solved_prime_query = """SELECT id AS s_prime_id, depmap_id, ccle_name, name, s_prime, ec50, auc, moa, target
                                FROM im_sprime_solved_s_prime WHERE ccle_name LIKE %s order by depmap_id"""
        for tissue in tissue_list:
            solved_prime_df = pd.read_sql(solved_prime_query, pg_conn, params=(f"%_{tissue}",))
            refresh_pooled_s_prime_mutations_for_tissue(tissue, gene_id_start, gene_id_max, gene_id_increment, solved_prime_df)
    except Exception as e:
        traceback.print_exc() 
    finally:
        cursor.close()

def refresh_pooled_s_prime_mutations_for_tissue(tissue, gene_id_start, gene_id_max, gene_id_increment, solved_prime_df):
    start_time = datetime.now()
    logger.info(f"refresh_pooled_s_prime_mutations started for tissue={tissue}")

    table_name = "fnl_sprime_pooled_delta_sprime"

    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    try:
        start_id = gene_id_start

        # Fetch mutation data
        mutation_query = """SELECT cell_line, gene_id, mutation_value FROM im_dep_sprime_damaging_mutations WHERE gene_id BETWEEN %s AND %s  order by gene_id"""
       
        prep_columns = ["gene_id", "mutation_value", "name", "s_prime", "auc", "ec50", "cell_line", "moa", "target"]

       
        logger.info(f"gene_id_start={gene_id_start}, gene_id_max={gene_id_max}, gene_id_increment={gene_id_increment}")
        while start_id <= gene_id_max:
            end_id = min(start_id + gene_id_increment - 1, gene_id_max)
            logger.info(f"Started for gene ids between [{start_id} - {end_id}].")
            sprime_mutation_rows = fetch_data_from_db(mutation_query, (start_id, end_id))
            mutation_df = pd.DataFrame(sprime_mutation_rows, columns=["cell_line", "gene_id", "mutation_value"])
            merged_df = pd.merge(mutation_df, solved_prime_df, left_on="cell_line", right_on="depmap_id", how="left")
            sprime_pooled_df = prepare_pooled_delta_s_results(merged_df[prep_columns], tissue)
            save_in_chunks_df(tissue, table_name, sprime_pooled_df)
            start_id = start_id + gene_id_increment
    except Exception as e:
        traceback.print_exc() 
    finally:
        cursor.close()
        end_time = datetime.now()
        logger.info(f"Completed in {(end_time - start_time).seconds} seconds")


def save_in_chunks_df(tissue, table, data_df, chunk_size=100000):
    total_rows_inserted = 0

    #logger.info(f"Chunk Size = {chunk_size}")

    column_names = data_df.columns.tolist()
    for i, start in enumerate(range(0, len(data_df), chunk_size)):
        end = start + chunk_size
        chunk = data_df.iloc[start:end]
        
        # Create a StringIO object to write DataFrame as CSV
        csv_buffer = io.StringIO()
        data_df.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)  # Rewind the StringIO object to the beginning
        #logger.info(f"Chunk has been copied to CSV.")


        # Use COPY FROM with the StringIO object
        with pg_conn.cursor() as cursor:
            cursor.copy_expert(
                f"COPY {table} ({', '.join(column_names)}) FROM STDIN WITH CSV",
                csv_buffer
            )
            pg_conn.commit()
        total_rows_inserted += len(chunk)
    logger.info(f"Total number of records inserted into '{table}' table for tissue={tissue} = {total_rows_inserted}")


def save_in_chunks_list(tissue, table, columns, data_list, chunk_size=100000):
    total_rows_inserted = 0

    #logger.info(f"Chunk Size = {chunk_size}")

    for i, start in enumerate(range(0, len(data_list), chunk_size)):
        end = start + chunk_size
        chunk = data_list[start:end]
        
        # Write chunk to CSV buffer using csv.writer
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerows(chunk)
        csv_buffer.seek(0)

        # Use COPY FROM with the StringIO object
        with pg_conn.cursor() as cursor:
            cursor.copy_expert(
                f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH CSV",
                csv_buffer
            )
            pg_conn.commit()
        total_rows_inserted += len(chunk)
    logger.info(f"Total number of records inserted into '{table}' table for tissue={tissue} = {total_rows_inserted}")


def median_absolute_deviation(data):
        # Calculate the median of the data
        median = np.median(data)
        # Calculate the absolute deviations from the median
        abs_deviation = np.abs(data - median)
        # Compute the median of the absolute deviations
        mad = np.median(abs_deviation)
        return mad

def fetch_df(file, **kwargs):
    data_path = Path(file)
    return pd.read_csv(data_path, **kwargs)


def load_cell_damaging_mutations_from_db(tissue_cell_lines, gene_id_start, gene_id_end):
    cursor = pg_conn.cursor()
    try:
        cursor.execute(cell_damaging_mutations_select.format(tissue_cell_lines), (gene_id_start, gene_id_end))
        damaging_mutations = cursor.fetchall()
        return damaging_mutations
    except Exception as e:
        traceback.print_exc() 
        pg_conn.rollback()
    finally:
        pg_conn.commit()
        cursor.close()
    

# Task_5: Create the Pooled delta S' results table
# gene_id, mutation_value, name, s_prime, auc, ec50, cell_line, moa, target
def prepare_pooled_delta_s_results(source_df, tissue):
    df_ref_group = source_df.loc[source_df['mutation_value'] == 0]
    df_test_group = source_df.loc[source_df['mutation_value'] == 2]

    # Reference group calculations
    compounds_ref_agg_mean = summarize_group(df_ref_group, 'ref')

    # Test group calculations
    compounds_test_agg_mean = summarize_group(df_test_group, 'test')

    # Merging reference and test data
    compounds_merge = pd.merge(compounds_ref_agg_mean, compounds_test_agg_mean, on=['name', 'gene_id'], how='inner')

    # Calculating deltas
    compounds_merge['delta_s_prime'] = compounds_merge['ref_pooled_s_prime'] - compounds_merge['test_pooled_s_prime']
    compounds_merge['delta_auc'] = compounds_merge['ref_pooled_auc'] - compounds_merge['test_pooled_auc']
    compounds_merge['delta_ec50'] = compounds_merge['ref_pooled_ec50'] - compounds_merge['test_pooled_ec50']

    # Additional calculations for median differences
    compounds_merge['delta_s_prime_median'] = compounds_merge['ref_median_s_prime'] - compounds_merge['test_median_s_prime']

    ref_groups = defaultdict(list)
    test_groups = defaultdict(list)
    for (name, gene_id), group in df_ref_group.groupby(['name', 'gene_id']):
        ref_groups[(name, gene_id)] = group['s_prime'].values

    for (name, gene_id), group in df_test_group.groupby(['name', 'gene_id']):
        test_groups[(name, gene_id)] = group['s_prime'].values

    # Compute p-values
    p_values = []
    for _, row in compounds_merge.iterrows():
        key = (row['name'], row['gene_id'])
        group1 = ref_groups.get(key, [])
        group2 = test_groups.get(key, [])
        if len(group1) > 0 and len(group2) > 0:
            stat, p_value = mannwhitneyu(group1, group2, alternative='two-sided')
        else:
            p_value = np.nan
        p_values.append(p_value)

    compounds_merge['p_val_median_man_whit'] = p_values
        
    # Sensitivity calculations
    conditions = [
        compounds_merge['delta_s_prime'] < -0.5,
        compounds_merge['delta_s_prime'] > 0.5
    ]
    choices_score = [-1, 1]
    choices_label = ['Sensitive', 'Resistant']

    compounds_merge['sensitivity_score'] = np.select(conditions, choices_score, default=0)
    compounds_merge['sensitivity'] = np.select(conditions, choices_label, default='Equivocal')
    
    # Merging drug MOA information
    df_drug_moa = source_df[["name", "moa", "target"]]
    df_drug_moa_unique = df_drug_moa.drop_duplicates(subset=['name'])
    compounds_merge = pd.merge(compounds_merge, df_drug_moa_unique, on='name', how='left')

    # Formatting MOA
    compounds_merge['moa'] = compounds_merge['moa'].apply(lambda x: x.split(",") if isinstance(x, str) else [str(x)])

    compounds_merge["tissue"] = tissue

    #logger.info(f"Pool data columns: {compounds_merge.columns}")
    #logger.info(f"Pool data row count: {compounds_merge.shape[0]}")

    return compounds_merge

def summarize_group(df, prefix):
    return df.groupby(['name', 'gene_id']).agg(
        **{f"{prefix}_pooled_s_prime": pd.NamedAgg(column='s_prime', aggfunc='mean'),
           f"{prefix}_median_s_prime": pd.NamedAgg(column='s_prime', aggfunc='median'),
           f"{prefix}_mad": pd.NamedAgg(column='s_prime', aggfunc=median_absolute_deviation),
           f"{prefix}_pooled_auc": pd.NamedAgg(column='auc', aggfunc='mean'),
           f"{prefix}_pooled_ec50": pd.NamedAgg(column='ec50', aggfunc='mean'),
           f"num_{prefix}_lines": pd.NamedAgg(column='cell_line', aggfunc='count'),
           f"{prefix}_s_prime_variance": pd.NamedAgg(column='s_prime', aggfunc='var')}
    ).reset_index()


def compute_pval(row):
    if len(row['ref_s_prime']) > 0 and len(row['test_s_prime']) > 0:
        return mannwhitneyu(row['ref_s_prime'], row['test_s_prime'], alternative='two-sided').pvalue
    return np.nan


def create_index(index_name, table_name, fields):
    start_time = datetime.now()
    
    drop_index_sql = f"DROP INDEX IF EXISTS {index_name}"
    create_index_sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({fields})"

    try:
        logger.info(f"Indexing {table_name} table.")
        # Create a cursor object
        cursor = pg_conn.cursor()

        cursor.execute(drop_index_sql)
        pg_conn.commit()
        logger.info(f"Index {index_name} has been dropped.")

        cursor.execute(create_index_sql)
        pg_conn.commit()
        logger.info(f"Index {index_name} has been created for fields {fields} .")
        
    except Exception as e:
        traceback.print_exc() 
    finally:
        end_time = datetime.now()
        cursor.close()
        logger.info(f"Duration to complete the process: {(end_time - start_time).seconds} seconds")