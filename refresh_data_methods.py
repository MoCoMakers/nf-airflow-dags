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

pg_hook = PostgresHook(postgres_conn_id='Comp_Bio_Hub_Postgres', schema='public')

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
            #logger.info(f"Duration to insert {len(chunk)} records: {(end_time_insert - start_time_insert).seconds} seconds")
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
            
            cursor.executemany(data_insert_sql, insert_rows)
            pg_conn.commit()
            #logger.info(f"{len(insert_rows)} rows inserted into {table_name}")
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


def refresh_s_prime_mutations(tissue, load_type, gene_id_start, gene_id_max, gene_id_increment):
    start_time = datetime.now()
    logger.info(f"refresh_s_prime_mutations_data_efficient started for tissue={tissue}")

    mutations_table_name = "im_sprime_s_prime_with_mutations"
    mutations_table_create_sql = im_sprime_s_prime_with_mutations_table_sql
    mutations_drop_table_sql = f"drop table if exists {mutations_table_name}"

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
            cursor.execute(mutations_drop_table_sql)
            pg_conn.commit()

            # 2) Create a new table
            cursor.execute(mutations_table_create_sql)
            pg_conn.commit()
            logger.info(f"DB table {mutations_table_name} has been created.")

        # Fetch s_prime_solved_df (small lookup table)
        s_prime_solved_data = fetch_data_from_db(im_sprime_solved_s_prime_select_sql, (f"%_{tissue}",))
        s_prime_solved_df = pd.DataFrame(s_prime_solved_data, columns=["s_prime_id", "depmap_id"])
        #s_prime_solved_df.set_index("depmap_id", inplace=True, drop=False)

        cursor.execute(cell_lines_for_tissue_select, (f"%_{tissue}",))
        tissue_cell_lines = cursor.fetchall()
        logger.info(f"Total number of cell lines for tissue={tissue}: {len(tissue_cell_lines)}")

        formatted_cell_lines = ', '.join(f"'{w[0]}'" for w in tissue_cell_lines)

        start_id = gene_id_start

        logger.info(f"gene_id_start={gene_id_start}, gene_id_max={gene_id_max}, gene_id_increment={gene_id_increment}")
        while start_id <= gene_id_max:
            end_id = (start_id + gene_id_increment) - 1
            logger.info(f"Started for gene ids between [{start_id} - {end_id}].")
            damaging_mutations_rows = load_cell_damaging_mutations_from_db(formatted_cell_lines, start_id, end_id)
            #logger.info(f"Total number of mutation rows for tissue={tissue} and gene ids between [{start_id} - {end_id}]: {len(damaging_mutations_rows)}")
            damaging_mutations_df = pd.DataFrame(damaging_mutations_rows, columns=["cell_line", "gene_id", "mutation_value"])
            merge_in_chunks(tissue, damaging_mutations_df, s_prime_solved_df)
            start_id = start_id + gene_id_increment
    except Exception as e:
        traceback.print_exc() 
    finally:
        cursor.close()
        end_time = datetime.now()
        logger.info(f"Completed in {(end_time - start_time).seconds} seconds")


def refresh_s_prime_mutations_sql(tissue, load_type, gene_id_start, gene_id_max, gene_id_increment):
    start_time = datetime.now()
    logger.info(f"refresh_s_prime_mutations_sql started for tissue={tissue}")

    mutations_table_name = "im_sprime_s_prime_with_mutations"
    mutations_table_create_sql = im_sprime_s_prime_with_mutations_table_sql
    mutations_drop_table_sql = f"drop table if exists {mutations_table_name}"

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
            cursor.execute(mutations_drop_table_sql)
            pg_conn.commit()

            # 2) Create a new table
            cursor.execute(mutations_table_create_sql)
            pg_conn.commit()
            logger.info(f"DB table {mutations_table_name} has been created.")

        start_id = gene_id_start

        logger.info(f"gene_id_start={gene_id_start}, gene_id_max={gene_id_max}, gene_id_increment={gene_id_increment}")
        while start_id <= gene_id_max:
            end_id = (start_id + gene_id_increment) - 1
            logger.info(f"Started for gene ids between [{start_id} - {end_id}].")
            sprime_mutation_rows = fetch_data_from_db(refresh_mutations_source_data_select, (tissue, start_id, end_id, f"%_{tissue}"))
            save_in_chunks(tissue, sprime_mutation_rows)
            start_id = start_id + gene_id_increment
    except Exception as e:
        traceback.print_exc() 
    finally:
        cursor.close()
        end_time = datetime.now()
        logger.info(f"Completed in {(end_time - start_time).seconds} seconds")

def save_in_chunks(tissue, sprime_mutation_data, chunk_size=50000):
    total_rows_inserted = 0

    logger.info(f"Chunk Size = {chunk_size}")
    # Split large DataFrame into smaller chunks

    for i, start in enumerate(range(0, len(sprime_mutation_data), chunk_size)):
        logger.info(f"Processing chunk {i}")
        end = start + chunk_size
        chunk = sprime_mutation_data[start:end]
        
        # Write chunk to CSV buffer using csv.writer
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerows(chunk)
        csv_buffer.seek(0)

        # Use COPY FROM with the StringIO object
        with pg_conn.cursor() as cursor:
            # 'cell_line', 'gene_id', 'mutation_value', 's_prime_id', 'tissue'
            cursor.copy_expert(
                "COPY im_sprime_s_prime_with_mutations (cell_line, gene_id, mutation_value, s_prime_id, tissue) FROM STDIN WITH CSV",
                csv_buffer
            )
            pg_conn.commit()
        total_rows_inserted += len(chunk)
    logger.info(f"Total number of records inserted into DB for tissue={tissue} = {total_rows_inserted}")


def merge_in_chunks(tissue, cell_line_mutations_df, s_prime_solved_df, chunk_size=50000):
    total_rows_inserted = 0

    logger.info(f"Chunk Size = {chunk_size}")
    # Split large DataFrame into smaller chunks

    #for start in range(0, len(cell_line_mutations_df), chunk_size):  
    for i, start in enumerate(range(0, len(cell_line_mutations_df), chunk_size)):
        logger.info(f"Processing chunk {i}")
        end = start + chunk_size
        chunk = cell_line_mutations_df.iloc[start:end]


        # Merge chunk with reference DataFrame
        chunk_merged = pd.merge(chunk, s_prime_solved_df, left_on="cell_line", right_on="depmap_id", how="left")

        # Drop unnecessary columns
        chunk_merged = chunk_merged.drop(columns=["depmap_id"])

        chunk_merged["tissue"] = tissue

        # Convert mutation_value to integer
        chunk_merged["mutation_value"] = chunk_merged["mutation_value"].astype(float).astype(int)

        # Create a StringIO object to write DataFrame as CSV
        csv_buffer = io.StringIO()
        chunk_merged.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)  # Rewind the StringIO object to the beginning

        # Use COPY FROM with the StringIO object
        with pg_conn.cursor() as cursor:
            # 'cell_line', 'gene_id', 'mutation_value', 's_prime_id', 'tissue'
            cursor.copy_expert(
                "COPY im_sprime_s_prime_with_mutations (cell_line, gene_id, mutation_value, s_prime_id, tissue) FROM STDIN WITH CSV",
                csv_buffer
            )
            pg_conn.commit()
        total_rows_inserted = total_rows_inserted + chunk_merged.shape[0]
    logger.info(f"Total number of records inserted into DB for tissue={tissue} = {total_rows_inserted}")


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