import psycopg2
import traceback
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
import csv
import sys
import os
import logging
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import utils
import io

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]  # <- Important
)

logger = logging.getLogger(__name__)  # This logger will be used across all functions

_config = utils.get_config_data_refresh()

gene_mutation_data_counts_table_sql = _config['sql']['gene_mutation_data_counts_table_sql']
gene_mutation_data_counts_insert_sql = _config['sql']['gene_mutation_data_counts_insert_sql']

im_sprime_s_prime_with_mutations_insert_sql = _config['sql']['im_sprime_s_prime_with_mutations_insert_sql']

source_data_for_fnl_sprime_table = _config['sql']['source_data_for_fnl_sprime_table']

fnl_sprime_pooled_delta_sprime_select = _config['sql']['fnl_sprime_pooled_delta_sprime_select']

sprime_mutations_for_gene_and_tissue = _config['sql']['sprime_mutations_for_gene_and_tissue']

im_omics_gene_select_sql = _config['sql']['im_omics_gene_select_sql']

refresh_mutations_source_data_select = _config['sql']['refresh_mutations_source_data_select']

cell_damaging_mutations_select = _config['sql']['cell_damaging_mutations_select']
cell_lines_for_tissue_select = _config['sql']['cell_lines_for_tissue_select']
im_sprime_solved_s_prime_select_sql = _config['sql']['im_sprime_solved_s_prime_select_sql']
im_sprime_s_prime_with_mutations_table_sql = _config['sql']['im_sprime_s_prime_with_mutations_table_sql']


DEP_PRISM_PATH = "/home/gatlay/nf_streamlit/app/data/DepMap/Prism19Q4"
DEP_PUBLIC_PATH = "/home/gatlay/nf_streamlit/app/data/DepMap/Public24Q2"

postgres_host = "XXXXX"
postgres_name = "XXXXX"
postgres_user = "XXXXX"
postgres_password = "XXXXX"

pg_conn = psycopg2.connect(
        host=postgres_host,
        database=postgres_name,
        user=postgres_user,
        password=postgres_password
)

def fetch_df(file, **kwargs):
    data_path = Path(file)
    return pd.read_csv(data_path, **kwargs)

def build_df(*args, **kwargs):
    # Load the data
    df = fetch_df(*args, **kwargs)
    
    return df

def fetch_data_from_db(select_sql, params=None):
    start_time = datetime.now()
    cursor = pg_conn.cursor()
    if params:
        cursor.execute(select_sql, params)
    else:
        cursor.execute(select_sql)
    rows = cursor.fetchall()
    end_time = datetime.now()
    print(f"Record count = {len(rows)}, {(end_time - start_time).seconds} seconds")
    return rows


# Steps to compare CSV data with im_sprime_s_prime_with_mutations table data:
# 1) Download "All S' by Mutation and Tissue” data from web tool
# 2) Put the downloaded CSV file into the folder where you are running this code.
# 3) Run this function and check console logs to see the row count comparisons, and number of items that don't match.
def qa_verify_im_sprime_s_prime_with_mutations_table(data_file_name, gene_id, tissue_name):
    df = build_df(data_file_name)
    r, c = df.shape
    logger.info(f"CSV row count = {r}")   

    df_records = df.to_dict('records')
    
    # name	moa	target	lower_limit	upper_limit	ec50	auc	row_name	screen_id	
    # EFF	EFF*100	EFF/EC50	S'	ccle	tissue	NF1 (4763)	group_sub
    df_dict = {}
    for item in df_records:
        pair_key = (item['name'], item['row_name'])
        if pair_key in df_dict.keys():
            current_val = df_dict[pair_key]
            current_val.append(item)
            df_dict[pair_key] = current_val
        else:
            df_dict[pair_key] = [item]

    db_data = fetch_data_from_db(sprime_mutations_for_gene_and_tissue, (gene_id, tissue_name))

    #logger.info(f"df_dict length = {len(df_dict)}")

    logger.info(f"DB data has been fetched. Total rows: {len(db_data)}")
    db_items_not_matching_with_csv = []
    for row in db_data:
        name = row[0]
        row_name = row[7]
        s_prime = row[12]
        key_pair = (name, row_name)
        df_vals = df_dict[key_pair]
        match_found = False
        for df_val in df_vals:
            if not match_found:
                df_s_prime = df_val["S'"]
                if df_s_prime == s_prime:
                    match_found = True
                else:
                    decimal_part_1 = str(df_s_prime).split(".")[1][:9]
                    decimal_part_2 = str(s_prime).split(".")[1][:9]
                    if decimal_part_1 == decimal_part_2:
                        match_found = True
            
        if not match_found:
            db_items_not_matching_with_csv.append(row)       

    logger.info(f"DB row count = {len(db_data)}")
    logger.info(f"db_items_not_matching_with_csv length = {len(db_items_not_matching_with_csv)}")

# Steps to compare CSV data with fnl_sprime_pooled_delta_sprime table data:
# 1) Download 'Pooled Delta S' for Selected Values' data from web tool
# 2) Put the downloaded CSV file into the folder where you are running this code.
# 3) Run this function and check console logs to see the row count comparisons, and number of items that don't match.
def qa_verify_fnl_sprime_pooled_delta_sprime(data_file_name, gene_id, tissue_name):
     # extracting only columns: 'name', 'ref_pooled_s_prime', 'num_ref_lines', 'test_pooled_s_prime', 'num_test_lines', 'delta_s_prime'
    column_order = ['name', 'ref_pooled_s_prime', 'num_ref_lines', 'test_pooled_s_prime', 'num_test_lines', 'delta_s_prime']
    df = build_df(data_file_name, usecols=column_order)
    df = df[column_order]
    r, c = df.shape
    logger.info(f"CSV row count = {r}")
    df_records = df.to_dict('records')
    df_dict = {}
    for item in df_records:
        df_dict[item['name']] = item

    #logger.info(f"df_dict length = {len(df_dict)}")
    
    
    db_data = fetch_data_from_db(fnl_sprime_pooled_delta_sprime_select, (gene_id, tissue_name))

    logger.info(f"DB row count = {len(db_data)}")

    names_not_in_csv = []

    no_match_ref_lines_items = []
    no_match_test_lines_items = []
    # name, (df_val, db_val)
    no_match_ref_pooled_s_prime_items = {}
    # name, (df_val, db_val)
    no_match_test_pooled_s_prime_items = {}
    # name, (df_val, db_val)
    no_match_delta_s_prime_items = {}
    
    #name, ref_pooled_s_prime, num_ref_lines, test_pooled_s_prime, num_test_lines, delta_s_prime
    for row in db_data:
        name = row[0]
        if name not in df_dict.keys():
            names_not_in_csv.append(name)

        ref_pooled_s_prime = row[1]
        num_ref_lines = row[2]
        test_pooled_s_prime = row[3]
        num_test_lines = row[4]
        delta_s_prime = row[5]
        
        df_values = df_dict[name]
        ref_pooled_s_prime_df = df_values['ref_pooled_s_prime']
        num_ref_lines_df = df_values['num_ref_lines']
        test_pooled_s_prime_df = df_values['test_pooled_s_prime']
        num_test_lines_df = df_values['num_test_lines']
        delta_s_prime_df = df_values['delta_s_prime']
        
        if num_ref_lines != num_ref_lines_df or num_test_lines != num_test_lines_df:
            no_match_ref_lines_items.append(name)
        
        if num_test_lines != num_test_lines_df:
            no_match_test_lines_items.append(name)

        if test_pooled_s_prime != test_pooled_s_prime_df:
            a = str(test_pooled_s_prime_df).split(".")[1][:12]
            b = str(test_pooled_s_prime).split(".")[1][:12]
            if(a != b):
                no_match_test_pooled_s_prime_items[name] = (test_pooled_s_prime_df, test_pooled_s_prime)

        if ref_pooled_s_prime != ref_pooled_s_prime_df:
            a = str(ref_pooled_s_prime_df).split(".")[1][:12]
            b = str(ref_pooled_s_prime).split(".")[1][:12]
            if(a != b):
                no_match_ref_pooled_s_prime_items[name] = (ref_pooled_s_prime_df, ref_pooled_s_prime)

        if delta_s_prime_df != delta_s_prime:
            a = str(delta_s_prime_df).split(".")[1][:12]
            b = str(delta_s_prime).split(".")[1][:12]
            if(a != b):
                no_match_delta_s_prime_items[name] = (delta_s_prime_df, delta_s_prime)

    logger.info(f"names_not_in_csv length = {len(names_not_in_csv)}")
    logger.info(f"names_not_in_csv = {names_not_in_csv}")

    logger.info(f"no_match_ref_lines_items length = {len(no_match_ref_lines_items)}")
    logger.info(f"no_match_test_lines_items length = {len(no_match_test_lines_items)}")

    logger.info(f"no_match_ref_pooled_s_prime_items length = {len(no_match_ref_pooled_s_prime_items)}")
    logger.info(f"no_match_test_pooled_s_prime_items length = {len(no_match_test_pooled_s_prime_items)}")

    logger.info(f"no_match_delta_s_prime_items length = {len(no_match_delta_s_prime_items)}")

    return df

def median_absolute_deviation(data):
        # Calculate the median of the data
        median = np.median(data)
        # Calculate the absolute deviations from the median
        abs_deviation = np.abs(data - median)
        # Compute the median of the absolute deviations
        mad = np.median(abs_deviation)
        return mad

def reindex_table(index_name):
    start_time = datetime.now()
    try:
        cursor = pg_conn.cursor()
        cursor.execute(f"REINDEX INDEX {index_name}")
        pg_conn.commit()
        logger.info(f"Index {index_name} has been reindexed.")
        
    except Exception as e:
        traceback.print_exc() 
    finally:
        end_time = datetime.now()
        cursor.close()
        logger.info(f"Duration to complete the process: {(end_time - start_time).seconds} seconds")


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



def refresh_data_counts(tissue, source_table_name, gene_id_start, gene_id_max, increment, load_type):
    logger.info(f"Increment = {increment}")
    start_time = datetime.now()
    cursor = pg_conn.cursor()
    total_data = 0

    data_type = None
    if source_table_name == 'im_sprime_s_prime_with_mutations':
        data_type = 'SPRIME_MUTATION'
    elif source_table_name == 'fnl_sprime_pooled_delta_sprime':
        data_type = 'SPRIME_POOL'


    table_name = "im_gene_mutation_data_counts"
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
            cursor.execute(gene_mutation_data_counts_table_sql)
            pg_conn.commit()
            logger.info(f"DB table {table_name} has been created.")

        
        start_id = gene_id_start
        while start_id <= gene_id_max:
            insert_rows = []
            end_id = min(start_id + increment - 1, gene_id_max)

            query = f"""
                SELECT '{data_type}', gene_id, %s, COUNT(*)
                FROM {source_table_name}
                WHERE gene_id between %s and %s AND tissue = %s
                GROUP BY gene_id
            """

            cursor.execute(query, (tissue, start_id, end_id, tissue))
            data_count_rows = cursor.fetchall()

            # data_type, gene_id, tissue, data_count
            for row in data_count_rows:
                total_data += row[3]
                insert_rows.append(row)
        
            #logger.info("Start writing to DB")
            cursor.executemany(gene_mutation_data_counts_insert_sql, insert_rows)   
            pg_conn.commit()    
            #logger.info("End writing to DB")

            logger.info(f"Processed genes between {start_id} and {end_id}")
            start_id += increment

        logger.info(f"Total number of {data_type} data for tissue={tissue} and genes [{gene_id_start} - {gene_id_max}]: {total_data}")

    except Exception as e:
        traceback.print_exc()
        pg_conn.rollback()
    finally:
        pg_conn.commit()
        cursor.close()
        end_time = datetime.now()
        logger.info(f"Completed in {(end_time - start_time).seconds} seconds")

def drop_table(table_name):
    
    drop_table_sql = f"drop table if exists {table_name}"

    cursor = pg_conn.cursor()

    try:
        cursor.execute(drop_table_sql)
        logger.info(f"Table {table_name} has been dropped.")
    except Exception as e:
        traceback.print_exc() 
        pg_conn.rollback()
    finally:
        pg_conn.commit()
        cursor.close()



#reindex_table("idx_mut_gene")
#drop_table('fnl_sprime_pooled_delta_sprime')
# create_index("idx_sprime_mut_gene", "im_sprime_s_prime_with_mutations", "gene_id")
# create_index("idx_sprime_mut_tissue", "im_sprime_s_prime_with_mutations", "tissue")
# create_index("idx_sprime_mut_gene_tissue", "im_sprime_s_prime_with_mutations", "gene_id, tissue")
# create_index("idx_pool_gene", "fnl_sprime_pooled_delta_sprime", "gene_id")
# create_index("idx_pool_tissue", "fnl_sprime_pooled_delta_sprime", "tissue")
# create_index("idx_pool_gene_tissue", "fnl_sprime_pooled_delta_sprime", "gene_id, tissue")