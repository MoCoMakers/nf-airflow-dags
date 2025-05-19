import psycopg2
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


test_query_select = """select * from im_sprime_s_prime_with_mutations mut where mut.gene_id>=1 and mut.gene_id<=100 and tissue='LUNG'"""


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
    #return rows


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

def prepare_pooled_delta_s_results(source_df):
    df_ref_group = source_df.loc[source_df['mutation_value'] == 0]
    df_test_group = source_df.loc[source_df['mutation_value'] == 2]

     # Reference group calculations
    compounds_ref_agg_mean = df_ref_group.groupby(['name', 'gene_id']).agg(
        ref_pooled_s_prime=pd.NamedAgg(column='s_prime', aggfunc='mean'),
        ref_median_s_prime=pd.NamedAgg(column='s_prime', aggfunc='median'),
        ref_mad=pd.NamedAgg(column='s_prime', aggfunc=median_absolute_deviation),
        ref_pooled_auc=pd.NamedAgg(column='auc', aggfunc='mean'),
        ref_pooled_ec50=pd.NamedAgg(column='ec50', aggfunc='mean'),
        # row_name = cell_line
        num_ref_lines=pd.NamedAgg(column='cell_line', aggfunc='count'),
        ref_s_prime_variance=pd.NamedAgg(column='s_prime', aggfunc='var')
    ).reset_index()
    
    # Test group calculations
    compounds_test_agg_mean = df_test_group.groupby(['name', 'gene_id']).agg(
        test_pooled_s_prime=pd.NamedAgg(column='s_prime', aggfunc='mean'),
        test_median_s_prime=pd.NamedAgg(column='s_prime', aggfunc='median'),
        test_mad=pd.NamedAgg(column='s_prime', aggfunc=median_absolute_deviation),
        test_pooled_auc=pd.NamedAgg(column='auc', aggfunc='mean'),
        test_pooled_ec50=pd.NamedAgg(column='ec50', aggfunc='mean'),
        # row_name = cell_line
        num_test_lines=pd.NamedAgg(column='cell_line', aggfunc='count'),
        test_s_prime_variance=pd.NamedAgg(column='s_prime', aggfunc='var')
    ).reset_index()


    # Merging reference and test data
    compounds_merge = pd.merge(compounds_ref_agg_mean, compounds_test_agg_mean, on=['name', 'gene_id'], how='inner')

    # Calculating deltas
    compounds_merge['delta_s_prime'] = compounds_merge['ref_pooled_s_prime'] - compounds_merge['test_pooled_s_prime']
    compounds_merge['delta_auc'] = compounds_merge['ref_pooled_auc'] - compounds_merge['test_pooled_auc']
    compounds_merge['delta_ec50'] = compounds_merge['ref_pooled_ec50'] - compounds_merge['test_pooled_ec50']

    # Additional calculations for median differences
    compounds_merge['delta_s_prime_median'] = compounds_merge['ref_median_s_prime'] - compounds_merge['test_median_s_prime']

    # Calculate p-value using Mann-Whitney U test
    p_values = []
    for index, row in compounds_merge.iterrows():
        group1 = df_ref_group[df_ref_group['name'] == row['name']]['s_prime']
        group2 = df_test_group[df_test_group['name'] == row['name']]['s_prime']
        stat, p_value = mannwhitneyu(group1, group2, alternative='two-sided')
        p_values.append(p_value)

    compounds_merge['p_val_median_man_whit'] = p_values


    # Sensitivity calculations
    compounds_merge['sensitivity_score'] = np.where(compounds_merge['delta_s_prime'] < -0.5, -1,
                                                        np.where(compounds_merge['delta_s_prime'] > 0.5, 1, 0))

    compounds_merge['sensitivity'] = np.where(compounds_merge['delta_s_prime'] < -0.5, 'Sensitive',
                                                        np.where(compounds_merge['delta_s_prime'] > 0.5, 'Resistant', 'Equivocal'))
    
    # Merging drug MOA information
    df_drug_moa = source_df[["name", "moa", "target"]]
    df_drug_moa_unique = df_drug_moa.drop_duplicates(subset=['name'])
    compounds_merge = pd.merge(compounds_merge, df_drug_moa_unique, on='name', how='left')

    # Formatting MOA
    def format_to_array(x):
        if isinstance(x, str):
            return x.split(",")
        return [str(x)]

    compounds_merge['moa'] = compounds_merge['moa'].apply(format_to_array)
    
    logger.info(f"The DataFrame has {len(compounds_merge)} rows and {compounds_merge.shape[1]} columns.")

    logger.info(compounds_merge.columns)
    return compounds_merge


def refresh_mutations_helper(tissue, gene_id_start, gene_id_end):
    start_time = datetime.now()
    try:
        logger.info(f"Started processing gene ids between {gene_id_start} and {gene_id_end}")
        
        # solved_prime.id, mut.cell_line, %%, mut.gene_id, mut.mutation_value, solved_prime.name, solved_prime.s_prime, solved_prime.ec50, solved_prime.auc, solved_prime.moa, solved_prime.target
        source_df = pd.read_sql(refresh_mutations_source_data_select, pg_conn, params=(tissue, gene_id_start, gene_id_end, f"%{tissue}"))

        columns_to_keep = ['id', 'cell_line', 'tissue', 'gene_id', 'mutation_value']
        mutations_df  = source_df[columns_to_keep]
        # Create a StringIO object to write DataFrame as CSV
        csv_buffer = io.StringIO()
        mutations_df.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)  # Rewind the StringIO object to the beginning

        prepare_pooled_delta_s_results(source_df)

        # Use COPY FROM with the StringIO object
        # with pg_conn.cursor() as cursor:
        #     cursor.copy_expert(
        #         "COPY im_sprime_s_prime_with_mutations (s_prime_id, cell_line, tissue, gene_id, mutation_value) FROM STDIN WITH CSV",
        #         csv_buffer
        #     )
        #     pg_conn.commit()
    except Exception as e:
        traceback.print_exc() 
    finally:
        end_time = datetime.now()
        logger.info(f"Duration to prepare and insert data for gene ids between {gene_id_start} and {gene_id_end}: {(end_time - start_time).seconds} seconds")

def create_indexes(index_name, table_name, fields):
    start_time = datetime.now()
    # index_name = "idx_mut_gene_id"
    # table_name = "im_dep_sprime_damaging_mutations"
    # fields = "gene_id"
    drop_index_sql = f"DROP INDEX IF EXISTS {index_name}"
    create_index_sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({fields})"

    try:
        # Create a cursor object
        cursor = pg_conn.cursor()

        # Execute the SQL command
        cursor.execute(drop_index_sql)
        cursor.execute(create_index_sql)

        # Commit and close
        pg_conn.commit()
        
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


def merge_in_chunks(tissue, cell_line_mutations_df, s_prime_solved_df, chunk_size=50000):
    total_rows_inserted = 0

    logger.info(f"Chunk Size = {chunk_size}")
    # Split large DataFrame into smaller chunks

    #for start in range(0, len(cell_line_mutations_df), chunk_size):  
    for i, start in enumerate(range(0, len(cell_line_mutations_df), chunk_size)):
        logger.info(f"Processing chunk {i}")
        end = start + chunk_size
        chunk = cell_line_mutations_df.iloc[start:end]

        #chunk.set_index("cell_line", inplace=True, drop=False)

        # Merge chunk with reference DataFrame
        #logger.info("Merge chunk with reference DataFrame")
        chunk_merged = pd.merge(chunk, s_prime_solved_df, left_on="cell_line", right_on="depmap_id", how="left")
        #chunk_merged = chunk.merge(s_prime_solved_df, left_index=True, right_index=True, how="left").reset_index(drop=True)

        # Drop unnecessary columns
        #logger.info("Drop unnecessary columns")
        chunk_merged = chunk_merged.drop(columns=["depmap_id"])

        #logger.info("Create the 'tissue' column")
        chunk_merged["tissue"] = tissue

        # Convert mutation_value to integer
        #logger.info("Convert 'mutation_value' to integer")
        chunk_merged["mutation_value"] = chunk_merged["mutation_value"].astype(float).astype(int)

        #logger.info("Start to copy the chunk to csv_buffer")
        # Create a StringIO object to write DataFrame as CSV
        csv_buffer = io.StringIO()
        chunk_merged.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)  # Rewind the StringIO object to the beginning

        #logger.info(f"csv_buffer copy is complete.")

        # Use COPY FROM with the StringIO object
        with pg_conn.cursor() as cursor:
            # 'cell_line', 'gene_id', 'mutation_value', 's_prime_id', 'tissue'
            cursor.copy_expert(
                "COPY im_sprime_s_prime_with_mutations (cell_line, gene_id, mutation_value, s_prime_id, tissue) FROM STDIN WITH CSV",
                csv_buffer
            )
            pg_conn.commit()
        #logger.info(f"S-prime mutations data has been saved to database.")
        total_rows_inserted = total_rows_inserted + chunk_merged.shape[0]
    logger.info(f"Total number of rows inserted into 'im_sprime_s_prime_with_mutations' table for tissue={tissue} = {total_rows_inserted}")


def refresh_s_prime_mutations(tissue, load_type, gene_id_start, gene_id_max, gene_id_increment):
    start_time = datetime.now()
    logger.info(f"refresh_s_prime_mutations_data_efficient started for tissue={tissue}")

    mutations_table_name = "im_sprime_s_prime_with_mutations"
    mutations_table_create_sql = im_sprime_s_prime_with_mutations_table_sql
    mutations_drop_table_sql = f"drop table if exists {mutations_table_name}"

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


def refresh_data_counts(tissue, source_table_name, gene_id_start, gene_id_max, increment, load_type):
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
                WHERE tissue = %s AND gene_id >= %s AND gene_id <= %s
                GROUP BY gene_id
            """
            cursor.execute(query, (tissue, tissue, start_id, end_id))
            data_count_rows = cursor.fetchall()

            # data_type, gene_id, tissue, data_count
            for row in data_count_rows:
                total_data += row[3]
                insert_rows.append(row)
            start_id += increment
        
            cursor.executemany(gene_mutation_data_counts_insert_sql, insert_rows)   
            pg_conn.commit()    

        logger.info(f"Total number of {data_type} data for tissue={tissue} and genes [{gene_id_start} - {gene_id_max}]: {total_data}")

    except Exception as e:
        traceback.print_exc()
        pg_conn.rollback()
    finally:
        pg_conn.commit()
        cursor.close()
        end_time = datetime.now()
        logger.info(f"Completed in {(end_time - start_time).seconds} seconds")


#qa_verify_fnl_sprime_pooled_delta_sprime("pooled_delta_s_prime.csv", 7300, "LUNG")
#qa_verify_im_sprime_s_prime_with_mutations_table("s_prime_mutation_tissue.csv",  7300, "LUNG")

#refresh_pooled_delta_s_results(7300, 'LUNG')

#refresh_mutations_helper('LUNG', 7300, 7300)

#create_indexes("idx_mut_gene_id_tissue", "im_sprime_s_prime_with_mutations", "gene_id, tissue")

#load_cell_damaging_mutations_from_db("LUNG", 1, 1000)

#refresh_s_prime_mutations("PANCREAS", "INCREMENTAL", 1, 18916, 60)

#fetch_data_from_db(test_query_select)

# tissue, source_table_name, gene_id_start, gene_id_max, increment, load_type
#refresh_data_counts('LUNG', 'im_sprime_s_prime_with_mutations', 1, 18916, 250, 'INITIAL')
#refresh_data_counts('PANCREAS', 'im_sprime_s_prime_with_mutations', 1, 18916, 250, 'INCREMENTAL')

fetch_data_from_db(test_query_select)