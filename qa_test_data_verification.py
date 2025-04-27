import traceback
import psycopg2
from datetime import datetime
from pathlib import Path
import csv
import pandas as pd
import time
import numpy as np
import pickle
import statistics


# name, ref_pooled_s_prime, num_ref_lines, test_pooled_s_prime, num_test_lines, delta_s_prime
fnl_sprime_pooled_delta_sprime_select = "select name, ref_pooled_s_prime, num_ref_lines, test_pooled_s_prime, num_test_lines, delta_s_prime from fnl_sprime_pooled_delta_sprime"

DEP_PRISM_PATH = "/home/gatlay/nf_streamlit/app/data/DepMap/Prism19Q4"
DEP_PUBLIC_PATH = "/home/gatlay/nf_streamlit/app/data/DepMap/Public24Q2"

postgres_host = "XXXX"
postgres_name = "XXXX"
postgres_user = "XXXX"
postgres_password = "XXXX"

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

def fetch_data_from_db(select_sql):
    cursor = pg_conn.cursor()
    cursor.execute(select_sql)
    rows = cursor.fetchall()
    # for r in rows:
    #     print(pickle.loads(r[1]))
    #print(f"Total number of rows fetched: {len(rows)}")
    return rows

# Steps to compare CSV data with DB data:
# 1) Download 'Pooled Delta S' for Selected Values' data from web tool
# 2) Put the downloaded CSV file into the folder where you are running this code.
# 3) Run this function and check console logs to see the row count comparisons, and number of items that don't match.
# TODO: Add gene_id, tissue_name parameters to this method because CSV content will be specific to gene and tissue.
def qa_verify_fnl_sprime_pooled_delta_sprime(data_file_name):
     # extracting only columns: 'name', 'ref_pooled_s_prime', 'num_ref_lines', 'test_pooled_s_prime', 'num_test_lines', 'delta_s_prime'
    column_order = ['name', 'ref_pooled_s_prime', 'num_ref_lines', 'test_pooled_s_prime', 'num_test_lines', 'delta_s_prime']
    df = build_df(data_file_name, usecols=column_order)
    df = df[column_order]
    r, c = df.shape
    print(f"CSV row count = {r}")
    df_records = df.to_dict('records')
    df_dict = {}
    for item in df_records:
        df_dict[item['name']] = item

    #print(f"df_dict length = {len(df_dict)}")
    
    # TODO: Add gene and tissue to match the content within the CSV file. 
    # We have the data for gene id (7300 = NF1) and tissue LUNG
    db_data = fetch_data_from_db(fnl_sprime_pooled_delta_sprime_select)

    print(f"DB row count = {len(db_data)}")

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

    print(f"names_not_in_csv length = {len(names_not_in_csv)}")
    print(f"names_not_in_csv = {names_not_in_csv}")

    print(f"no_match_ref_lines_items length = {len(no_match_ref_lines_items)}")
    print(f"no_match_test_lines_items length = {len(no_match_test_lines_items)}")

    print(f"no_match_ref_pooled_s_prime_items length = {len(no_match_ref_pooled_s_prime_items)}")
    print(f"no_match_test_pooled_s_prime_items length = {len(no_match_test_pooled_s_prime_items)}")

    print(f"no_match_delta_s_prime_items length = {len(no_match_delta_s_prime_items)}")

    return df

qa_verify_fnl_sprime_pooled_delta_sprime("pooled_delta_s_prime.csv")