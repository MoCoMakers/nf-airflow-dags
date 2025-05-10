import psycopg2
from pathlib import Path
import pandas as pd
import numpy as np
import utils 

_config = utils.get_config_data_refresh()

postgres_host = _config['db']['postgres_host']
postgres_name = _config['db']['postgres_name']
postgres_user = _config['db']['postgres_user']
postgres_password = _config['db']['postgres_password']

pg_conn = psycopg2.connect(
        host=postgres_host,
        database=postgres_name,
        user=postgres_user,
        password=postgres_password
)

DEP_PRISM_PATH = _config['files_path']['dep_prism_path']
DEP_PUBLIC_PATH = _config['files_path']['dep_public_path']

SEC_RESP_DOSE_CURVE = _config['files_path']['sec_resp_dose_curve']
OMICS_MUTATIONS_MATRIX = _config['files_path']['omics_mutations_matrix']
    

def analyze_mutation_tissue():
    file_name = 's_prime_mutation_tissue.csv'

    cell_line_ccle_dict = {}
    cell_line_group_count_dict = {}

    total_rows = 0

    chunksize = 20000
    for chunk in pd.read_csv(Path("") / file_name, chunksize=chunksize):
        total_rows = total_rows + len(chunk.values)
        for row in chunk.values:
            name = row[1]
            cell_line = row[8]
            ccle_name = row[14]
            if name == '9-aminoacridine' and cell_line == 'ACH-001075':
                print(f"{row}")
            if cell_line in cell_line_ccle_dict.keys():
                current_ccles = cell_line_ccle_dict[cell_line]
                current_ccles.append(ccle_name)
                cell_line_ccle_dict[cell_line] = list(set(current_ccles))
                cell_line_group_count_dict[cell_line] = cell_line_group_count_dict[cell_line] + 1
            else:
                cell_line_ccle_dict[cell_line] = [ccle_name]
                cell_line_group_count_dict[cell_line] = 1

    print(f"Total number of rows in file {file_name} = {total_rows}")

    print(f"cell_line_ccle_dict length = {len(cell_line_ccle_dict)}")
    print(f"cell_line_group_count_dict length = {len(cell_line_group_count_dict)}")
    for key,value in cell_line_group_count_dict.items():
        print(f"Row count for cell line {key} : {value}")
    print(f"Sum of cell line groups row counts : {sum(cell_line_group_count_dict.values())}")


def confirm_pooled_delta_s_prime_with_mutation_tissue_data(test_name):
    file_name = 's_prime_mutation_tissue.csv'
    
    name_row_name_ref_dict = {}
    name_row_name_test_dict = {}

    total_rows = 0

    chunksize = 20000
    for chunk in pd.read_csv(Path("") / file_name, chunksize=chunksize):
        total_rows = total_rows + len(chunk.values)
        for row in chunk.values:
            name = row[1]
            row_name = row[8]
            s_prime = row[13]
            mutation_value = row[16]
            if mutation_value == 0:
                if name in name_row_name_ref_dict.keys():
                    current_val = name_row_name_ref_dict[name]
                    row_names = current_val[0]
                    s_prime_values = current_val[1]
                    row_names.append(row_name)
                    s_prime_values.append(s_prime)
                    # list(set(current_val))
                    name_row_name_ref_dict[name] = (row_names, s_prime_values)
                else:
                    name_row_name_ref_dict[name] = ([row_name], [s_prime])
            else:
                if name in name_row_name_test_dict.keys():
                    current_val = name_row_name_test_dict[name]
                    row_names = current_val[0]
                    s_prime_values = current_val[1]
                    row_names.append(row_name)
                    s_prime_values.append(s_prime)
                    # list(set(current_val))
                    name_row_name_test_dict[name] = (row_names, s_prime_values)
                else:
                    name_row_name_test_dict[name] = ([row_name], [s_prime])

    print(f"Total number of rows in file {file_name} = {total_rows}")
    print(f"name_row_name_ref_dict length = {len(name_row_name_ref_dict)}")
    print(f"name_row_name_test_dict length = {len(name_row_name_test_dict)}")
    
    print(f"num_ref_lines for {test_name} = {len(name_row_name_ref_dict[test_name][0])}")
    print(f"ref s_prime length  = {len(name_row_name_ref_dict[test_name][1])}")
    print(f"ref_pooled_s_prime  = {np.mean(name_row_name_ref_dict[test_name][1])}")

    print(f"num_test_lines for {test_name} = {len(name_row_name_test_dict[test_name][0])}")
    print(f"test s_prime values for  {test_name} = {name_row_name_test_dict[test_name][1]}")
    print(f"test_pooled_s_prime  = {np.mean(name_row_name_test_dict[test_name][1])}")