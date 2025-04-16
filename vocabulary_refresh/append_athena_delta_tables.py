import getopt
import json
import os
import shutil
import sys

import pandas as pd

'''
default config values
To override default config values, copy the keys to be overriden to a json file,
and indicate this file as --config parameter

Known issue: we always load tables to a dataset in the default user's project

Expected locations for CSVs are:
    standard vocabulary tables
        - one directory for all, 
        - each file has the same name as the target table without prefix, in UPPER case
    custom mapping files
        - can be in multiple directories (passed as a list),
        - files need to be named as <athena_table>_delta.csv
        - the structure of the files must match the structure of the standard vocabulary tables
        - the delta_folders list in the config is the path to these directories
'''
# ----------------------------------------------------

config_default = {

    "local_athena_csv_path": "somewhere",
    "gs_athena_csv_path": "gs://some_path",
    "delta_folders": ["./mimic_delta_folder", "./custom_delta_folder"],
    "combined_athena_delta_tables_path": "./athena_delta_tables",
    "athena_csv_delimiter": "\t",
    "athena_csv_quote": "",

    "local_mapping_csv_path": "custom_mapping_csv",
    "gs_mapping_csv_path": "gs://some_path",
    "mapping_csv_delimiter": ",",
    "mapping_csv_quote": "\"",

    "schemas_dir_all_csv": "omop_schemas_vocab_bq",

    "variables":
        {
            "@bq_target_project": "bq_target_project",
            "@bq_target_dataset": "bq_target_dataset"
        },

    "vocabulary_tables":
        [
            "domain",
            "relationship",
            "concept_class",
            "drug_strength",
            "concept_synonym",
            "concept_ancestor",

            "concept",
            "concept_relationship",
            "vocabulary"
        ],

    "bq_athena_temp_table_prefix": "tmp_",
    "custom_mapping_table": "custom_mapping"
}

# ----------------------------------------------------
# read_params()
# ----------------------------------------------------

def read_params():

    print('Read params...')
    params = {
        "config_file": "optional: indicate '-c' for 'config', json file name. Defaults are hard-coded"
    }

    # Parsing command line arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:], "c:",  ["config="])
        if len(opts) == 0:
            raise getopt.GetoptError("read_params() error", "Mandatory argument is missing.")
    except getopt.GetoptError:
        print("Please indicate correct params:")
        print(params)
        print("for example:\npython append_athena_delta_tables.py --config vocabulary_refresh.conf")
        sys.exit(2)

    for opt, arg in opts:

        if opt == '-c' or opt == '--config':
            if os.path.isfile(arg):
                params['config_file'] = arg
            else:
                params['config_file'] = ''

    print(params)
    return params

# ----------------------------------------------------
# read_config()
# ----------------------------------------------------

def read_config(config_file):

    print('Read config...')
    config = {}
    config_read = {}

    if os.path.isfile(config_file):
        with open(config_file) as f:
            config_read = json.load(f)

    for k in config_default:
        s = config_read.get(k, config_default[k])
        config[k] = s

    print(config)
    print('Loading tables...')
    print(config['vocabulary_tables'])
    return config

''' 
----------------------------------------------------
    load_table()
    return codes: 0, 1, 2
----------------------------------------------------
'''

def update_date_format(df):
    """
    The _delta tables have a date format of YYYY-MM-DD. Remove the - so the date format matches the standard vocabulary
    tables (i.e. YYYYMMDD)
    """
    date_cols = ['valid_start_date', 'valid_end_date']

    for col in date_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace('-', '', regex=False)

    return df


def main():
    params = read_params()
    args = read_config(params['config_file'])
    athena_dir = args["local_athena_csv_path"]
    delta_dirs = args["delta_folders"]
    save_path = args["combined_athena_delta_tables_path"]
    
    # Get list of "athena" tables
    athena_tables = {f for f in os.listdir(athena_dir) if f.endswith(".csv")}

    # Process each athena table
    for athena_table in athena_tables:
        athena_path = os.path.join(athena_dir, athena_table)

        # Look for corresponding _delta tables (lowercase) across all delta_dirs
        delta_dfs = []
        delta_filename = f'{os.path.splitext(athena_table)[0].lower()}_delta.csv'

        for delta_dir in delta_dirs:
            delta_path = os.path.join(delta_dir, delta_filename)
            if os.path.exists(delta_path):
                df = pd.read_csv(delta_path, keep_default_na=False)
                if not df.empty:
                    if ('valid_start_date' in df.columns) or ('valid_end_date' in df.columns):
                        df = update_date_format(df)
                    delta_dfs.append(df)

        # Create the save path for the appended table
        table_save_path = os.path.join(save_path, athena_table)

        # If there are no non-empty delta tables, skip this athena table
        if not delta_dfs:
            print(f"No non-empty delta tables found for {athena_table}, copying Athena table {athena_table} to {save_path}.")
            shutil.copyfile(athena_path, table_save_path)
            continue

        # Merge available _delta tables first
        delta_combined = pd.concat(delta_dfs, ignore_index=True)

        # Open file for writing
        chunk_size = 100_000
        with open(table_save_path, 'w', newline='') as f_out:
            first_chunk = True
            chunk_iter = pd.read_csv(athena_path, sep='\t', chunksize=chunk_size, keep_default_na=False)

            prev_chunk = next(chunk_iter, None)  # Read the first chunk

            while prev_chunk is not None:
                next_chunk = next(chunk_iter, None)  # Peek at the next chunk

                # Check if this is NOT the last chunk and its size is smaller than expected
                if next_chunk is not None and len(prev_chunk) < chunk_size:
                    raise ValueError(f"Error: Expected {chunk_size} rows, but dataframe only has {len(chunk)} rows.")

                # Write the chunk to file
                prev_chunk.to_csv(f_out, sep='\t', index=False, header=first_chunk, mode='a')
                first_chunk = False  # Ensure headers are written only once

                # Move to the next chunk
                prev_chunk = next_chunk

            # Append the delta table after the athena table
            delta_combined.to_csv(f_out, sep='\t', index=False, header=False, mode='a')

        print(f"Updated {athena_table} with {len(delta_combined)} new rows.")

    print("Processing complete!")


if __name__ == "__main__":
    main()
