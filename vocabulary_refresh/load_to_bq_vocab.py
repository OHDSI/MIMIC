#
# Load CSV tables from GS to BigQuery
#
# Provide schema JSONs in schema_path
# Run from current directory
# 

import os
import sys
import getopt
import json

# ----------------------------------------------------
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
        - one directory for all,
        - the name of the directory should be CUSTOM_MAPPING, in upper case
        - the path in the config is the path to this directory
            i.e. in the given default config the location of the custom mapping files is 
            gs://some_path/CUSTOM_MAPPING/*.csv
'''
# ----------------------------------------------------

config_default = {

    "local_athena_csv_path":    "somewhere",
    "gs_athena_csv_path":       "gs://some_path",
    "athena_csv_delimiter":     "\t",
    "athena_csv_quote":         "",

    "local_mapping_csv_path":   "custom_mapping_csv",
    "gs_mapping_csv_path":      "gs://some_path",
    "mapping_csv_delimiter":    ",",
    "mapping_csv_quote":        "\"",

    "schemas_dir_all_csv":      "omop_schemas_vocab_bq",

    "variables": 
    {
        "@bq_target_project":        "bq_target_project",
        "@bq_target_dataset":        "bq_target_dataset"
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

    "bq_athena_temp_table_prefix":  "tmp_",
    "custom_mapping_table":     "custom_mapping"
}

# ----------------------------------------------------
# read_params()
# ----------------------------------------------------

def read_params():

    print('Read params...')
    params = {
        "steps":    ["mandatory: indicate '-a' for 'athena', '-m' for (custom) 'mapping' or both"],
        "config_file":   "optional: indicate '-c' for 'config', json file name. Defaults are hard-coded"
    }
    
    # Parsing command line arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:],"ampc:",["athena", "mapping", "config="])
        if len(opts) == 0:
            raise getopt.GetoptError("read_params() error", "Mandatory argument is missing.")
    except getopt.GetoptError:
        print("Please indicate correct params:")
        print(params)
        print("for example:\npython load_to_bq_vocab.py --athena --config vocabulary_refresh.conf")
        sys.exit(2)

    st = []
    for opt, arg in opts:

        if opt == '-c' or opt == '--config':
            if os.path.isfile(arg):
                params['config_file'] = arg
            else:
                params['config_file'] = ''             

        if opt == '-a' or opt == '--athena':
            st.append('athena')
        if opt == '-m' or opt == '--mapping':
            st.append('mapping')
        params['steps'] = st

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

def load_table(table, gs_path, field_delimiter, quote, config):

    return_code = 0

    schema_path = '{dir}/{table}.json'
    # bq_table = '{project}.{dataset}.{prefix}{table}'
    bq_table = '{dataset}.{prefix}{table}'

    bq_load_command = \
        "bq --location=US load --replace " + \
        " --source_format=CSV  " + \
        " --allow_quoted_newlines=True " + \
        " --skip_leading_rows=1 " + \
        " --field_delimiter=\"{field_delimiter}\" " + \
        " --quote=\"{quote}\" " + \
        " {table_name} " + \
        " \"{files_path}\" " + \
        "\"{schema_path}\" "
        # " --quote=\"\\\"\" " + \ # doesn't work for CONCEPT and CONCEPT_SYNONYM, sometimes is required for other tables
        # " --autodetect " + \ # does not work for empty tables

    table_path = bq_table.format( \
        # project=config['bq_target_project'], 
        dataset=config['variables']['@bq_target_dataset'], \
        prefix=config['bq_athena_temp_table_prefix'], table=table)
    table_schema = schema_path.format(dir=config['schemas_dir_all_csv'], table=table)
    
    if os.path.isfile(table_schema):
        bqc = bq_load_command.format( \
            table_name = table_path, \
            files_path = gs_path, \
            schema_path = table_schema,
            field_delimiter=field_delimiter,
            quote=quote
        )
        print('To BQ: ' + bqc)

        try:
            os.system(bqc)
        except Exception as e:
            return_code = 2 # error during execution of the command
            raise e
    else:
        return_code = 1 # file not found
        print ('Schema file {f} is not found.'.format(f=table_schema))

    return return_code

'''
----------------------------------------------------
    main()
    return code:
        0 = success
        lower byte = number of Athena tables failed to load
        upper byte = if Custom mapping table failed to load
----------------------------------------------------
'''

def main():
    params = read_params()

    config = read_config(params.get('config_file'))

    return_code = 0
    gs_file_path = '{gs_path}/{table}{ext}'

    for step in params['steps']:

        rca = 0
        if step == 'athena':
            for table in config['vocabulary_tables']:

                gs_path = gs_file_path.format(
                    gs_path=config['gs_athena_csv_path'], table=table.upper(), ext='.csv')

                rc = load_table(table, gs_path, config['athena_csv_delimiter'], \
                    config['athena_csv_quote'], config)
                if rc != 0:
                    rca += 1
                    continue
        rcm = 0
        if step == 'mapping':

            gs_path = gs_file_path.format(
                gs_path=config['gs_mapping_csv_path'], table='*', ext='.csv')

            rc = load_table(config['custom_mapping_table'], gs_path, config['mapping_csv_delimiter'], \
                config['mapping_csv_quote'], config)
            if rc != 0:
                rcm += 1

    return_code = rca + rcm * 0xff
    return return_code

# ----------------------------------------------------
# go
# ----------------------------------------------------
return_code = main()

exit(return_code)

