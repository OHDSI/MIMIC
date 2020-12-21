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

Known issue: the tables are loaded only to default user's project

Expected locations for CSVs are:
    - one directory for all
'''
# ----------------------------------------------------

config_default = {

    "bq_target_project":        "bq_target_project",
    "bq_target_dataset":        "bq_target_dataset",

    "overwrite": "no",

    "source_format":            "csv",
    "csv_path":                 "somewhere",
    "csv_delimiter":            ",",
    "csv_quote":                "\\\"",
    "schemas_dir_all_csv":      "scripts/delivery/cdm_schemas_custom",

    "bq_temp_table_prefix":     "tmp_",

    "bq_tables":
    [
        "observation",
        "drug_era",
        "fact_relationship",
        "observation_period",
        "cohort",
        "visit_occurrence",
        "drug_strength",
        "condition_era",
        "cdm_source",
        "measurement",
        "domain",
        "note_nlp",
        "metadata",
        "provider",
        "person",
        "concept_relationship",
        "cohort_attribute",
        "procedure_occurrence",
        "care_site",
        "vocabulary",
        "specimen",
        "death",
        "source_to_concept_map",
        "device_exposure",
        "relationship",
        "payer_plan_period",
        "concept",
        "location",
        "condition_occurrence",
        "concept_synonym",
        "cohort_definition",
        "attribute_definition",
        "drug_exposure",
        "note",
        "concept_ancestor",
        "cost",
        "dose_era",
        "concept_class",
        "visit_detail"
    ]


}

# ----------------------------------------------------
# read_params()
# ----------------------------------------------------

def read_params():

    print('Read params...')
    params = {
        "config_file":   ""
    }
    
    # Parsing command line arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:],"c:",["config="])
    except getopt.GetoptError:
        print("Please indicate correct params:")
        print("config_file:     optional: indicate '-c' for 'config', json file name. Defaults are hard-coded")
        print("for example:\npython load_to_bq.py --config c.conf")
        sys.exit(2)

    st = []
    for opt, arg in opts:

        if opt == '-c' or opt == '--config':
            if os.path.isfile(arg):
                params['config_file'] = arg
            else:
                raise getopt.GetoptError("read_params() error. Config file is not found: " + arg)

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
    print(config['bq_tables'])
    return config


''' 
----------------------------------------------------
    create_bq_dataset()
    return codes: system
    todo: check existence
----------------------------------------------------
'''
def create_bq_dataset(ds_name):

    bqc = 'bq mk {ds_name}'.format(ds_name=ds_name)

    print('Create dataset: ', bqc)
    rc = os.system(bqc)

    return rc    


''' 
----------------------------------------------------
    load_table()
    return codes: 0, 1, 2
----------------------------------------------------
'''
def load_table(table, files_path, field_delimiter, quote, config):

    return_code = 0

    schema_path = '{dir}/{table}.json'

    bq_load_command = 'bq --location=US load {replace} --source_format={format_u} {more_params} \
                    {dataset}.{prefix}{table} {files_path}/{format_l}/{table}-*{ext} {schema_path}'

    more_params_csv = "" + \
            " --allow_quoted_newlines=True " + \
            " --skip_leading_rows=1 " + \
            " --field_delimiter=\"{field_delimiter}\" " + \
            " --quote=\"{quote}\" "

    more_params = more_params_csv.format(
        field_delimiter=field_delimiter,
        quote=quote
    )

    table_schema = schema_path.format(dir=config['schemas_dir_all_csv'], table=table)
    
    if os.path.isfile(table_schema):
        bqc = bq_load_command.format( \
            replace     = '--replace' if config["overwrite"] == 'yes' else '', \
            format_u    = config["source_format"].upper(), \
            more_params = more_params, \
            dataset     = config['bq_target_dataset'], \
            prefix      = config['bq_temp_table_prefix'], \
            table       = table, \
            files_path  = files_path, \
            format_l    = config["source_format"].lower(), \
            ext         = '.' + config["source_format"], \
            schema_path = table_schema
        )
        print('To BQ: ' + bqc)

        rc = os.system(bqc)
        if rc != 0:
            return_code = 2 # error during execution of the command

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
----------------------------------------------------
'''

def main():
    params = read_params()

    config = read_config(params.get('config_file'))

    file_path = '{files_path}/{table}-*{ext}'

    create_bq_dataset(config['bq_target_dataset'])

    rca = 0
    for table in config['bq_tables']:

        # files_path = file_path.format(
        #     files_path=config['csv_path'], table=table, ext='.csv')

        rc = load_table(table, config['csv_path'], config['csv_delimiter'], \
            config['csv_quote'], config)
        if rc != 0:
            break
            # rca += 1
            # continue

    return rc #rca

# ----------------------------------------------------
# go
# ----------------------------------------------------
return_code = main()
print('load_to_bq.exit()', return_code)

exit(return_code)

# last edit: 2020-12-21
