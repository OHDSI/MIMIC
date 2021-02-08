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
import datetime

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

    "bq_target_project":        "project...",
    "bq_target_dataset":        "mimiciv_delivery_demo",

    "source_format":            "csv",
    "csv_path":                 "gs://...",
    "csv_delimiter":            ",",
    "csv_quote":                "\\\"",
    "schemas_dir_all_csv":      "scripts/delivery/cdm_schemas_custom",

    "bq_temp_table_prefix":     "",

    "bq_tables":
    [

        # dimension tables
        "location",
        "care_site",
        "provider",
        "person",

        # event tables
        "death",
        "visit_occurrence",
        "visit_detail",
        "condition_occurrence",
        "procedure_occurrence",
        "specimen",
        "measurement",
        "observation",
        "drug_exposure",
        "device_exposure",
        "fact_relationship",
        "observation_period",

        # tables which are not populated
        "cohort",
        "cohort_attribute",
        "cohort_definition",
        "attribute_definition",
        "cost",
        "note",
        "note_nlp",
        "metadata",
        "payer_plan_period",

        # eras and cdm_source
        "condition_era",
        "drug_era",
        "dose_era",
        "cdm_source",
        
        # vocabulary tables
        "source_to_concept_map",
        "concept_class",
        "domain",
        "relationship",
        "vocabulary",
        "concept",
        "concept_relationship", # to shard
        "concept_ancestor", # to shard
        "concept_synonym",
        "drug_strength"

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

    print('Try to load: {0}'.format(table))

    return_code = 0

    schema_path = '{dir}/{table}.json'

    bq_load_command = 'bq --location=US load --replace --source_format={format_u} {more_params} \
                    {dataset}.{prefix}{table} {files_path}/{table}{shard_mark}{ext} {schema_path}'

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

        # try to load single file
        bqc = bq_load_command.format( \
            format_u    = config["source_format"].upper(), \
            more_params = more_params, \
            dataset     = config['bq_target_dataset'], \
            prefix      = config['bq_temp_table_prefix'], \
            table       = table, \
            files_path  = files_path, \
            format_l    = config["source_format"].lower(), \
            ext         = '.' + config["source_format"], \
            schema_path = table_schema,
            shard_mark  = ''
        )
        print('To BQ: ' + bqc)
        rc = os.system(bqc)

        # if failed (URI not found), try to load multiple files
        if rc != 0:
            bqc = bq_load_command.format( \
                format_u    = config["source_format"].upper(), \
                more_params = more_params, \
                dataset     = config['bq_target_dataset'], \
                prefix      = config['bq_temp_table_prefix'], \
                table       = table, \
                files_path  = files_path, \
                format_l    = config["source_format"].lower(), \
                ext         = '.' + config["source_format"], \
                schema_path = table_schema,
                shard_mark  = '*'
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
    nice output about execution result

    s_filename: script executed (any string actually)
    status:     0 = ok, !0 = error
    msg:        additional info if there is any
----------------------------------------------------
'''
def nice_message(s_filename, status, msg):
    time =    datetime.datetime.now()
    file =    s_filename.ljust(35, ' ')
    result =  'Done.' if status==0 else 'Error'
    message = '' if len(msg)==0 else ': ' + msg if len(msg.split('\n')) == 1 \
        else '\n' + '\n'.join(map(lambda x: ''.ljust(4) + x, msg.split('\n')))

    return '{0} | {1} | {2}{3}'.format(time, file, result, message)


'''
----------------------------------------------------
    main()
    return code:
        0 = success
        lower byte = number of Athena tables failed to load
----------------------------------------------------
'''

def main():

    rc = 0
    duration = datetime.datetime.now()
    params = read_params()
    config = read_config(params.get('config_file'))

    s_done = []
    s_done.append(nice_message('start...', 0, ''))

    create_bq_dataset(config['bq_target_dataset'])

    for table in config['bq_tables']:

        rc = load_table(table, config['csv_path'], config['csv_delimiter'], \
            config['csv_quote'], config)
        if rc != 0:
            break

    s_done.append(nice_message('finish', 0, ''))

    print('\nTables are loaded to {pr}.{ds}'.format(pr=config['bq_target_project'], ds=config['bq_target_dataset']))
    for a in s_done:
        print(a)
    duration = datetime.datetime.now() - duration
    print('Run time: {0}'.format(duration)) # timedelta HH:MM:SS.f

    return rc

# ----------------------------------------------------
# go
# ----------------------------------------------------
return_code = main()
print('load_to_bq.exit()', return_code)

exit(return_code)

# last edit: 2021-02-07
