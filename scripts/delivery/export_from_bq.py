#
# Export given tables from BigQuery to Google Storage
#

import os
import getopt
import sys
import json
import datetime

# export_cmd = 'bq --location=US extract --destination_format=CSV --compression=NONE --field_delimiter=\t --print_header=TRUE \
#                 {proj}:{db}.{table} gs://{bucket}/csv/{table}/*{ext}'


# ----------------------------------------------------
# default config values
# To override default config values, copy the keys to be overriden to a json file,
# and indicate this file as --config parameter
# ----------------------------------------------------

config_default = {

            
    "bq_project":      "...",
    "bq_dataset":      "...",

    "destination_format":       "csv",
    "destination_type":         "local",

    "gs_bucket_path":   "gs://...",
    "local_path":       "../...",


    "destination_formats": [
        {
            "id": "avro",
            "more_params": "--use_avro_logical_types"
        },
        {
            "id": "csv",
            "more_params": "--field_delimiter=\",\" --print_header=TRUE"
        }
    ],

    "destination_types": [
        "gs",
        "local"
    ],

    "bq_tables": [

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

    print('Reading params...')
    params = {
        "config_file":      "",
        "destination_type": "",
        "destination_format": ""
    }
    
    # Parsing command line arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:],"c:t:f:",["config=,type=,format="])
        # if len(args) == 0:
        #     raise getopt.GetoptError("read_params() error", "Mandatory argument is missing.")

    except getopt.GetoptError as err:
        print(err.args)
        print("Please indicate correct params:")
        print("config_file:         optional: indicate '-c' for 'config', local config json file")
        print("destination_type:    optional: indicate '-t' for 'type', 'gs' or 'local' literal")
        print("destination_format:  optional: indicate '-f' for 'format', 'avro' or 'csv' literal")
        sys.exit(2)

    # get config files namess
    for opt, arg in opts:
        if opt == '-c' or opt == '--config':
            if os.path.isfile(arg):
                params['config_file'] = arg
            else:
                raise getopt.GetoptError("read_params() error. Config file is not found: " + arg)
        if opt == '-t' or opt == '--type':
            params['destination_type'] = arg
        if opt == '-f' or opt == '--format':
            params['destination_format'] = arg

    print('\nthe params given', params)
    return params

# ----------------------------------------------------
# read_config()
# ----------------------------------------------------

def read_config(params):
    
    print('Reading config...')
    config = {}
    config_read = {}

    config_file = params['config_file']

    if os.path.isfile(config_file):
        with open(config_file) as f:
            config_read = json.load(f)
    else:
        print('Config file is not found:', config_file)

    # config has lower priority
    for k in config_default:
        s = config_read.get(k, config_default[k])
        config[k] = s
    
    # params have higher priority
    for k in config_default:
        s = params.get(k)
        if not(s == "" or s == None):
            config[k] = s

    print(config)
    return config


# commands

# gs destination - always required step
# export_cmd_onedir = 'bq --location=US extract --destination_format={format_u} --compression=NONE {more_params} \
#                 {proj}:{db}.{table} {bucket_path}/{format_l}/{table}{ext}'
export_cmd_onedir = 'bq --location=US extract --destination_format={format_u} --compression=NONE {more_params} \
                {proj}:{db}.{table} {bucket_path}/{table}{shard_mark}{ext}'

# final destination - optional step
# copy_cmd_onedir = 'gsutil cp {bucket_path}/{format_l}/{table}{ext} {local_path}/{format_l}/'
copy_cmd_onedir = 'gsutil cp {bucket_path}/{table}*{ext} {local_path}/'

# func

def mkdir_if_needed(p):    
    if not os.path.isdir(p):
        bqc = 'mkdir {0}'.format(p)
        print(bqc)    
        rc = os.system(bqc)
        if rc != 0: exit(rc)


# main

def main():

    rc = 0
    duration = datetime.datetime.now()
    params = read_params()
    config = read_config(params)

    # set local variables from configuration
    bq_tables = config["bq_tables"]
    destination_type = config["destination_type"]
    destination_format = config["destination_format"]
    
    more_params = ''
    for f in config["destination_formats"]:
        if destination_format == f["id"]:
            more_params = f["more_params"]
    
    local_destination_path = config["local_path"]
    gs_bucket_path = config["gs_bucket_path"]
    
    bq_project = config["bq_project"]
    bq_dataset = config["bq_dataset"]


    for bq_table in bq_tables:

        # gs destination - required step always
        bqc = export_cmd_onedir.format(
                format_u = destination_format.upper(), format_l = destination_format.lower(),
                more_params = more_params,
                proj = bq_project, db = bq_dataset, table = bq_table,
                bucket_path = gs_bucket_path,
                ext = '.' + destination_format,
                shard_mark = ''
            )
        print(bqc)
        rc = os.system(bqc)
        

        if rc != 0: # a possible obstacle is when a table is too large. Then try to shard export.
            bqc = export_cmd_onedir.format(
                    format_u = destination_format.upper(), format_l = destination_format.lower(),
                    more_params = more_params,
                    proj = bq_project, db = bq_dataset, table = bq_table,
                    bucket_path = gs_bucket_path,
                    ext = '.' + destination_format,
                    shard_mark = '*'
                )
            print(bqc)
            rc = os.system(bqc)

        if rc != 0: break

        # final destination - if this type of export is selected
        if destination_type == 'local' and rc == 0:
            # create path if not exists
            mkdir_if_needed(local_destination_path)
            # mkdir_if_needed('{0}/{1}'.format(local_destination_path, destination_format))
            bqc = copy_cmd_onedir.format(
                    format_l = destination_format.lower(),
                    table = bq_table,
                    bucket_path = gs_bucket_path,
                    ext = '.' + destination_format,
                    local_path = local_destination_path
                )
            print(bqc)
            rc = os.system(bqc)
            if rc != 0: break

    duration = datetime.datetime.now() - duration
    print('Run time: {0}'.format(duration)) # timedelta HH:MM:SS.f

    return rc

# go

rc = main()
exit(rc)

# last edit: 2021-02-07
