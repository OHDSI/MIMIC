#
# Export given tables from BigQuery to Google Storage
#

import os
import getopt
import sys
import json

# export_cmd = 'bq --location=US extract --destination_format=CSV --compression=NONE --field_delimiter=\t --print_header=TRUE \
#                 {proj}:{db}.{table} gs://{bucket}/csv/{table}/*{ext}'


# ----------------------------------------------------
# default config values
# To override default config values, copy the keys to be overriden to a json file,
# and indicate this file as --config parameter
# ----------------------------------------------------

config_default = {

            
    "bq_project":      "odysseus-mimic-dev",
    "bq_dataset":      "mimiciv_demo_202012_cdm_531",

    "destination_format":       "csv",
    "destination_type":         "gs",

    "gs_bucket_path":   "gs://mimic_iv_to_omop/export_cdm_demo",
    "local_path":       "../export_cdm",


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

    print('the params given', params)
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
export_cmd_onedir = 'bq --location=US extract --destination_format={format_u} --compression=NONE {more_params} \
                {proj}:{db}.{table} {bucket_path}/{format_l}/{table}-*{ext}'

# final destination - optional step
copy_cmd_onedir = 'gsutil cp {bucket_path}/{format_l}/{table}-*{ext} {local_path}/{format_l}/'

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


    # gs destination - required step always
    for bq_table in bq_tables:

        bqc = export_cmd_onedir.format(
                format_u = destination_format.upper(), format_l = destination_format.lower(),
                more_params = more_params,
                proj = bq_project, db = bq_dataset, table = bq_table,
                bucket_path = gs_bucket_path,
                ext = '.' + destination_format
            )
        print(bqc)
        rc = os.system(bqc)
        if rc != 0: break

    # final destination - if this type of export is selected

    if destination_type == 'local' and rc == 0:

        # create path if not exists
        mkdir_if_needed(local_destination_path)
        mkdir_if_needed('{0}/{1}'.format(local_destination_path, destination_format))

        for bq_table in bq_tables:

            mkdir_if_needed('{0}/{1}/{2}'.format(
                local_destination_path, destination_format, bq_table))

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

    return rc

# go

rc = main()
exit(rc)

# last edit: 2020-12-21