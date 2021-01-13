#
# Generate SQL script to convert datetime fields in the imported dataset
# 

import os
import sys
import getopt
import json
import re
# import bq_run_script

# ----------------------------------------------------
'''
    python gen_bq_tmp_to_cdm_delivery.py
    To create config file, copy the config_default content to a .conf file and set the desired values
    target dataset name template: name_dateref_cdm_version (i.e. synpuf_2018q3_cdm_53)
'''
# ----------------------------------------------------

config_default = {

    "output_sql_file":          "scripts/delivery/crete_cdm_from_tmp_gen.sql",

    "bq_target_project":        "odysseus-mimic-dev",
    "bq_target_dataset":        "mimiciv_delivery_demo",

    "bq_temp_table_prefix":     "tmp_",

    "schema_dir":               "scripts/delivery/cdm_schemas_custom",

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
        "config_file":   "optional: indicate '-c' for 'config', json file name. Default config file name is the same as script."
    }
    
    # Parsing command line arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:],"c:",["config="])
        # if len(opts) == 0:
        #     raise getopt.GetoptError("read_params() error", "Mandatory argument is missing.")
    except getopt.GetoptError:
        print("Please indicate correct params:")
        print(params)
        print("for example:\npython gen_bq_tmp_to_cdm_delivery.py --config gen_bq_tmp_to_cdm_delivery.conf")
        sys.exit(2)

    st = []

    params['config_file'] = os.path.basename(__file__).replace('py', 'conf')

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
    return config


# ------------------------------------------------------------------------
# Get concept fields and Co from schemas provided
# {tables: [{table, fields: [field]}]}
# ------------------------------------------------------------------------
def generate_fields_for_table(table_name, schema_dir):

    table_schema = '{dir}/{table}.json'.format(dir=schema_dir, table=table_name)

    with open(table_schema) as f:
        j_raw = json.load(f)

    target_fields = []

    for fld in j_raw:
        field_name = fld['name']
        field_type = fld['type']

        if field_type == 'DATETIME':
            field = 'PARSE_DATETIME(\'%Y-%m-%d %h:%m:s\', {field_name}) AS {field_name}'
        else:
            if field_type == 'DATE':
                field = 'PARSE_DATE(\'%Y-%m-%d\', {field_name}) AS {field_name}'
            else:
                field = field_name
        
        # plus comma
        if len(target_fields) < len(j_raw) - 1:
            field = field + ','

        target_fields.append(target_field)

    return target_fields



''' 
----------------------------------------------------
    generate_queries()
    returns a list of query strings
----------------------------------------------------
'''

def generate_queries(header, result_tables, config):

    s_queries = ['-- {0}\n\n'.format(header)]

    # config['bq_target_project']

    # to add list of fields and exclude not standard fields
    s_query = 'CREATE OR REPLACE TABLE `{target_project}`.{target_dataset}.{table} AS \n' + \
        'SELECT {fields} FROM `{cdm_project}`.{cdm_dataset}.{prefix}{table};\n'

    for t in result_tables:
        
        flds = generate_fields_for_table(t, config['schema_dir'])

        s_queries.append(s_query.format(
            target_project = config['bq_target_project'],
            target_dataset = config['bq_target_dataset'],
            cdm_project   = config['bq_cdm_project'],
            cdm_dataset   = config['bq_cdm_dataset'],
            prefix        = result_tables['prefix'],
            table         = t,
            fields        = flsds
            ))

    return s_queries

'''
----------------------------------------------------
    main()
    return code:
        0 = success
        ...
----------------------------------------------------
'''

def main():
    return_code = 0

    params = read_params()

    config = read_config(params.get('config_file'))

    print('Generating script...')

    s_cdm_tables = config['bq_tables']
    s_cdm_queries = generate_queries('Convert DATE and DATETIME fields', s_cdm_tables, config)

    with open(config['output_sql_file'], 'w') as f:

        f.write('-- bq_cdm_to_target generated script --\n\n')
        f.write('-- Unload to target')

        for s in s_cdm_queries:
            f.write(s)

        f.close()
        print('Generated', f.name)

    # for s in s_cdm_queries:
    #     rc = bq_run_script.run_query(s)
    #     if rc != 0:
    #         return_code += 1
    #         continue

    return return_code

# ----------------------------------------------------
# go
# ----------------------------------------------------
return_code = main()

exit(return_code)

