#
# Generate SQL script to populate ATLAS compatible dataset from CDM tables
# 

import os
import sys
import getopt
import json
import re
# import bq_run_script

# ----------------------------------------------------
'''
    python gen_bq_cdm_to_atlas.py
    To create config file, copy the config_default content to a .conf file and set the desired values
    ATLAS dataset name template: name_dateref_cdm_version (i.e. synpuf_2018q3_cdm_53)
'''
# ----------------------------------------------------

config_default = {

    "bq_cdm_project":           "a_project",
    "bq_cdm_dataset":           "a_dataset",

    "bq_atlas_project":         "a_project",
    "bq_atlas_dataset":         "name_dateref_cdm_version",

    "cdm_prefix":               "cdm_",
    "voc_prefix":               "voc_",

    "voc_ddl":                  "../etl/ddl_voc_5_3_1.sql",
    "cdm_ddl":                  "../etl/ddl_cdm_5_3_1.sql",

    "output_sql_file":          "../etl/output_file.sql"
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
        print("for example:\npython gen_bq_cdm_to_atlas.py --config gen_bq_cdm_to_atlas.conf")
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

''' 
----------------------------------------------------
    read_ddl()
    extracts names of tables with given prefix created by the given script
    returns a list of tables without prefix
----------------------------------------------------
'''

def read_ddl(ddl_file, cdm_dataset, prefix):

    result_tables = {
        "prefix": prefix,
        "tables": []
    }

    if os.path.isfile(ddl_file):

        s_raw = open(ddl_file).read().split('\n')
        s_ddl = filter(lambda x: x.strip()[0:2] != '--', s_raw)
        pr = cdm_dataset + '.' + prefix
        print('read_ddl.tables_template', pr)
        s_tables = filter(lambda x: pr in x, s_ddl)

        # CREATE OR REPLACE TABLE dataset.cdm_cohort_definition (
        for s_t in s_tables:
            t = s_t.partition(pr)[2].partition('(')[0].strip()
            result_tables['tables'].append(t)

            print('read_ddl.s_t', s_t)
            print('read_ddl.t', t)
            break

        # print(result_tables['tables'][1])

    return result_tables

''' 
----------------------------------------------------
    generate_queries()
    returns a list of query strings
----------------------------------------------------
'''

def generate_queries(header, result_tables, config):

    s_queries = ['-- {0}\n\n'.format(header)]

    # config['bq_atlas_project']

    # to add list of fields and exclude not standard fields
    s_query = 'CREATE OR REPLACE TABLE `{atlas_project}`.{atlas_dataset}.{table} AS \n' + \
        'SELECT * FROM `{cdm_project}`.{cdm_dataset}.{prefix}{table};\n'

    for t in result_tables['tables']:
        s_queries.append(s_query.format(
            atlas_project = config['bq_atlas_project'],
            atlas_dataset = config['bq_atlas_dataset'],
            cdm_project   = config['bq_cdm_project'],
            cdm_dataset   = config['bq_cdm_dataset'],
            prefix        = result_tables['prefix'],
            table         = t
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

    s_cdm_tables = read_ddl(config['cdm_ddl'], config['bq_cdm_dataset'], config['cdm_prefix'])
    s_cdm_queries = generate_queries('Copy CDM tables', s_cdm_tables, config)


    s_voc_tables = read_ddl(config['voc_ddl'], config['bq_cdm_dataset'], config['voc_prefix'])
    s_voc_queries = generate_queries('Copy Vocabulary tables', s_voc_tables, config)

    with open(config['output_sql_file'], 'w') as f:

        f.write('-- bq_cdm_to_atlas generated script --\n\n')
        f.write('-- Unload to ATLAS')

        for s in s_voc_queries:
            f.write(s)

        f.write('\n')
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

