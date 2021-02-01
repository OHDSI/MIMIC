#
# Generate SQL script to run basic Unit Tests against CDM tables
# 

import os
import sys
import getopt
import json
import re
import datetime


# ----------------------------------------------------
'''
    python gen_bq_ut_basic.py
    To create config file, copy the config_default content to a .conf file and set the desired values
'''
# ----------------------------------------------------

config_default = {

    "etl_project":             "@etl_project",
    "etl_dataset":             "@etl_dataset",

    "metrics_project":         "@metrics_project",
    "metrics_dataset":         "@metrics_dataset",

    "cdm_prefix":               "cdm_",

    "cdm_ddl":                  "etl/ddl/ddl_cdm_5_3_1.sql",

    "output_sql_file":          "test/ut/ut_basic_gen.sql",

    "tables": [  
        {
            "table": "",
            "tst": [
                {
                    "source_field": "person_id"
                },
                {
                    "source_field": "death_date",
                    "fk_table": "`@source_project`.@core_dataset.admissions",
                    "fk_field": "deathtime",
                    "inactive_status": "any text helping you to remember why"
                }
            ]
        }
    ]
}

'''
    Supposing that output metrics table is already created (see ut_start.sql)
'''

# ----------------------------------------------------
# read_params()
# ----------------------------------------------------

def read_params():

    print('Read params...')
    params = {
        "config_file":      ""
    }

    
    # Parsing command line arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:],"c:",["config=,"])
        # if len(opts) == 0:
        #     raise getopt.GetoptError("read_params() error", "Mandatory argument is missing.")
    except getopt.GetoptError:
        print("Please indicate correct params:")
        print("config_file:    optional: indicate '-c' for 'config', local config json file")
        print("for example:\npython scripts/gen_bq_ut_basic.py -c test/ut/gen_bq_ut_basic.conf")
        sys.exit(2)

    st = []

    params['config_file'] = os.path.basename(__file__).replace('py', 'conf')

    for opt, arg in opts:
        if opt == '-c' or opt == '--config':
            if os.path.isfile(arg):
                params['config_file'] = arg


    print(params)
    return params

# ----------------------------------------------------
# read_config()
# ----------------------------------------------------

def read_config(config_file):
    
    print('Reading config...')
    config = {}
    config_read = {}

    if os.path.isfile(config_file):
        with open(config_file) as f:
            config_read = json.load(f)

    # local config has higher priority
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
        print(pr)
        s_tables = filter(lambda x: pr in x, s_ddl)

        # CREATE OR REPLACE TABLE dataset.cdm_cohort_definition (
        for s_t in s_tables:
            t = s_t.partition(pr)[2].partition('(')[0].strip()
            result_tables['tables'].append(t)
        print(result_tables['tables'][1])

    return result_tables

'''
----------------------------------------------------
    query templates
----------------------------------------------------
'''

q_header = "\n" +\
    "-- -------------------------------------------------------------------\n" + \
    "-- {header}\n" + \
    "-- -------------------------------------------------------------------\n" + \
    "\n"

''' 
----------------------------------------------------
    generate_unique_test()
    returns test query for the given table
----------------------------------------------------
'''
def generate_unique_test(table_config, config):

    q_template = "\n" + \
        "-- -------------------------------------------------------------------\n" + \
        "-- unique\n" + \
        "-- -------------------------------------------------------------------\n" + \
        "\n" + \
        "INSERT INTO `{metrics_project}`.{metrics_dataset}.report_unit_test\n" + \
        "SELECT\n" + \
        "    CAST(NULL AS STRING)                AS report_id,\n" + \
        "    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS\n" + \
        "    'cdm_{table_name}'          AS table_id,\n" + \
        "    'unique'                            AS test_type, -- unique, not null, concept etc.\n" + \
        "    '{source_field}'            AS field_name,\n" + \
        "    CAST(NULL AS STRING)                AS criteria_json,\n" + \
        "    (COUNT({source_field}) - COUNT(DISTINCT {source_field}) = 0) AS test_passed\n" + \
        "FROM\n" + \
        "    `{etl_project}`.{etl_dataset}.cdm_{table_name}\n" + \
        ";\n"


    result_queries = ""
    test_list = table_config.get('unique')
    if test_list != None:
        for tst in test_list:
            print(tst)
    
            if tst.get('inactive_status') == None:

                result_queries = result_queries + "\n" + \
                    q_template.format(
                        metrics_project = config['metrics_project'],
                        metrics_dataset = config['metrics_dataset'],
                        etl_project   = config['etl_project'],
                        etl_dataset   = config['etl_dataset'],
                        table_name    = table_config['table'].replace(config['cdm_prefix'], ''),
                        source_field  = tst['source_field']
                    )

    return result_queries 


''' 
----------------------------------------------------
    generate_fk_test()
    returns test query for the given table
----------------------------------------------------
'''
def generate_fk_test(table_config, config):

    q_template = "\n" + \
        "-- -------------------------------------------------------------------\n" + \
        "-- FK to {fk_table}.{fk_field}\n" + \
        "-- -------------------------------------------------------------------\n" + \
        "\n" + \
        "INSERT INTO `{metrics_project}`.{metrics_dataset}.report_unit_test\n" + \
        "SELECT\n" + \
        "    CAST(NULL AS STRING)                AS report_id,\n" + \
        "    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS\n" + \
        "    'cdm_{table_name}'          AS table_id,\n" + \
        "    'foreign key'                       AS test_type, -- unique, not null, concept etc.\n" + \
        "    '{source_field}'                         AS field_name,\n" + \
        "    CAST(NULL AS STRING)                AS criteria_json,\n" + \
        "    (COUNT(cdm.{source_field}) = 0)          AS test_passed -- FK source\n" + \
        "FROM\n" + \
        "    `{etl_project}`.{etl_dataset}.cdm_{table_name} cdm\n" + \
        "LEFT JOIN\n" + \
        "    {fk_table} fk\n" + \
        "        ON cdm.{source_field} = fk.{fk_field}\n" + \
        "WHERE\n" + \
        "    fk.{fk_field} IS NULL -- FK target\n" + \
        "{unit_id_clause}" + \
        ";\n"

    q_by_unit = "    AND cdm.unit_id = '{unit_id}'\n"

    result_queries = ""
    test_list = table_config.get('fk')
    if test_list != None:
        for tst in test_list:
            print(tst)

            if tst.get('inactive_status') == None:

                uic = q_by_unit.format(unit_id=tst['unit_id']) if tst.get('unit_id') != None else ''
                
                result_queries = result_queries + \
                    q_template.format(
                        metrics_project = config['metrics_project'],
                        metrics_dataset = config['metrics_dataset'],
                        etl_project   = config['etl_project'],
                        etl_dataset   = config['etl_dataset'],
                        table_name    = table_config['table'].replace(config['cdm_prefix'], ''),
                        source_field  = tst['source_field'],
                        fk_table      = tst['fk_table'],
                        fk_field      = tst['fk_field'],
                        unit_id_clause = uic
                    )

    return result_queries 

''' 
----------------------------------------------------
    generate_required_test()
    returns test query for the given table
----------------------------------------------------
'''
def generate_required_test(table_config, config):

    q_template = "\n" + \
        "-- -------------------------------------------------------------------\n" + \
        "-- required\n" + \
        "-- -------------------------------------------------------------------\n" + \
        "\n" + \
        "INSERT INTO `{metrics_project}`.{metrics_dataset}.report_unit_test\n" + \
        "SELECT\n" + \
        "    CAST(NULL AS STRING)                AS report_id,\n" + \
        "    FORMAT_DATETIME('%Y-%m-%d %X', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS\n" + \
        "    'cdm_{table_name}'          AS table_id,\n" + \
        "    'required'                            AS test_type, -- unique, not null, concept etc.\n" + \
        "    '{source_field}'            AS field_name,\n" + \
        "    CAST(NULL AS STRING)                AS criteria_json,\n" + \
        "    (COUNT(*) - COUNT({source_field}) = 0) AS test_passed\n" + \
        "FROM\n" + \
        "    `{etl_project}`.{etl_dataset}.cdm_{table_name}\n" + \
        ";\n"


    result_queries = ""
    test_list = table_config.get('required')
    if test_list != None:
        for tst in test_list:
            print(tst)
    
            if tst.get('inactive_status') == None:

                result_queries = result_queries + "\n" + \
                    q_template.format(
                        metrics_project = config['metrics_project'],
                        metrics_dataset = config['metrics_dataset'],
                        etl_project   = config['etl_project'],
                        etl_dataset   = config['etl_dataset'],
                        table_name    = table_config['table'].replace(config['cdm_prefix'], ''),
                        source_field  = tst['source_field']
                    )

    return result_queries 



''' 
----------------------------------------------------
    generate_queries()
    returns queries
    iterates through the given table list
----------------------------------------------------
'''
def generate_queries(header, config):

    s_queries = [q_header.format(header=header)]

    for t in config['tables']:
        s_queries.append(q_header.format(header=t['table']))
        s_queries.append(generate_unique_test(t, config))
        s_queries.append(generate_fk_test(t, config))
        s_queries.append(generate_required_test(t, config))

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


    s_header = 'MIMIC IV UT script generated {datetime} by {gen_script_name}'.format(
        datetime = datetime.datetime.now(),
        gen_script_name = os.path.basename(__file__)
    )

    s_test_queries = generate_queries(s_header, config)

    with open(config['output_sql_file'], 'w') as f:

        for s in s_test_queries:
            f.write(s)

        f.close()
        print('Generated', f.name)

    return return_code

# ----------------------------------------------------
# go
# ----------------------------------------------------
return_code = main()

exit(return_code)

