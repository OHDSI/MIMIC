# ------------------------------------------------------------------------
#
# Generate etrics queries for event tables
# Run the generated queries in BigQuery
#
# ------------------------------------------------------------------------

import os
import sys
import json

# ----------------------------------------------------
# default config values
# To override default config values, copy the keys to be overriden to a json file,
# and indicate this file as --config parameter
# ----------------------------------------------------

config_default = {

    "variables": {

        "omop_db":      '`@etl_project`.@etl_dataset',
        "result_db":    '`@metrics_project`.@metrics_dataset',

        "config_to_read": "qa_fields.conf",
        "config_to_write": "qa_fields_generated.conf",

        "ddl_script": "../etl/ddl/ddl_cdm_5_3_1.sql",
        "cdm_tables_config": "cdm_tables.conf",

        "qa_report_table": "report_qa_test",

        "qa_generated_script":  "qa_generated.sql"
    }
    
}

# ------------------------------------------------------------------------
# query templates
# ------------------------------------------------------------------------

# ------------------------------------------------------------------------
# counts
# ------------------------------------------------------------------------

bq_qa_counts_cdm_insert = \
    '-- -------------------------------------------------------------------\n'
    '-- QA template for {omop_table} table\n'
    '-- -------------------------------------------------------------------\n'
    'INSERT INTO {result_db}.{qa_report_table}\n' + \
    'SELECT\n' + \
    '    CAST(NULL AS STRING)                AS report_id, -- task_id, run_id, target_dataset etc.\n' + \
    '    FORMAT_DATETIME(\'%Y-%m-%d %X\', CURRENT_DATETIME()) AS report_starttime, -- X = HH:MM:SS\n' + \
    '    \'{omop_table}\'                  AS table_id,\n' + \
    '    src.unit_id                         AS unit_id,\n' + \
    '    CAST(NULL AS STRING)                AS field_name,\n' + \
    '    \'row_counts are equal in src and cdm\'           AS test_description,\n' + \
    '    CAST(NULL AS STRING)                AS condition_json, -- for some complex conditions\n' + \
    '    (src.row_count - cdm.row_count = 0) AS test_passed\n' + \
    'FROM\n' + \
    '(\n' + \
    '{src_subselect}\n' +\
    ') src\n' + \
    'LEFT JOIN\n' + \
    '(\n' + \
    '    SELECT unit_id, COUNT(*) AS row_count\n' + \
    '    FROM\n' + \
    '        {omop_db}.{omop_table}\n' + \
    ') cdm\n' + \
    '    ON src.unit_id = cdm.unit_id\n' + \
    ';\n\n'

bq_qa_counts_src_subselect = \
    '{union} ' + \
    '    SELECT \n' + \
    '        \'{omop_table}.{src_table}\' AS unit_id, COUNT(*) AS row_count\n' + \
    '    FROM\n' + \
    '        {omop_db}.{src_table}\n' + \
    '    WHERE\n' + \
    '        {src_where}\n' + \

bq_qa_counts_cdm_variables = {
    "result_db",
    "qa_report_table",
    "omop_db",
    "omop_table",
    "src_subselect"
}

bq_qa_counts_src_variables = {
    "union"
    "omop_db",
    "omop_table",
    "src_table",
    "src_where"
}


# ------------------------------------------------------------------------
# udf
# ------------------------------------------------------------------------

# ------------------------------------------------------------------------
# Get concept fields and Co from locally generated conf
# {tables: [{table, fields: [{ concept_field, source_value_field}]}]}
# ------------------------------------------------------------------------
def get_target_tables_from_conf(tables_config_filename):

    with open(tables_config_filename) as f:
        target_tables = json.load(f)

    return target_tables

# ------------------------------------------------------------------------
# Generate executable sql for totals
# ------------------------------------------------------------------------
def generate_queries_counts(target_tables):
    queries = []

    s_query = bq_totals_create.format(result_db = result_db, result_table = totals_table)
    queries.append(s_query)

    for j_table in target_tables:
        
        omop_table = j_table.get('table')

        s_query = bq_totals_insert.format(
                result_db = result_db, totals_table = totals_table,
                omop_db = omop_db, omop_prefix = omop_prefix, omop_table = omop_table
            )
        queries.append(s_query)

    return queries

# ------------------------------------------------------------------------
# Save the generated queries to an sql file
# ------------------------------------------------------------------------
def save_queries_to_file(queries, query_file):
    # path to this script relatively to the current dir
    work_dir = os.path.dirname(__file__)
    work_file = os.path.join(work_dir, query_file)
    with open(work_file, 'w') as f:
        for q in queries:
            f.write(q)
        f.close()
    return work_file

# ------------------------------------------------------------------------
# main
# ------------------------------------------------------------------------

def main():

    # read parameters
    # ddl config
    config_filename = config_file_default
    # read actual
    if len(sys.argv) == 2:
        config_filename = sys.argv[1]
    # tables config
    #   todo
    # run / do not run generated queries
    #   todo
    # -----

    # generate our conf from standard ddl conf
    print('generate from {file}\n'.format(file=config_filename))
    target_tables = get_target_tables_from_ddl(config_filename, tables_config_filename)

    # # read conf from previously generated file
    # work_dir = os.path.dirname(__file__)
    # work_file = os.path.join(work_dir, tables_config_filename)
    # target_tables = get_target_tables_from_conf(work_file)

    # queries

    queries = generate_queries_totals(target_tables)
    result_file = save_queries_to_file(queries, query_totals_file)   

    print('queries have been saved to {0}'.format(result_file))


    # return

# run
main()
