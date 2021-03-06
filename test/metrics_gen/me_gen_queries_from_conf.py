# ------------------------------------------------------------------------
#
# Generate etrics queries for event tables
# Run the generated queries in BigQuery
#
# ------------------------------------------------------------------------

import os
import sys
import json

# ------------------------------------------------------------------------
# const
# ------------------------------------------------------------------------

omop_prefix = ''
result_prefix = 'me_top_'

omop_db = '`@etl_project`.@etl_dataset'
result_db = '`@metrics_project`.@metrics_dataset'

totals_table = 'me_total'
mapping_rate_table = 'me_mapping_rate'
tops_together_table = 'me_tops_together'

# ouput files
query_top100_file = 'me_top100_from_conf.sql'
query_totals_file = 'me_totals_from_conf.sql'
query_see_result_file = 'me_see_result_from_conf.sql'
query_mapping_rate_file = 'me_mapping_rate_from_conf.sql'
query_tops_together_file = 'me_tops_together_from_conf.sql'

# source config file - ddl
config_file_default = 'cdm_tables.conf'
# work config file - can be generated by this script and edited
tables_config_filename = 'me_concept_fields.conf'

# ------------------------------------------------------------------------
# query templates
# ------------------------------------------------------------------------

# ------------------------------------------------------------------------
# totals
# ------------------------------------------------------------------------

bq_totals_create = \
    'DROP TABLE IF EXISTS {result_db}.{result_table};\n' + \
    'CREATE TABLE {result_db}.{result_table}\n' + \
    '(\n' + \
    '    table_name        STRING,\n' + \
    '    count             INT64\n' + \
    ');\n\n'

bq_totals_insert = \
    'INSERT INTO {result_db}.{totals_table}\n' + \
    'SELECT\n' + \
    '    \'{omop_table}\'     AS table_name,\n' + \
    '    COUNT(*)             AS count\n' + \
    'from {omop_db}.{omop_prefix}{omop_table} ev\n' + \
    ';\n\n'


bq_mapping_rate_create = \
    'DROP TABLE IF EXISTS {result_db}.{result_table};\n' + \
    'CREATE TABLE {result_db}.{result_table}\n' + \
    '(\n' + \
    '    table_name        STRING,\n' + \
    '    concept_field     STRING,\n' + \
    '    count             INT64,\n' + \
    '    percent           FLOAT64,\n' + \
    '    total             INT64\n' + \
    ');\n\n'

# without totals_table
bq_mapping_rate_insert = \
    'INSERT INTO {result_db}.{mapping_rate_table}\n' + \
    'SELECT\n' + \
    '    \'{omop_table}\'        AS table_name,\n' + \
    '    \'{concept_field}\'     AS concept_field,\n' + \
    '    COUNT(CASE WHEN {concept_field} > 0 THEN 1 ELSE NULL END)      AS count,\n' + \
    '    IF(COUNT(*) > 0,\n' + \
    '        ROUND(CAST(COUNT(IF({concept_field} > 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),\n' + \
    '        0)    AS percent,\n' + \
    '    COUNT(*)                AS total\n' + \
    'FROM {omop_db}.{omop_prefix}{omop_table} ev\n' + \
    'WHERE {concept_field} IS NOT NULL\n' + \
    ';\n\n'
# to calculate value_as_concept_id etc MR right, consider only not null concept_id
#    'WHERE {source_value_field} IS NOT NULL\n' + \

# # with totals_table
# bq_mapping_rate_insert = \
#     'INSERT INTO {result_db}.{mapping_rate_table}\n' + \
#     'SELECT\n' + \
#     '    \'{omop_table}\'        AS table_name,\n' + \
#     '    \'{concept_field}\'     AS concept_field,\n' + \
#     '    COUNT(*)                AS count,\n' + \
#     '    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2)    AS percent,\n' + \
#     '    tt.count                AS total\n' + \
#     'FROM {omop_db}.{omop_prefix}{omop_table} ev\n' + \
#     'INNER JOIN {result_db}.{totals_table} tt\n' + \
#     '    ON tt.table_name = \'{omop_table}\'\n' + \
#     'WHERE ev.{concept_field} <> 0\n' + \
#     'GROUP BY tt.count\n' + \
#     ';\n\n'

# ------------------------------------------------------------------------
# tops
# ------------------------------------------------------------------------

bq_top_create = \
    'DROP TABLE IF EXISTS {result_db}.{result_table};\n' + \
    'CREATE TABLE {result_db}.{result_table}\n' + \
    '(\n' + \
    '    concept_field     STRING,\n' + \
    '    category          STRING,\n' + \
    '    source_value      STRING,\n' + \
    '    concept_id        INT64,\n' + \
    '    concept_name      STRING,\n' + \
    '    count             INT64,\n' + \
    '    percent           FLOAT64\n' + \
    ');\n\n'

# # without totals_table

# bq_mapped_insert = \
#     'INSERT INTO {result_db}.{result_table}\n' + \
#     'SELECT\n' + \
#     '    \'{concept_field}\'     AS concept_field,\n' + \
#     '    \'Mapped\'              AS category,\n' + \
#     '    CAST(ev.{source_value_field} AS STRING)  AS source_value,\n' + \
#     '    vc.concept_id           AS concept_id,\n' + \
#     '    vc.concept_name         AS concept_name,\n' + \
#     '    COUNT(IF(ev.{concept_field} <> 0, 1, NULL)) AS count,\n' + \
#     '    IF(COUNT(*) > 0,\n' + \
#     '        ROUND(CAST(COUNT(IF(ev.{concept_field} <> 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),\n' + \
#     '        0)    AS percent,\n' + \
#     'FROM {omop_db}.{omop_prefix}{omop_table} ev\n' + \
#     'LEFT JOIN {omop_db}.voc_concept vc\n' + \
#     '    on ev.{concept_field} = vc.concept_id\n' + \
#     'WHERE ev.{source_value_field} IS NOT NULL \n' + \
#     'GROUP BY ev.{source_value_field}, vc.concept_id, vc.concept_name\n' + \
#     'ORDER BY count DESC\n' + \
#     'LIMIT 100\n' + \
#     ';\n\n'

# bq_unmapped_insert = \
#     'INSERT INTO {result_db}.{result_table}\n' + \
#     'SELECT\n' + \
#     '    \'{concept_field}\'      AS concept_field,\n' + \
#     '    \'Unmapped\'            AS category,\n' + \
#     '    CAST(ev.{source_value_field} AS STRING)  AS source_value,\n' + \
#     '    NULL                    AS concept_id,\n' + \
#     '    CAST(NULL AS STRING)    AS concept_name,\n' + \
#     '    COUNT(IF(COALESCE(ev.{concept_field}, 0) = 0, 1, NULL)) AS count,\n' + \
#     '    IF(COUNT(*) > 0,\n' + \
#     '        ROUND(CAST(COUNT(IF(COALESCE(ev.{concept_field}, 0) = 0, 1, NULL)) AS FLOAT64) / COUNT(*) * 100, 2),\n' + \
#     '        0)    AS percent,\n' + \
#     'FROM {omop_db}.{omop_prefix}{omop_table} ev\n' + \
#     'WHERE ev.{source_value_field} IS NOT NULL\n' + \
#     'GROUP BY ev.{source_value_field}\n' + \
#     'ORDER BY count desc\n' + \
#     'LIMIT 100\n' + \
#     ';\n\n'

# with totals_table

bq_mapped_insert = \
    'INSERT INTO {result_db}.{result_table}\n' + \
    'SELECT\n' + \
    '    \'{concept_field}\'     AS concept_field,\n' + \
    '    \'Mapped\'              AS category,\n' + \
    '    CAST(ev.{source_value_field} AS STRING)  AS source_value,\n' + \
    '    vc.concept_id           AS concept_id,\n' + \
    '    vc.concept_name         AS concept_name,\n' + \
    '    COUNT(*)                AS count,\n' + \
    '    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent\n' + \
    'from {omop_db}.{omop_prefix}{omop_table} ev\n' + \
    'inner join {omop_db}.voc_concept vc\n' + \
    '    on ev.{concept_field} = vc.concept_id\n' + \
    'inner join {result_db}.{totals_table} tt\n' + \
    '    on tt.table_name = \'{omop_table}\'\n' + \
    'where ev.{concept_field} <> 0\n' + \
    'group by ev.{source_value_field}, vc.concept_id, vc.concept_name, tt.count\n' + \
    'order by count desc\n' + \
    'limit 100\n' + \
    ';\n\n'

bq_unmapped_insert = \
    'INSERT INTO {result_db}.{result_table}\n' + \
    'SELECT\n' + \
    '    \'{concept_field}\'      AS concept_field,\n' + \
    '    \'Unmapped\'            AS category,\n' + \
    '    CAST(ev.{source_value_field} AS STRING)  AS source_value,\n' + \
    '    NULL                    AS concept_id,\n' + \
    '    CAST(NULL AS STRING)    AS concept_name,\n' + \
    '    COUNT(*)                AS count,\n' + \
    '    ROUND(CAST(count(*) AS FLOAT64) / tt.count * 100, 2) AS percent\n' + \
    'from {omop_db}.{omop_prefix}{omop_table} ev\n' + \
    'inner join {result_db}.{totals_table} tt\n' + \
    '    on tt.table_name = \'{omop_table}\'\n' + \
    'where ev.{concept_field} = 0\n' + \
    'group by ev.{source_value_field}, tt.count\n' + \
    'order by count desc\n' + \
    'limit 100\n' + \
    ';\n\n'

# ------------------------------------------------------------------------
# see results
# ------------------------------------------------------------------------

bq_see_result = \
    'SELECT *\n' + \
    'from {result_db}.{result_prefix}{omop_table} rt\n' + \
    'order by concept_field, category, count desc\n' + \
    ';\n\n'


bq_tops_together_create = \
    'DROP TABLE IF EXISTS {result_db}.{result_table};\n' + \
    'CREATE TABLE {result_db}.{result_table}\n' + \
    '(\n' + \
    '    table_name        STRING,\n' + \
    '    concept_field     STRING,\n' + \
    '    category          STRING,\n' + \
    '    source_value      STRING,\n' + \
    '    concept_id        INT64,\n' + \
    '    concept_name      STRING,\n' + \
    '    count             INT64,\n' + \
    '    percent           FLOAT64\n' + \
    ');\n\n'

bq_tops_together_insert = \
    'INSERT INTO {result_db}.{result_table}\n' + \
    'SELECT\n' + \
    '    \'{omop_table}\'      AS table_name,\n' + \
    '    *\n' + \
    'from {result_db}.{result_prefix}{omop_table} rt\n' + \
    'order by concept_field, category, count desc\n' + \
    ';\n\n'


# ------------------------------------------------------------------------
# udf
# ------------------------------------------------------------------------

# ------------------------------------------------------------------------
# Get concept fields and Co from OMOP ddl conf
# {tables: [{table, fields: [field]}]}
# ------------------------------------------------------------------------
def get_target_tables_from_ddl(source_config_filename, output_config_filename):

    with open(source_config_filename) as f:
        j_raw = json.load(f)

    target_tables = []

    for table in j_raw['tables']:
        table_name = table['table']
        fields = table['fields']

        concept_fields = []
        source_value_fields = []

        for field in fields:
            field_name = field['field']
            if 'concept_id' in field_name:
                concept_fields.append(field_name)
            if 'source_value' in field_name:
                source_value_fields.append(field_name)

        target_fields = []

        for fld in concept_fields:
            # try 1
            fld_val = fld.replace('concept_id', 'source_value')
            if fld_val not in source_value_fields:
                fld_val = ''
                # try 2
                fld_val = fld.replace('source_concept_id', 'source_value')
                if fld_val not in source_value_fields:
                    fld_val = ''

            target_field = {"concept_field": fld, "source_value_field": fld_val}
            target_fields.append(target_field)

        if len(target_fields) > 0:
            target_tables.append( 
                { "table": table_name.replace(omop_prefix, ''), "fields": target_fields }
            )

    # path to this script relatively to the current dir
    work_dir = os.path.dirname(__file__)
    work_file = os.path.join(work_dir, output_config_filename + '.orig')

    with open(work_file, 'w') as f:
        f.write(json.dumps(target_tables, sort_keys=False, indent=4, separators=(',', ': ')))
        f.close()

    return target_tables

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
def generate_queries_totals(target_tables):
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
# Generate executable sql for top 100s
# ------------------------------------------------------------------------
def generate_queries_top100(target_tables):
    queries = []

    for j_table in target_tables:
        
        omop_table = j_table.get('table')
        j_fields = j_table.get('fields')

        result_table = result_prefix + j_table.get('table')
        
        s_query = bq_top_create.format(result_db = result_db, result_table = result_table)
        queries.append(s_query)

        for j_field in j_fields:
            
            concept_field = j_field.get('concept_field')
            source_value_field = j_field.get('source_value_field')

            # generate mapped for all
            if source_value_field == '': source_value_field = concept_field
            s_query = bq_mapped_insert.format(
                    result_db = result_db, result_table = result_table,
                    omop_db = omop_db, omop_prefix = omop_prefix, omop_table = omop_table,
                    concept_field = concept_field, 
                    source_value_field = source_value_field,
                    totals_table = totals_table # see commented out "with totals_table"
                )
            queries.append(s_query)

            # generate unmapped only if source_value_field is defined
            if source_value_field != '':

                s_query = bq_unmapped_insert.format(
                        result_db = result_db, result_table = result_table,
                        omop_db = omop_db, omop_prefix = omop_prefix, omop_table = omop_table,
                        concept_field = concept_field, 
                        source_value_field = source_value_field,
                        totals_table = totals_table # see commented out "with totals_table"
                    )
                queries.append(s_query)

    return queries

# ------------------------------------------------------------------------
# Generate executable sql for mapping_rate
# ------------------------------------------------------------------------
def generate_queries_mapping_rate(target_tables):
    queries = []

    s_query = bq_mapping_rate_create.format(result_db = result_db, result_table = mapping_rate_table)
    queries.append(s_query)

    for j_table in target_tables:
        
        omop_table = j_table.get('table')
        j_fields = j_table.get('fields')

        for j_field in j_fields:
            
            concept_field = j_field.get('concept_field')
            source_value_field = j_field.get('source_value_field')
            if source_value_field == '': source_value_field = concept_field

            s_query = bq_mapping_rate_insert.format(
                    result_db = result_db, mapping_rate_table = mapping_rate_table,
                    omop_db = omop_db, omop_prefix = omop_prefix, omop_table = omop_table,
                    concept_field = concept_field,
                    source_value_field = source_value_field
                    # totals_table = totals_table # see commented out "with totals_table"
                )
            queries.append(s_query)

    return queries

# ------------------------------------------------------------------------
# Generate executable sql to select and view results
# ------------------------------------------------------------------------
def generate_queries_see_result(target_tables):
    queries = []

    for j_table in target_tables:
        
        omop_table = j_table.get('table')

        s_query = bq_see_result.format(
                result_db = result_db,
                result_prefix = result_prefix, omop_table = omop_table
            )
        queries.append(s_query)

    return queries

# ------------------------------------------------------------------------
# Generate executable sql for putting tops together
# ------------------------------------------------------------------------
def generate_queries_tops_together(target_tables):
    queries = []

    s_query = bq_tops_together_create.format(result_db = result_db, result_table = tops_together_table)
    queries.append(s_query)

    for j_table in target_tables:
        
        omop_table = j_table.get('table')

        s_query = bq_tops_together_insert.format(
                result_db = result_db, result_table = tops_together_table,
                result_prefix=result_prefix, omop_table = omop_table
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

    # # generate our conf from standard ddl conf
    # print('generate from {file}\n'.format(file=config_filename))
    # target_tables = get_target_tables_from_ddl(config_filename, tables_config_filename)

    # read conf from previously generated file
    work_dir = os.path.dirname(__file__)
    work_file = os.path.join(work_dir, tables_config_filename)
    target_tables = get_target_tables_from_conf(work_file)

    # queries

    queries = generate_queries_totals(target_tables)
    result_file = save_queries_to_file(queries, query_totals_file)   

    print('queries have been saved to {0}'.format(result_file))

    queries = generate_queries_top100(target_tables)
    result_file = save_queries_to_file(queries, query_top100_file)   

    print('queries have been saved to {0}'.format(result_file))

    queries = generate_queries_mapping_rate(target_tables)
    result_file = save_queries_to_file(queries, query_mapping_rate_file)   

    print('queries have been saved to {0}'.format(result_file))

    queries = generate_queries_tops_together(target_tables)
    result_file = save_queries_to_file(queries, query_tops_together_file)   

    print('queries have been saved to {0}'.format(result_file))

    queries = generate_queries_see_result(target_tables)
    result_file = save_queries_to_file(queries, query_see_result_file)   

    print('queries have been saved to {0}'.format(result_file))

    # return

    # bq_command = "bq query --use_legacy_sql=false \"{query}\""
    # s_done = []
    # for s_query in queries:
    #     bqc = bq_command.format(query=s_query)
    #     # print(s_query)
    #     os.system(bqc)
    #     s_done.append(s_query.split('\n')[1])


    # print('Done. See me_ tables in dataset {0}.'.format(result_db))

    # print('Now in ' + result_db)
    # os.system("bq ls " + result_db)
    # print('\nCommands executed:')
    # for a in s_done:
    #     print(a)


# run
main()
