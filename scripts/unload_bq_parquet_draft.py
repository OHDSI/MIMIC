#
# Unload hive tables to BigQuery
#
# Version to create Achilles compatible tables
# 

# pyspark --packages com.databricks:spark-avro_2.11:4.0.0

# for report/**/*.json
# python webreports/manage.py eject epic_ehr.dev

## -- go

from pyspark.sql import SparkSession
import os

# 0 = don't, 1 = do
do_load_cdm = 1
do_load_voc = 1
do_backup_cdm = 0
do_backup_voc = 0
# do_remove_dev_features = 0

# dev databases
db_hive_cdm = 'starr_epic_etl_ref'
db_hive_voc = 'vocabulary2_ref'

# dev etl config
db_bq_cdm = 'starr_epic_staging'
db_bq_cdm_bak = ''
gs_path_cdm = 'gs://starr_epic_etl_ext/parquet/'

# dev voc config
db_bq_voc = 'starr_epic_vocabulary'
db_bq_voc_bak = 'starr_epic_vocabulary_0514'
gs_path_voc = 'gs://starr_epic_vocabulary/parquet/'

file_extension = 'parquet'
# file_format_name = 'com.databricks.spark.avro'
file_format_name = 'parquet'
bq_load_command = "bq --location=US load --replace --source_format=PARQUET {table_name} \"{files_path}\""
bq_backup_command = "bq cp --force {src_table_name} {dst_table_name}"

# tables list
tables = [
'cdm_care_site',
'cdm_cdm_source',
'cdm_cohort',
'cdm_cohort_attribute',
'cdm_condition_era',
'cdm_condition_occurrence',
'cdm_cost',
'cdm_death',
'cdm_device_exposure',
'cdm_dose_era',
'cdm_drug_era',
'cdm_drug_exposure',
'cdm_fact_relationship',
'cdm_location',
'cdm_measurement',
'cdm_metadata',
'cdm_note',
'cdm_note_nlp',
'cdm_observation',
'cdm_observation_period',
'cdm_payer_plan_period',
'cdm_person',
'cdm_procedure_occurrence',
'cdm_provider',
'cdm_source_to_concept_map',
'cdm_specimen',
'cdm_visit_detail',
'cdm_visit_occurrence',
'join_voc_drug',
'lk_clean_drug',
'lk_clean_drug_codes',
'lk_clean_drug_med_admin',
'lk_clean_drug_order_med',
'lk_clean_dx',
'lk_clean_mapped_drug',
'lk_clean_mapped_dx',
'lk_clean_mapped_meas',
'lk_clean_mapped_obs',
'lk_clean_mapped_proc',
'lk_clean_meas',
'lk_clean_obs',
'lk_clean_proc',
'lk_concept_drug',
'lk_concept_dx',
'lk_concept_meas',
'lk_concept_obs',
'lk_concept_proc',
'lk_concept_route',
'lk_drug_codes_ndc',
'lk_drug_codes_rx0',
'lk_drug_codes_rx1',
'lk_person',
'stcm_invalid',
'stcm_valid'
]

vocs = [
    'voc_concept',
    'voc_concept_ancestor',
    'voc_concept_class',
    'voc_concept_relationship',
    'voc_concept_synonym',
    'voc_domain',
    'voc_drug_strength',
    'voc_relationship',
    'voc_vocabulary'
]

done = []

spark = SparkSession.builder.appName('unload_bq_dev').enableHiveSupport().getOrCreate()


for table in tables:
    if do_backup_cdm == 1:
        bqc = bq_backup_command.format(src_table_name = db_bq_cdm + '.' + table, \
            dst_table_name = db_bq_cdm_bak + '.' + table)
        print('backup BQ table: ' + bqc)
        os.system(bqc)
        done.append(bqc)

    if do_load_cdm == 1:        
        print('Unloading ' + db_hive_cdm + '.' + table)
        os.system("gsutil rm " + gs_path_cdm + table + '/*')
        df = spark.table(db_hive_cdm + '.' + table)
        print('To gs: ' + gs_path_cdm + table)
        df.write.format(file_format_name).save(gs_path_cdm + table)
    
        bqc = bq_load_command.format(table_name = db_bq_cdm + '.' + table, \
            files_path = gs_path_cdm + table + '/*.' + file_extension)
        print('To BQ: ' + bqc)
        os.system(bqc)
        done.append(bqc)


for table in vocs:
    if do_backup_voc == 1:
        bqc = bq_backup_command.format(src_table_name = db_bq_voc + '.' + table, \
            dst_table_name = db_bq_backup + '.' + table)
        print('backup BQ table: ' + bqc)
        os.system(bqc)
        done.append(bqc)

    if do_load_voc == 1:        
        print('Unloading ' + db_hive_voc + '.' + table)
        os.system("gsutil rm " + gs_path_voc + table + '/*')
        df = spark.table(db_hive_voc + '.' + table)
        print('To gs: ' + gs_path_voc + table)
        df.write.format(file_format_name).save(gs_path_voc + table)
    
        bqc = bq_load_command.format(table_name = db_bq_voc + '.' + table, \
            files_path = gs_path_voc + table + '/*.' + file_extension)
        print('To BQ: ' + bqc)
        os.system(bqc)
        done.append(bqc)

# print('Now in ' + db_bq)
# os.system("bq ls " + db_bq)
print('\nCommands executed:')
for a in done:
    print(a)
