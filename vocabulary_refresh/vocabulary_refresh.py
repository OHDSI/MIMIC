'''
    Refresh OMOP Vocabularies and Custom Concepts
'''

# ----------------------------------------------------
'''
These tables are kept after clean-up:
    tmp_custom_mapping - the source table loaded from CSV files
    tmp_custom_concept_skipped 
        - output table with tmp_custom_mapping rows which were not processed because of errors

Known issue:
    need to stop if the previous step failed

'''
# ----------------------------------------------------

import os
import sys
import getopt
import json

# read params
# - what step does the user want to run
def read_params():

    params_description = {
        "step": "mandatory: indicate '-s' for 'step', integer step number" ,
        "config_file":   "optional: indicate '-c' for 'config', default is 'vocabulary_refresh.conf'"
    }
    params = {}

    step_values = {
        "0": "run all steps",

        "10": "run steps 11-13 for athena standard vocabularies",
        "11": "copy files to gs",
        "12": "load from gs to intermediate bq tables tmp_*",
        "13": "create final voc tables from intermediate",

        "20": "run steps 21-23 for custom mapping",
        "21": "copy files to gs",
        "22": "load from gs to intermediate bq table tmp_custom_mapping, verify the table",
        "23": "create custom concepts and its relationships in voc tables",

        "30": "run steps 31-32 for vocabulary tables",
        "31": "check vocabulary tables",
        "32": "clean up empty check tables"
    }
    
    print('Read params...')

    # Parsing command line arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:],"s:c:",["step=", "config="])

        params = {
            "step": -10,
            "config_file": "vocabulary_refresh.conf"
        }

        for opt, arg in opts:
            if opt in ['-s', '--step']:
                params['step'] = int(arg)
            if opt in ['-c', '--config']:
                params['config_file'] = arg

        if params['step'] < 0:
            raise getopt.GetoptError("read_params() error", "Mandatory argument is missing.")
        if not os.path.isfile(params['config_file']):
            raise getopt.GetoptError("read_params() error", "Config file '{f}' is not found".format(
                f=params['config_file']))

    except getopt.GetoptError as err:
        print(params)
        print(err.args)
        print("Please indicate correct params:")
        print(params_description)
        for k in sorted(step_values.keys()):
            s = "    {k} - {v}".format(k=k, v=step_values[k])
            print(s)
        sys.exit(2)

    print(params)
    return params

# read paths and target vocabulary dataset name
def read_config(config_file):

    print('Read config file...')
    config = {}

    if os.path.isfile(config_file):
        with open(config_file) as f:
            config = json.load(f)

    print(config_file)
    return config

# ----------------------------------------------------
# const
# ----------------------------------------------------

#####To refresh standard vocabularies#####

# ----------------------------------------------------
# main
# ----------------------------------------------------

def main():

    gsutil_rm_csv = "gsutil rm {target_path}/*.csv"
    gsutil_cp_csv = "gsutil cp {source_path}/*.csv {target_path}/"
    dataset_fullname = "`{project}.{dataset}`"
    run_command_load = "python load_to_bq_vocab.py --{step_name} --config {config_file}"
    run_command_bq_script = "python bq_run_script.py --config {config_file} {script_file}"

    params = read_params()
    config = read_config(params['config_file'])

    return_code = 0

    # 1. Download and unzip files from Athena
    # We start from from the point when Athena vocabularies are downloaded and CPT vocabulary is updated

    # 2. Copy files to GCP bucket
    if return_code == 0 and params['step'] in [11, 10, 0]:
        run_command = gsutil_rm_csv.format(target_path=config['gs_athena_csv_path'])
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

        run_command = gsutil_cp_csv.format(
            source_path=config['local_athena_csv_path'], target_path=config['gs_athena_csv_path'])
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

    # 3. Load files to intermediate BQ tables
    if return_code == 0 and params['step'] in [12, 10, 0]:
        run_command = run_command_load.format(step_name="athena", config_file=params['config_file'])
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

    # 4. Populate target vocabulary tables from intermediate tables
    if return_code == 0 and params['step'] in [13, 10, 0]:
        run_command = run_command_bq_script.format( \
            config_file=params['config_file'], script_file="create_voc_from_tmp.sql")
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)


    #####To refresh or add new custom mapping#####

    # 5. Copy custom mapping files to custom_mapping_csv/ folder, and update custom_mapping_list.tsv
    # It is a manual step

    # 6. Copy custom mapping files to GCP bucket
    if return_code == 0 and params['step'] in [21, 20, 0]:
        run_command = gsutil_rm_csv.format(target_path=config['gs_mapping_csv_path'])
        print(run_command)
        run_command = gsutil_cp_csv.format(
            source_path=config['local_mapping_csv_path'], target_path=config['gs_mapping_csv_path'])
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

    # 7. Load files to the intermediate BQ table (tmp_custom_mapping)
    if return_code == 0 and params['step'] in [22, 20, 0]:
        run_command = run_command_load.format(step_name="mapping", config_file=params['config_file'])
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

        run_command = run_command_bq_script.format( \
            config_file=params['config_file'], script_file="check_custom_loaded.sql")
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

    # 8. Add custom concepts to vocabulary tables from the intermediate table
    if return_code == 0 and params['step'] in [23, 20, 0]:
        run_command = run_command_bq_script.format( \
            config_file=params['config_file'], script_file="custom_vocabularies.sql")
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

    #####Verify the result#####

    # 9. Verify target tables
    if return_code == 0 and params['step'] in [31, 30, 0]:
        run_command = run_command_bq_script.format( \
            config_file=params['config_file'], script_file="vocabulary_check_bq.sql")
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

    # 10. Clean up temporary tables from the previous step
    # collect the list to clean up
    if return_code == 0 and params['step'] in [32, 30, 0]:
        run_command = run_command_bq_script.format( \
            config_file=params['config_file'], script_file="vocabulary_cleanup_bq.sql")
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

# ----------------------------------------------------
# go
# ----------------------------------------------------
main()

exit(0)
