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

        "10": "run step 11 to generate complete vocabulary tables",
        "11": "combine standard vocabulary / athena tables with custom _delta tables",

        "20": "run steps 21-23 to copy to and setup tables on BigQuery",
        "21": "copy combined athena + _delta tables to gs bucket",
        "22": "load from gs to intermediate bq tables tmp_*",
        "23": "create final voc tables from intermediate",

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
    run_command_load = "python load_to_bq_vocab.py --config {config_file}"
    run_append_delta_tables = "python append_athena_delta_tables.py --config {config_file}"
    # run_command_bq_script = "python bq_run_script.py --config {config_file} {script_file}"
    run_command_bq_script = "python bq_run_script.py -c {config_file} {script_file}"

    params = read_params()
    config = read_config(params['config_file'])

    return_code = 0

    # 1. Download and unzip files from Athena
    # We start from the point when Athena vocabularies are downloaded and CPT vocabulary is updated

    # 2. Combine Athena tables with custom mapping _delta tables
    if return_code == 0 and params['step'] in [11, 10, 0]:
        run_command = run_append_delta_tables.format(config_file=params['config_file'])
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

    # 3. Copy files to GCP bucket
    if return_code == 0 and params['step'] in [21, 20, 0]:
        run_command = gsutil_rm_csv.format(target_path=config['gs_athena_csv_path'])
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

        run_command = gsutil_cp_csv.format(
            source_path=config['combined_athena_delta_tables_path'], target_path=config['gs_athena_csv_path'])
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

    # 4. Load files to intermediate BQ tables
    if return_code == 0 and params['step'] in [22, 20, 0]:
        run_command = run_command_load.format(config_file=params['config_file'])
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

    # 5. Populate target vocabulary tables from intermediate tables
    if return_code == 0 and params['step'] in [23, 20, 0]:
        run_command = run_command_bq_script.format( \
            config_file=params['config_file'], script_file="create_voc_from_tmp.sql")
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

    # 6. Verify target tables
    if return_code == 0 and params['step'] in [31, 30, 0]:
        run_command = run_command_bq_script.format( \
            config_file=params['config_file'], script_file="vocabulary_check_bq.sql")
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

    # 7. Clean up temporary tables from the previous step
    # collect the list to clean up
    if return_code == 0 and params['step'] in [32, 30, 0]:
        run_command = run_command_bq_script.format( \
            config_file=params['config_file'], script_file="vocabulary_cleanup_bq_m.sql") # remove only empty check tables
        print(run_command)
        return_code = os.system(run_command)
        print("return_code", return_code)

# ----------------------------------------------------
# go
# ----------------------------------------------------
main()

exit(0)
