#
# Load mimiciii CSV custom mapping from local path to BigQuery
#
# Run from current directory
# 

import os
import sys
import getopt
import json

# ----------------------------------------------------
'''
    python bq_load_mimiciii_custom_mapping.py
'''
# ----------------------------------------------------

config_default = {

    "bq_target_project":        "odysseus-mimic-dev",
    "bq_target_dataset":        "mimiciii_extras_concept",

    "local_mapping_csv_path":   "../../mimic-omop/extras/concept",
    "files_extension":          ".csv",
    "mapping_csv_delimiter":    ",",
    "mapping_csv_quote":        "\\\""
}

# ----------------------------------------------------
# read_params()
# ----------------------------------------------------

def read_params():

    print('Read params...')
    params = {
        "config_file":   "optional: indicate '-c' for 'config', json file name. Defaults are hard-coded"
    }
    
    # Parsing command line arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:],"c:",["config="])
        # if len(opts) == 0:
        #     raise getopt.GetoptError("read_params() error", "Mandatory argument is missing.")
    except getopt.GetoptError:
        print("Please indicate correct params:")
        print(params)
        print("for example:\npython bq_load_mimiciii_custom_mapping.py --config config.conf")
        sys.exit(2)

    st = []
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
    load_table()
    return codes: 0, 1, 2
----------------------------------------------------
'''

def load_table(table, file_path, config):

    return_code = 0

    bq_table = '{dataset}.{prefix}{table}'

    bq_load_command = \
        "bq --location=US load --replace " + \
        " --source_format=CSV  " + \
        " --allow_quoted_newlines=True " + \
        " --skip_leading_rows=1 " + \
        " --field_delimiter=\"{field_delimiter}\" " + \
        " --quote=\"{quote}\" " + \
        " --autodetect " + \
        " {table_name} " + \
        " \"{files_path}\" "

    table_path = bq_table.format(dataset=config['bq_target_dataset'], prefix="", table=table)
    
    if os.path.isfile(file_path):
        bqc = bq_load_command.format( \
            table_name = table_path, \
            files_path = file_path, \
            field_delimiter=config['mapping_csv_delimiter'], \
            quote=config['mapping_csv_quote']
        )
        print('To BQ: ' + bqc)

        try:
            os.system(bqc)
        except Exception as e:
            return_code = 2 # error during execution of the command
            raise e
    else:
        return_code = 1 # file not found
        print ('Source file {f} is not found.'.format(f=file_path))

    return return_code

'''
----------------------------------------------------
    main()
    return code:
        0 = success
        lower byte = number of Athena tables failed to load
        upper byte = if Custom mapping table failed to load
----------------------------------------------------
'''

def main():
    params = read_params()

    config = read_config(params.get('config_file'))

    return_code = 0

    print('Loading tables...')


    for entry in os.scandir(config['local_mapping_csv_path']):
        if entry.name.endswith(config['files_extension']):
            table = entry.name.strip(config['files_extension'])
            rc = load_table(table, entry.path, config)
            if rc != 0:
                return_code += 1
                continue

    return return_code

# ----------------------------------------------------
# go
# ----------------------------------------------------
return_code = main()

exit(return_code)

