#
# Run workflows according to main etlconf
# 
# 
# to update bq_run_script to replace more than on pair of project-dataset
# 
#

import os
import sys
import getopt
import json
import datetime

# ----------------------------------------------------
# default config values
# To override default config values, copy the keys to be overriden to a json file,
# and indicate this file as --config parameter
# ----------------------------------------------------

config_default = {

    "workflow": "ddl, staging, etl etc",
    "comment": "",
    "type": "sql or py",
    "variables": 
    {
        "@target_project": "target project name",
        "@target_dataset": "target dataset name"
    },
    "scripts": 
    [
        {"script": "path_relative_to_the_project_root",          "comment": ""}
    ]

}

# ----------------------------------------------------
# read_params()
# ----------------------------------------------------

def read_params():

    print('Reading params...')
    params = {
        "config_file":   "mandatory: indicate '-c' for 'config', json file name",
        "script_files": []
    }
    
    # Parsing command line arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:],"c:",["config="])
        if len(opts) == 0:
            print("Expected:\npython run_workflow.py -c ../conf/workflow_ddl.conf")
            raise getopt.GetoptError("read_params() error", "Mandatory argument is missing.")

    except getopt.GetoptError as err:
        print(err.args)
        print("Please indicate correct params:")
        print(params)
        sys.exit(2)

    params['config_file'] = ''
    for opt, arg in opts:
        if opt == '-c' or opt == '--config':
            if os.path.isfile(arg):
                params['config_file'] = arg

    params['script_files'] = []
    for arg in args:
        if os.path.isfile(arg):
            params['script_files'].append({"script": arg})

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
    
    for k in config_default:
        s = config_read.get(k, config_default[k])
        config[k] = s

    print(config)
    return config


'''
----------------------------------------------------
    main()
    return codes: 0 = ok, !0 = error
----------------------------------------------------
'''

def main():

    params = read_params()
    config = read_config(params['config_file'])

    run_command_bq_script = "python scripts/bq_run_script.py -c {config_file} {script_file}"

    to_run = \
        config['scripts'] \
            if len(params['script_files']) == 0 \
        else params['script_files']

    # run all given scripts at a time
    run_command = run_command_bq_script.format(
        script_file= ' '.join(map( lambda s : s['script'], to_run)),
        config_file= params['config_file']
    )
    print(run_command)
    rc = os.system(run_command)

    return rc

# ----------------------------------------------------
# run
# ----------------------------------------------------
return_code = main()

print('run_workflow.exit()', return_code)
exit(return_code)

# last edit: 2020-11-12
