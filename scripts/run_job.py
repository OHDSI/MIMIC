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
    {
        "workflow": "ddl, staging, etl etc",
        "comment": "",
        "type": "sql or py",
        "variables": 
        {
            "target_project": "target project name",
            "target_dataset": "target dataset name"
        },
        "scripts": 
        [
            {"script": "path relative to the project root",          "comment": ""}
        ]
    },
}

# ----------------------------------------------------
# read_params()
# ----------------------------------------------------

def read_params():

    print('Reading params...')
    params = {
        "config_file":   "mandatory: indicate '-c' for 'config', json file name"
    }
    
    # Parsing command line arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:],"c:",["config="])
        if len(args) == 0:
            print("Expected:\npython run_job.py ../conf/workflow_ddl.conf")
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
            params['script_files'].append(arg)

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
    return codes: 0, ...
----------------------------------------------------
'''

def main():

    # params = read_params()
    # config = read_config(params['config_file'])

    # run_command_bq_script = "python bq_run_script.py --config {config_file} {script_file}"
    # to update bq_run_script to replace more than on pair of project-dataset
    run_command_bq_script = "python scripts/bq_run_script.py {script_file}"

    s_done = []
    rc = 0

    for s in config_default['scripts']:

        s_filename = s['script']
        run_command = run_command_bq_script.format(script_file=s_filename)
        print(run_command)
        rc = os.system(run_command)
        print("return_code", rc)

        if rc != 0:
            s_done.append('{2} | {0} | Error in:\n{1}'.format(s_filename.ljust(30, ' '), run_command, datetime.datetime.now()))
            break
        else:    
            s_done.append('{1} | {0} | Done.'.format(s_filename.ljust(30, ' '), datetime.datetime.now()))


    print('\nScripts executed:')
    for a in s_done:
        print(a)

    return rc

# ----------------------------------------------------
# run
# ----------------------------------------------------
return_code = main()

print('run_job.exit()', return_code)
exit(return_code)

# last edit: 2020-10-21
