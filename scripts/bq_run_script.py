#
# Run multi-query script in BigQuery
#

import os
import sys
import getopt
import json

# ----------------------------------------------------
# default config values
# To override default config values, copy the keys to be overriden to a json file,
# and indicate this file as --config parameter
# ----------------------------------------------------

config_default = {
 
    "bq_project_template":      "No project replacement by default",
    "bq_dataset_template":      "No dataset replacement by default",

    "bq_target_project":        "",
    "bq_target_dataset":        ""
}

# ----------------------------------------------------
# read_params()
# ----------------------------------------------------

def read_params():

    print('Reading params...')
    params = {
        "config_file":   "optional: indicate '-c' for 'config', json file name",
        "script_files":   ["mandatory: indicate at least one script name as unnamed arguments"]
    }
    
    # Parsing command line arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:],"c:",["config="])
        if len(args) == 0:
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

# ----------------------------------------------------
# trim_queries()
# ----------------------------------------------------

def trim_queries(s_queries):
    s_result = []
    for q in s_queries:
        q = remove_comments(q)
        if len(q) > 2:
            s_result.append(q)
    return s_result


# ----------------------------------------------------
# remove_comments()
# ----------------------------------------------------

def remove_comments(s_query):

    print('Remove_comments()...')
    # print(s_query)

    s_lines_src = s_query.split('\n')
    s_result = ""

    for s in s_lines_src:
        comment_flag = s.replace(' ', '')[0:2]

        if comment_flag != '--' and len(s) > 0:
            s_result = s_result + s + '\n'

    # print('Comments removed...')
    # print(s_result)

    return s_result

# ----------------------------------------------------
# format_query()
# ----------------------------------------------------

def format_query(s_query, config):

    print('Formatting query...')

    s_result = s_query \
        .replace(config['bq_project_template'], config['bq_target_project']) \
        .replace(config['bq_dataset_template'], config['bq_target_dataset']) \
        .replace('`', '\\`')

    print(s_result)
    return s_result

# ----------------------------------------------------
# main()
# ----------------------------------------------------

def main():

    params = read_params()
    config = read_config(params['config_file'])

    bq_command = "bq query --use_legacy_sql=false \"{query}\""
    s_done = []
    
    for s_filename in params['script_files']:

        print('Run script {file}\n'.format(file=s_filename))

        s_queries = open(s_filename).read().split(';')
        s_queries = trim_queries(s_queries)

        for s_query in s_queries:
            bqc = bq_command.format(query=format_query(s_query, config))
            print('Starting query...')
            os.system(bqc)
        
        s_done.append(s_filename)

    print('\nScripts executed:')
    for a in s_done:
        print(a)

# ----------------------------------------------------
# run
# ----------------------------------------------------
main()

exit(0)
