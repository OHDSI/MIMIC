#
# Run multi-query script in BigQuery
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

    "variables": {
            
        "bq_target_project":      "No project replacement by default",
        "bq_target_dataset":      "No dataset replacement by default"
    }
    
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
    not_found = []
    for arg in args:
        if os.path.isfile(arg):
            params['script_files'].append(arg)
        else:
            not_found.append(arg)

    print('scripts to run', params)
    print('not found', not_found)
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
        .replace('"', '\\"') \
        .replace('`', '\\`')

    for var, val in config['variables'].items():
        s_result = s_result.replace(var, val)

    print(s_result)
    return s_result

'''
----------------------------------------------------
    nice output about execution result

    s_filename: script executed
    status:     0 = ok, !0 = error
    msg:        additional info if there is any
----------------------------------------------------
'''
def nice_message(s_filename, status, msg):
    time =    datetime.datetime.now()
    file =    s_filename.ljust(35, ' ')
    result =  'Done.' if status==0 else 'Error'
    message = '' if len(msg)==0 else ': ' + msg if len(msg.split('\n')) == 1 \
        else '\n' + '\n'.join(map(lambda x: ''.ljust(4) + x, msg.split('\n')))

    return '{0} | {1} | {2}{3}'.format(time, file, result, message)


'''
----------------------------------------------------
    main()
    return codes: 0 = ok, !0 = error
----------------------------------------------------
'''

def main():

    params = read_params()
    config = read_config(params['config_file'])

    bq_command = "bq query --use_legacy_sql=false \"{query}\""
    s_done = []
    rc = 0

    for s_filename in params['script_files']:

        print('Run script {file}\n'.format(file=s_filename))

        s_queries = open(s_filename).read().split(';')
        s_queries = trim_queries(s_queries)
        
        query_no = 0
        for s_query in s_queries:

            bqc = bq_command.format(query=format_query(s_query, config))
            query_no += 1
            print('Starting query...')
            rc = os.system(bqc)

            if rc != 0:
                break
        
        s_done.append(
            nice_message(s_filename, rc, '' if rc==0 else 'See query No {0}'.format(query_no)))

        if rc != 0:
            break


    print('\nScripts executed:')
    for a in s_done:
        print(a)

    return rc


# ----------------------------------------------------
# run
# ----------------------------------------------------
return_code = main()

print('bq_run_script.exit()', return_code)
exit(return_code)

# last edit: 2020-11-12
