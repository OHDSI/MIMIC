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

        "@variable_1":      "No project replacement by default",
        "@variable_2":      "No dataset replacement by default"
    },
    
    "escaping_chars": {
        '"': '\\"'
        # '`': '\\`' # don't replace in windows OS
    }
}

# ----------------------------------------------------
# read_params()
# ----------------------------------------------------

def read_params():

    print('Reading params...')
    params = {
        "etlconf_file":     "",
        "config_file":      "",
        "script_files":     [],
        "files_not_found":  []
    }
    
    # Parsing command line arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:],"e:c:",["etlconf=,config="])
        if len(args) == 0:
            raise getopt.GetoptError("read_params() error", "Mandatory argument is missing.")

    except getopt.GetoptError as err:
        print(err.args)
        print("Please indicate correct params:")
        print("etlconf_file:   optional: indicate '-e' for 'etlconf', global config json file")
        print("config_file:    optional: indicate '-c' for 'config', local config json file")
        print("script_files:   [mandatory: indicate at least one script name as unnamed argument]")
        sys.exit(2)

    # get config files namess
    for opt, arg in opts:
        if opt == '-e' or opt == '--etlconf':
            if os.path.isfile(arg):
                params['etlconf_file'] = arg
        if opt == '-c' or opt == '--config':
            if os.path.isfile(arg):
                params['config_file'] = arg

    # collect script names
    for arg in args:
        if os.path.isfile(arg):
            params['script_files'].append(arg)
        else:
            params['files_not_found'].append(arg)

    print('scripts to run', params)
    return params

# ----------------------------------------------------
# read_config()
# ----------------------------------------------------

def read_config(etlconf_file, config_file):
    
    print('Reading config...')
    config = {}
    config_read = {}
    etlconf_read = {}

    if os.path.isfile(etlconf_file):
        with open(etlconf_file) as f:
            etlconf_read = json.load(f)

    if os.path.isfile(config_file):
        with open(config_file) as f:
            config_read = json.load(f)

    # global config has lower priority
    for k in config_default:
        s = etlconf_read.get(k, config_default[k])
        config[k] = s
    
    # local config has higher priority
    for k in config_default:
        s = config_read.get(k, config[k])
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

    s_result = s_query

    for var, val in config['escaping_chars'].items():
        s_result = s_result.replace(var, val)

    for var, val in config['variables'].items():
        s_result = s_result.replace(var, val)

    print(s_result)
    return s_result


'''
----------------------------------------------------
    1) make the command a single line
    windows or maybe 2022 style
    (TODO sometimes: investigate what happened with multiline text for bq query)
    2) remove comments at the end of the lines

    bqc: ready to run bq command 
----------------------------------------------------
'''
def troubleshooting_bqc_format(bqc):

    print('Troubleshooting_bqc_format()...')

    s_lines_src = bqc.split('\n')
    s_result = ""

    for s in s_lines_src:

        # remove trailing comment
        comment_pos = s.find('--')
        if comment_pos > -1:
            s = s[0:comment_pos].strip()

        # remove new line
        if len(s) > 0:
            s_result = s_result + s + ' ' # add space instead of new line

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

    rc = 0
    duration = datetime.datetime.now()
    params = read_params()
    config = read_config(params['etlconf_file'], params['config_file'])

    # stop if any files are not found

    if len(params['files_not_found']) > 0:
        rc = 2 # No such file or directory # Linux OS error code
        for s_filename in params['files_not_found']:
            print('No such file or directory: {file}\n'.format(file=s_filename))

    else:
        bq_command = "bq query --use_legacy_sql=false \"{query}\""
        s_done = []
        s_done.append(nice_message('start...', 0, ''))

        for s_filename in params['script_files']:

            print('Run script {file}\n'.format(file=s_filename))

            s_queries = open(s_filename).read().split(';')
            s_queries = trim_queries(s_queries)
            
            query_no = 0
            for s_query in s_queries:

                bqc = bq_command.format(
                    query=troubleshooting_bqc_format(format_query(s_query, config))
                )
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
        duration = datetime.datetime.now() - duration
        print('Run time: {0}'.format(duration)) # timedelta HH:MM:SS.f
        
    return rc


# ----------------------------------------------------
# run
# ----------------------------------------------------
return_code = main()

print('bq_run_script.exit()', return_code)
exit(return_code)

# last edit: 2021-02-07
