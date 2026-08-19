#
# Run full SQL script files in BigQuery without splitting on semicolons.
# Intended for BigQuery scripting features such as DECLARE, ASSERT,
# and CREATE TEMP TABLE reused later in the same script.
#

import os
import sys
import getopt
import json
import datetime
import subprocess

config_default = {

    "variables": {

        "@variable_1":      "No project replacement by default",
        "@variable_2":      "No dataset replacement by default"
    },
    
    "escaping_chars": {
        '"': '\\"'
    }
}


def read_params():

    print('Reading params...')
    params = {
        "etlconf_file":     "",
        "config_file":      "",
        "script_files":     [],
        "files_not_found":  []
    }
    
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

    for opt, arg in opts:
        if opt == '-e' or opt == '--etlconf':
            if os.path.isfile(arg):
                params['etlconf_file'] = arg
        if opt == '-c' or opt == '--config':
            if os.path.isfile(arg):
                params['config_file'] = arg

    for arg in args:
        if os.path.isfile(arg):
            params['script_files'].append(arg)
        else:
            params['files_not_found'].append(arg)

    print('scripts to run', params)
    return params


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

    for k in config_default:
        s = etlconf_read.get(k, config_default[k])
        config[k] = s
    
    for k in config_default:
        s = config_read.get(k, config[k])
        config[k] = s

    print(config)
    return config


def remove_comments(s_query):

    print('Remove_comments()...')

    s_lines_src = s_query.split('\n')
    s_result = ""

    for s in s_lines_src:
        comment_flag = s.replace(' ', '')[0:2]

        if comment_flag != '--' and len(s) > 0:
            s_result = s_result + s + '\n'

    return s_result


def format_query(s_query, config):

    print('Formatting query...')

    s_result = s_query

    for var, val in config['escaping_chars'].items():
        s_result = s_result.replace(var, val)

    for var, val in config['variables'].items():
        s_result = s_result.replace(var, val)

    print(s_result)
    return s_result


def troubleshooting_bqc_format(bqc):

    print('Troubleshooting_bqc_format()...')

    s_lines_src = bqc.split('\n')
    s_result = ""

    for s in s_lines_src:

        comment_pos = s.find('--')
        if comment_pos > -1:
            s = s[0:comment_pos].strip()

        if len(s) > 0:
            s_result = s_result + s + ' '

    return s_result


def nice_message(s_filename, status, msg):
    time =    datetime.datetime.now()
    file =    s_filename.ljust(35, ' ')
    result =  'Done.' if status==0 else 'Error'
    message = '' if len(msg)==0 else ': ' + msg if len(msg.split('\n')) == 1 else '\n' + '\n'.join(map(lambda x: ''.ljust(4) + x, msg.split('\n')))

    return '{0} | {1} | {2}{3}'.format(time, file, result, message)


def main():

    rc = 0
    duration = datetime.datetime.now()
    params = read_params()
    config = read_config(params['etlconf_file'], params['config_file'])

    if len(params['files_not_found']) > 0:
        rc = 2
        for s_filename in params['files_not_found']:
            print('No such file or directory: {file}\n'.format(file=s_filename))

    else:
        bq_base_cmd = ["bq", "query", "--use_legacy_sql=false"]
        s_done = []
        s_done.append(nice_message('start...', 0, ''))

        for s_filename in params['script_files']:

            print('Run script {file}\n'.format(file=s_filename))

            s_query = open(s_filename).read()
            formatted_query = troubleshooting_bqc_format(format_query(remove_comments(s_query), config))
            cmd = bq_base_cmd + [formatted_query]

            print('Starting query...')
            try:
                completed = subprocess.run(cmd, capture_output=True, text=True)
                rc = completed.returncode
                if rc != 0:
                    print('bq stdout:\n' + completed.stdout)
                    print('bq stderr:\n' + completed.stderr)
            except FileNotFoundError:
                rc = 127
                print('Error: bq CLI not found in PATH')

            s_done.append(
                nice_message(s_filename, rc, '' if rc==0 else 'See script output above'))

            if rc != 0:
                break

        print('\nScripts executed:')
        for a in s_done:
            print(a)
        duration = datetime.datetime.now() - duration
        print('Run time: {0}'.format(duration))
        
    return rc


return_code = main()

print('bq_run_waveform_script.exit()', return_code)
exit(return_code)
