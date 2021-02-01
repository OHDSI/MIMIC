'''
-------------------------------------------------------------------
@2020, Odysseus Data Services, Inc. All rights reserved
MIMIC IV CDM Conversion
-------------------------------------------------------------------

Iterate trhough waveform source csv files, organized in folders 
Folders hierarcy:
    root_source_files/case_id/subject_id/wfdb_reference_id.csv

'''

import subprocess
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

        "@waveforms_csv_path":    "gs://...",

        "@wf_project": "...",
        "@wf_dataset": "..."
    }
    
}


##################################################################################

# gsutil ls gs://mimic_iv_to_omop/waveforms/source_data/csv/

# subprocess.check_output('gsutil ls gs://mimic_iv_to_omop/waveforms/source_data/csv/', shell=True)

##################################################################################

# ----------------------------------------------------
# read_params()
# ----------------------------------------------------

def read_params():

    print('Reading params...')
    params = {
        "etlconf_file":     "",
        "config_file":      ""
    }
    
    # Parsing command line arguments
    try:
        opts, args = getopt.getopt(sys.argv[1:],"e:c:",["etlconf=,config="])
        if len(opts) == 0:
            raise getopt.GetoptError("read_params() error", "Mandatory argument is missing.")

    except getopt.GetoptError as err:
        print(err.args)
        print("Please indicate correct params:")
        print("etlconf_file:   optional: indicate '-e' for 'etlconf', global config json file")
        print("config_file:    optional: indicate '-c' for 'config', local config json file")
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


'''
    get_files_list()
    it is CSVs we are going to load to a details table
'''
def get_files_list(path):

    s = subprocess.check_output('gsutil ls -r {0}'.format(path), shell=True)
    files_list = s.decode('utf-8').split()
    files_list = filter(lambda x: '.csv' in x, files_list)

    return files_list


'''
    get_header_csv()
    create a "header table" from the files list
'''
def get_header_csv(files_list, root_path):

    result = []
    s_template = "{case_id},{subject_id},{short_reference_id},{long_reference_id}\n"
    result.append(s_template.format(
        case_id            = "case_id",
        subject_id         = "subject_id",
        short_reference_id = "short_reference_id",
        long_reference_id  = "long_reference_id"
    ))

    for s in files_list:
        s_parts = s.replace(root_path, '')
        parts = s_parts.split('/')
        result.append(s_template.format(
            case_id            = parts[1],
            subject_id         = parts[2],
            short_reference_id = parts[3].replace('.csv', ''),
            long_reference_id  = s
        ))
    print(result)
    return result

''''''
def create_tmp_csv(header_csv, table_name):

    csv_temp_name = "tmp_{0}.csv".format(table_name)

    with open(csv_temp_name, 'w') as f:
        for s in header_csv:
            f.write(s)
        f.close()
        print('Generated', f.name)

    return csv_temp_name


table_wf_header = 'wf_header'
table_wf_details = 'wf_details'


''' 
----------------------------------------------------
    load_table()
    return codes: 0, 1, 2
----------------------------------------------------
'''

def load_table(table, file_path, config, replace_flag):

    return_code = 0

    bq_table = '{dataset}.{prefix}{table}'

    bq_load_command = \
        "bq --location=US load --replace={replace_flag} " + \
        " --source_format=CSV  " + \
        " --allow_quoted_newlines=True " + \
        " --skip_leading_rows=1 " + \
        " --autodetect " + \
        " {table_name} " + \
        " \"{files_path}\" "

    table_path = bq_table.format(dataset=config['variables']['@wf_dataset'], prefix="", table=table)
    
    if os.path.isfile(file_path) or 'gs:/' in file_path:
        bqc = bq_load_command.format( \
            table_name = table_path, \
            files_path = file_path, \
            replace_flag = replace_flag
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
    main function
'''
def main():
    rc = 0
    duration = datetime.datetime.now()
    params = read_params()
    config = read_config(params['etlconf_file'], params['config_file'])
    
    # read folders structure
    fl = get_files_list(config['variables']['@waveforms_csv_path'])
    header_csv = get_header_csv(fl, config['variables']['@waveforms_csv_path'])
    tmp_csv = create_tmp_csv(header_csv, table_wf_header)

    # load a header table
    load_table(table_wf_header, tmp_csv, config, True)
    if os.path.exists(tmp_csv):
        os.remove(tmp_csv)

    # load a details table
    replace_flag = True
    print(fl)
    for f in fl:
        print(f)
        load_table(table_wf_details, f, config, replace_flag)
        replace_flag = False

    duration = datetime.datetime.now() - duration
    print('Run time: {0}'.format(duration)) # timedelta HH:MM:SS.f

    return rc

# -------------

if __name__ == '__main__':
    # test1.py executed as script
    # do something
    main()