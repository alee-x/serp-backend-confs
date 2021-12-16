import os 
import subprocess
import logging
import time
import json

from airflow.hooks.base_hook import BaseHook
from airflow import AirflowException

def run_load_load(**context):
    ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']
    csv_file = context['ti'].xcom_pull(key='csv_toload_filename')
    print("csv file name: {0}".format(csv_file))

    local_dir = context['ti'].xcom_pull(key='local_dir')

    schema = "load{0}t".format(project_code.lower())
    # overwrite schema if ms_database_schema is provided in attributes
    if 'ms_database_schema' in ledger["attributes"]:
        schema = "load{0}t".format(ledger["attributes"]["ms_database_schema"].lower())

    #table = "nrdav2_{0}_{1}".format(ledger['label'],ledger['version'])
    table = ledger["attributes"]["targettablename"].format_map(Default(ledger)).format_map(Default(ledger["attributes"]))
    print("targettablename: ",table)
    # metadata = json.loads(ledger["attributes"]["schema"])
    # if metadata['schema_definition']['destinationTablename']:
    #     table = table.format_map(Default(destinationtablename=metadata['schema_definition']['destinationTablename'])).format_map(Default(ledger)).format_map(Default(ledger["attributes"]))        
    #     print("destinationtablename: ",table)


    sql_file_name = os.path.join(os.sep, local_dir, "{0}_{1}.sql".format(schema,table)) 

    db_conn_id = ledger['attributes']['ms_database_conn_id']
    db_name = ledger['attributes']['ms_database_name']    
    
    mssql_conn = BaseHook.get_connection(db_conn_id)

    # Old DB2 code copying from {nrda_schema}.{table} to {load_schema}.{table} using NRDA Cursor / LOAD / RunStats
    # TODO: How dow we want this to happen in SQLServer?

    # col_delimiter = "0xA7" # "§"
    # col_delimiter = "0x7C" # |
    col_delimiter = "0x01"
    row_terminator = "0x0a"

    bcp_output_file = "{0}/log_bcp_output_{1}_{2}.txt".format(local_dir,schema,table)
    print("bcp output file: ", bcp_output_file)

    bcp_stm = """ 
/opt/mssql-tools/bin/bcp [{4}].[{5}] IN {2} -S {6} -U {0} -P {1} -d {7} -t {8} -r {9} -c -h TABLOCK -b 50000 -e {3}/log_bcp_error_{4}_{5}.txt > {10}
    """.format(mssql_conn.login,mssql_conn.password,csv_file,local_dir,schema,table,mssql_conn.host,db_name,col_delimiter,row_terminator,bcp_output_file)
    # -r 0x0a - CRLF - row terminator 
    # -t 0xA7 - column delimiter §
    # -t 0x01 - current delimiter
    print(bcp_stm.replace("-P "+mssql_conn.password, "-P "+"xxxx"))

    logging.info("load_load sql file is generated at: {0}".format(sql_file_name))

    ## How to call BCP? Do we need a .sql file first? Is that for the multiple commands (load & runstats)?  Create .bat file instead - "runquery.bat commands.sql"?  Call BCP then SQLCMD
    # Install sqlcmd and bcp the SQL Server command-line tools on Linux  https://docs.microsoft.com/en-us/sql/linux/sql-server-linux-setup-tools?view=sql-server-ver15
    
    print("Starting bcp load...")
    start = time.time()
    
    retval = subprocess.Popen(['bash', '-c', bcp_stm]) #, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    retval.wait()
    # (stdout, stderr) = retval.communicate()

    print("TimeTaken: ", time.time() - start)

    error_list = ['0\nError', '[Microsoft][ODBC Driver 17 for SQL Server]', 'NativeError', '\n\n0 rows copied']

    # if any(e in stdout for e in error_list):
    #     raise ValueError("BCP failed")

    # Check if there is any useful info in the bcp error file.  
    # To avoid PII, only include rows to identify the columns and reasons: #@ Row 341002, Column 8: String data, right truncation @#
    bcp_err_file = "{0}/log_bcp_error_{1}_{2}.txt".format(local_dir,schema,table)
    bcp_err_detail = ""
    print("bcp_err_file: ", bcp_err_file)
    if os.path.exists(bcp_err_file): 
        with open(bcp_err_file, 'r', encoding="iso-8859-1") as output_err:
            head = [next(output_err,None) for x in range(40)]           # Get the first x rows of the error file (e.g. 40 rows will detail 20 errors)
            for line in head:
                if ( line is not None and line.startswith('#@ Row') ):  # Get rows prefixed "#@"
                    bcp_err_detail = bcp_err_detail + line
            # Print bcp_err_detail below - after print(*output_tail,sep='\n')

    print("bcp_output_file: ", bcp_output_file)
    with open(bcp_output_file, 'r') as output:
        output_str = output.read()
        output_tail = output_str.split('\n')[-10:]
        print(*output_tail,sep='\n')
        if bcp_err_detail != "":
            print(bcp_err_detail)       # Print column error information from the BCP error file
        if any(e in output_str for e in error_list):
            print("fail")
            raise ValueError("BCP failed")

    if retval.returncode != 0:
        print("return code not 0")
    #     print(stderr)
        raise ValueError("Failed")
    else:
        print("success")



class Default(dict):
    def __missing__(self, key):
        return key
