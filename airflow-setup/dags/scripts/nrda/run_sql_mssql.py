import os 
import subprocess
import logging
from datetime import datetime
import pprint
import pathlib

from airflow.hooks.base_hook import BaseHook

def run_sql_mssql(mssql_login,mssql_password,error_msg_dir,output_msg_dir,**context):
    ledger = context['dag_run'].conf['ledger']
    
    project_code = context['dag_run'].conf['project_code']
    target_label = ledger["attributes"]["target_label_name"].format(label = ledger['label'], classification = ledger['classification'], version = ledger['version'])
    ts = datetime.today().strftime('%Y%m%dT%H%M%S')
    sql_file_name = os.path.join(os.sep, "tmp", "mssql_{0}_{1}_{2}_{3}.sql".format(project_code,ledger['label'],ledger['version'],ts)) 
    
    #print(ledger["attributes"]["sqlcodeblock"])     
    code_block = ledger["attributes"]["sqlcodeblock"].format(source_label = ledger['label'], target_label = target_label, source_table = ledger['location_details'])
    print(code_block)


    success_msg = "<<RUN_SQL_SUCCESS>>"

    sql_stm = """{0}
    PRINT('{1}')
    """.format(code_block,success_msg)

    sql_file_dir = os.path.dirname(sql_file_name)
    pathlib.Path(sql_file_dir).mkdir(parents=True, exist_ok=True)

    with open(sql_file_name, 'w') as text_file:
        text_file.write(sql_stm)

    print(sql_file_dir)
    print(output_msg_dir)

    sql_output = os.path.join(output_msg_dir,"sql_output.txt")

    db_conn_id = ledger['attributes']['ms_database_conn_id']
    db_name = ledger['attributes']['ms_database_name']
    mssql_conn = BaseHook.get_connection(db_conn_id)

    sql_stm = '/opt/mssql-tools/bin/sqlcmd -S {4} -U {0} -P {1} -d {5} -i {2} -o {3}  -t 5 -m-1 -p 1 -X 1'.format(mssql_login,mssql_password,sql_file_name,sql_output,mssql_conn.host,db_name)

    retval = subprocess.Popen(['bash', '-c', sql_stm], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    retval.wait()
    (stdout, stderr) = retval.communicate()

    print("stdout:")
    print(stdout)

    with open(sql_output, 'r') as output:
        output_str = output.read()
        #print(output_str)
        if success_msg not in output_str:
            print("fail")              
            raise ValueError("SQL failed")

    #house keeping
    os.remove(sql_file_name)

    if retval.returncode != 0:
        print(stderr)
    else:
        print("success")
