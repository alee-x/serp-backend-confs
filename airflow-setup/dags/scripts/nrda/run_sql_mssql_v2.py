import os 
import subprocess
import logging
from datetime import datetime

def run_sql_mssql(mssql_server, mssql_database, mssql_login,mssql_password, error_msg_dir,output_msg_dir,**context):

    ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']
    target_label = ledger["attributes"]["target_label_name"].format(label = ledger['label'], version = ledger['version'], classification = ledger['classification'])
    ts = datetime.today().strftime('%Y%m%dT%H%M%S')
    sql_file_name = os.path.join(os.sep, "tmp", "mssql_{0}_{1}_{2}_{3}.sql".format(project_code,ledger['label'],ledger['version'],ts)) 
    print(ledger)
    #print(ledger["attributes"]["sqlcodeblock"])


    code_block = ledger["attributes"]["sqlcodeblock"].format(source_label = ledger['label'], target_label = target_label, source_table = ledger['location_details'])
    print(code_block)


    success_msg = "<<RUN_SQL_SUCCESS>>"

    sql_stm = """{0}
    PRINT('{1}')
    """.format(code_block,success_msg)



    with open(sql_file_name, 'w') as text_file:
        text_file.write(sql_stm)

    sql_output = os.path.join(output_msg_dir,"sql_output.txt")

    sql_stm = '/opt/mssql-tools/bin/sqlcmd -S {0} -U {1} -P {2} -d {3} -i {4} -o {5}  -t 5 -m-1 -p 1 -X 1'.format(mssql_server, mssql_login,mssql_password, mssql_database, sql_file_name,sql_output)

    retval = subprocess.Popen(['bash', '-c', sql_stm], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    retval.wait()
    (stdout, stderr) = retval.communicate()

    print("stdout:")
    print(stdout)

    with open(sql_output, 'r') as output:
        output_str = output.read()
        print(output_str)
        if success_msg not in output_str:
            print("fail")              
            raise ValueError("SQL failed")

    #house keeping
    os.remove(sql_file_name)

    if retval.returncode != 0:
        print(stderr)
    else:
        print("success")
