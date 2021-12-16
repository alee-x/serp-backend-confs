import os
import subprocess
import logging
import time

from airflow.hooks.base_hook import BaseHook

def run_indexing_mssql(**context):
    ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']
    index_col_list = context['ti'].xcom_pull(key='schema_cols_to_index')
    if index_col_list is None:
        index_col_list = []

    schema = "load{0}t".format(project_code.lower())
    # overwrite schema if ms_database_schema is provided in attributes
    if 'ms_database_schema' in ledger["attributes"]:
        schema = "load{0}t".format(ledger["attributes"]["ms_database_schema"].lower())
    table = "nrdav2_{0}_{1}".format(ledger['label'],ledger['version'])

    index_stm = ""

    for item in index_col_list:
        index_stm = index_stm + """
IF NOT EXISTS ( SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('{0}.{1}') AND name='{1}_{2}_I1' )
	BEGIN
        PRINT('Create indexes on load table for column: {2}');
        PRINT(CONVERT(varchar(24), GetDate(), 121));
        CREATE INDEX {1}_{2}_I1 ON {0}.{1} ({2});
        PRINT(CONVERT(varchar(24), GetDate(), 121));
    END
        """.format(schema,table,item['col_name'])

    index_stm += "<<INDEXING_SUCCESS>>"

    sql_file_name = os.path.join(os.sep, "tmp", "{0}_xref_{1}_.sql".format(schema,table)) 

    with open(sql_file_name, 'w') as text_file:
         text_file.write(index_stm)
    #logging.info(xref_stm)
    logging.info("run_indexing sql file is generated at: {0}".format(sql_file_name))

    sql_output = os.path.join("/tmp","log_indexing_output_{0}_{1}.txt".format(schema,table))

    print("sql output file: ", sql_output)

    db_conn_id = ledger['attributes']['ms_database_conn_id']
    db_name = ledger['attributes']['ms_database_name']

    mssql_conn = BaseHook.get_connection(db_conn_id)

    ## TODO ## Change the timeout value "-t" to something appropriate ## For testing, 1800 seconds = 30 mins, 10800 seconds = 3 hours, 43200 = 12h
    sql_stm = '/opt/mssql-tools/bin/sqlcmd -S {4} -U {0} -P {1} -d {5} -i {2} -o {3}  -t 43200 -m-1 -p 1 -X 1'.format(mssql_conn.login,mssql_conn.password,sql_file_name,sql_output,mssql_conn.host,db_name)
    
    print("Starting indexing...")
    start = time.time()

    retval = subprocess.Popen(['bash', '-c', sql_stm])

    retval.wait()
    # (stdout, stderr) = retval.communicate()

    print("TimeTaken: ", time.time() - start)

    success_msg = "<<INDEXING_SUCCESS>>"

    with open(sql_output, 'r') as output:
        output_str = output.read()
        print(output_str)
        if success_msg not in output_str:
            print("fail")
            raise ValueError("SQL failed")

    if retval.returncode != 0:
        print("error")
        # print(stderr)
    else:
        print("success")