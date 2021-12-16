import os 
import subprocess
import logging

from airflow.hooks.base_hook import BaseHook

def create_views_mssql(error_msg_dir,output_msg_dir,**context):
    ledgers = context['dag_run'].conf['ledgers']
    project_code = context['dag_run'].conf['project_code']
    db_conn_id = "serpsql"
    db_name = "NRDAv2_TestLoading"
    view_schema_prefix = "nrda"   

    
    if("database_conn_id" in ledgers[0]["attributes"] and ledgers[0]["attributes"]["ms_database_conn_id"].strip()):
        db_conn_id = ledgers[0]['attributes']['ms_database_conn_id']

    if("database_name" in ledgers[0]["attributes"] and ledgers[0]["attributes"]["ms_database_name"].strip()):
        db_name  = ledgers[0]['attributes']['ms_database_name']

    if("viewschemaprefix" in ledgers[0]["attributes"] and ledgers[0]["attributes"]["viewschemaprefix"].strip()):
        view_schema_prefix = ledgers[0]["attributes"]["viewschemaprefix"]

    view_schema = "{0}{1}v".format(view_schema_prefix.lower(),project_code.lower())
    # overwrite schema if ms_database_schema is provided in attributes
    if 'ms_database_schema' in ledgers[0]['attributes']:
        view_schema = "{0}{1}v".format(view_schema_prefix.lower(),ledgers[0]["attributes"]["ms_database_schema"].lower())

    sql_stm = """ """
    sql_file_name = os.path.join(os.sep, "tmp", "create_views_{0}_{1}.sql".format(view_schema,context['ds_nodash'])) 

    for ledger in ledgers:
        label = ledger["label"]
        classification = ledger["classification"]
        version = ledger["version"]
        base_table = ledger["location_details"]
        view_name = ledger["attributes"]["targettablename"].format(label = label, classification = classification, version = version)
        sql_stm += """
        IF NOT EXISTS (SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{0}')
        BEGIN 
            DECLARE @statement nvarchar(100) 
            SET @statement =  N'CREATE SCHEMA [{0}]'
            EXEC sp_executesql @statement
        END
        
        IF EXISTS (SELECT * FROM sys.views WHERE Object_id = OBJECT_ID(N'{0}.{1}') and type = 'V')
        DROP VIEW [{0}].[{1}]
        GO

        CREATE VIEW [{0}].[{1}] AS 
        SELECT * 
        FROM {2}
        GO
        """.format(view_schema.lower(),view_name.lower(), base_table.lower())

    success_msg = "<<RUN_SQL_SUCCESS>>"

    sql_stm = """{0}
    PRINT('{1}')
    """.format(sql_stm,success_msg)

    # run create view
    print("create view statement: {0}".format(sql_stm))


    with open(sql_file_name, 'w') as text_file:
         text_file.write(sql_stm)
       
    mssql_conn = BaseHook.get_connection(db_conn_id)

    sql_output = os.path.join(output_msg_dir,"log_create_views_mssql_output_{0}.txt".format(view_schema.lower()))

    sql_stm = '/opt/mssql-tools/bin/sqlcmd -S {0} -U {1} -P {2} -d {3} -i {4} -o {5} -t 5 -m-1 -p 1 -X 1'.format(mssql_conn.host,mssql_conn.login,mssql_conn.password,db_name,sql_file_name,sql_output)
    #print(sql_stm)
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