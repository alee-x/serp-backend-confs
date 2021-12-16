import os 
import subprocess
import logging

def run_provision_on_mssql(error_msg_dir,output_msg_dir,**context):
    ledgers = context['dag_run'].conf['ledgers']
    project_code = context['dag_run'].conf['project_code']    
    filtered_template_list = context['ti'].xcom_pull(key='filtered_template_list')
    db_conn_id = "serpsql"
    db_name = "NRDAv2_TestLoading"
    schema_prefix = "nrda"   

    if("ms_database_conn_id" in ledgers[0]["attributes"] and ledgers[0]["attributes"]["ms_database_conn_id"].strip()):
        db_conn_id = ledgers[0]['attributes']['ms_database_conn_id']

    if("ms_database_name" in ledgers[0]["attributes"] and ledgers[0]["attributes"]["ms_database_name"].strip()):
        db_name  = ledgers[0]['attributes']['ms_database_name']

    if("viewschemaprefix" in ledgers[0]["attributes"] and ledgers[0]["attributes"]["viewschemaprefix"].strip()):
        schema_prefix = ledgers[0]["attributes"]["viewschemaprefix"]

    sql_stm = """ """
    sql_file_name = os.path.join(os.sep, "tmp", "provision_on_mssql_{0}_{1}.sql".format(schema_prefix.lower(),context['ds_nodash'])) 





    # loop through each chosen template and apply the sql on staging to create view or table
    for template in filtered_template_list:
        print(template)
        source_ledger = next((l for l in ledgers if l['label'].lower() == template["source_label"].lower() and l['classification'].lower() == template["source_classification"].lower()), None)
        table_or_view_name = template["source_label"]
        if source_ledger:
           table_or_view_name = source_ledger["attributes"]["target_label_name"].format(label = source_ledger['label'], classification = source_ledger['classification'], version = source_ledger['version'])
        if template["is_view"] == "true":
            view_schema = "{0}{1}v".format(schema_prefix.lower(),project_code.lower())
            # overwrite schema if ms_database_schema is provided in attributes
            if 'ms_database_schema' in ledgers[0]["attributes"]:
               view_schema = "{0}{1}v".format(schema_prefix.lower(),ledgers[0]["attributes"]["ms_database_schema"].lower())

            query = template["provision_sql_query"].format(schema = view_schema, label = source_ledger['label'], classification = source_ledger['classification'], version = source_ledger['version'])
            sql_stm += """
            IF NOT EXISTS (SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{0}')
            BEGIN
                DECLARE @statement nvarchar(100) 
                SET @statement =  N'CREATE SCHEMA [{0}]'
                EXEC sp_executesql @statement
            END

            IF EXISTS (SELECT * FROM sys.views WHERE Object_id = OBJECT_ID(N'{0}.{1}') and type = 'V')
            DROP VIEW [{0}].[{1}]1
            GO

            CREATE VIEW [{0}].[{1}] AS 
            {2}
            GO
            """.format(view_schema.lower(),table_or_view_name.lower(), query)
        else:
            table_schema = "base{0}t".format(project_code.lower())
            # overwrite schema if ms_database_schema is provided in attributes
            if 'ms_database_schema' in ledgers[0]["attributes"]:
               table_schema = "base{0}t".format(ledgers[0]["attributes"]["ms_database_schema"].lower())

            query = template["provision_sql_query"].format(schema = table_schema, label = source_ledger['label'], classification = source_ledger['classification'], version = source_ledger['version'])
            sql_stm += """
            IF NOT EXISTS (SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{0}')
            BEGIN
                DECLARE @statement nvarchar(100) 
                SET @statement =  N'CREATE SCHEMA [{0}]'
                EXEC sp_executesql @statement
            END

            IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = {0} AND TABLE_NAME = {1})
	            DROP TABLE [{0}].[{1}]

            SELECT * INTO [{0}].[{1}]
            FROM ({2}) 
            AS original
            """.format(table_schema.lower(),table_or_view_name.lower(), query)
    
    success_msg = "<<RUN_SQL_SUCCESS>>"

    sql_stm = """{0}
    PRINT('{1}')
    """.format(sql_stm,success_msg)

    print("run provisioning on mssql statement: {0}".format(sql_stm))





    with open(sql_file_name, 'w') as text_file:
         text_file.write(sql_stm)
       
    mssql_conn = BaseHook.get_connection(db_conn_id)

    sql_output = os.path.join(output_msg_dir,"log_provisioning_on_mssql_output_{0}_{1}.txt".format(schema_prefix.lower(),context['ds_nodash']))

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

