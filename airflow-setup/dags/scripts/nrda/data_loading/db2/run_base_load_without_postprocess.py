import os 
import subprocess
import logging
from airflow.hooks.base_hook import BaseHook

def run_base_load_without_postprocess(**context):
    db2_conn = BaseHook.get_connection('db2_prsail_conn')
    local_dir = context['ti'].xcom_pull(key='local_dir')

    ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']
    
    load_schema = "load{0}t".format(project_code.lower())
    base_schema = "base{0}t".format(project_code.lower())
    # overwrite schema if ms_database_schema is provided in attributes
    if 'db2_database_schema' in ledger["attributes"]:
        load_schema = "load{0}t".format(ledger["attributes"]["db2_database_schema"].lower())
        base_schema = "base{0}t".format(ledger["attributes"]["db2_database_schema"].lower())
    table = "nrdav2_{0}_{1}".format(ledger['label'],ledger['version'])
    if 'versionenabled' in ledger["attributes"]:
        if ledger["attributes"]["versionenabled"].lower() == 'false':
            table = "nrdav2_{0}".format(ledger['label'])

    sql_file_name = os.path.join(local_dir, "{0}_load_without_postprocess_{1}.sql".format(base_schema,table)) 
           
    sql_stm = """
connect to pr_sail user {0} using {1};

declare NRDACursor cursor for select *,current_date from {2}.{4};
load from NRDACursor of cursor messages {5}/error_load_without_postprocess_{3}_{4}.txt replace into {3}.{4} nonrecoverable;
runstats on table {3}.{4} with distribution and detailed indexes all;

connect reset;
    """.format(db2_conn.login,db2_conn.password,load_schema,base_schema,table,local_dir)

    with open(sql_file_name, 'w') as text_file:
         text_file.write(sql_stm)
    #logging.info(sql_stm)
    logging.info("run_base_load_without_postprocess sql file is generated at: {0}".format(sql_file_name))
    #os.system(". /usr/local/airflow/sqllib/db2profile & db2 -tvmf {0} | tee {1}/log_{2}_{3}.txt".format(sql_file_name,output_msg_dir,load_schema,table))
    subprocess.call(['bash', '-c', '. /usr/local/airflow/sqllib/db2profile && db2 -tvmf {0} -z {1}/log_load_without_postprocess_{2}_{3}.txt'.format(sql_file_name,local_dir,base_schema,table)])
 