import os 
import subprocess
import logging
from airflow.hooks.base_hook import BaseHook
import psycopg2

def run_load_load(**context):

    ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']
    csv_file = context['ti'].xcom_pull(key='csv_toload_filename')
    print("csv file name: {0}".format(csv_file))
    
    local_dir = context['ti'].xcom_pull(key='local_dir')

    schema = "load{0}t".format(project_code.lower())
    # overwrite schema if ms_database_schema is provided in attributes
    if 'pg_database_schema' in ledger["attributes"]:
        schema = "load{0}t".format(ledger["attributes"]["pg_database_schema"].lower())
    table = "nrdav2_{0}_{1}".format(ledger['label'],ledger['version'].replace('-',''))
    if 'versionenabled' in ledger["attributes"]:
        if ledger["attributes"]["versionenabled"].lower() == 'false':
            table = "nrdav2_{0}".format(ledger['label'])
    
    full_table_name = "{0}.{1}".format(schema, table)
    print(full_table_name)


    db_conn_id = ledger['attributes']['pg_database_conn_id']
    db_name = ledger['attributes']['pg_database_name']
    pg_conn = BaseHook.get_connection(db_conn_id)

    conn = psycopg2.connect("host={0} dbname={1} user={2} password={3} port={4}".format(pg_conn.host, db_name, pg_conn.login, pg_conn.password, pg_conn.port))
    cur = conn.cursor()
    with open(csv_file, 'r') as f:
        cur.copy_from(f, full_table_name, sep=',',null='') #https://www.psycopg.org/docs/cursor.html#cursor.copy_from

    conn.commit()
#     sql_file_name = os.path.join(local_dir, "{0}_load_{1}.sql".format(schema,table)) 
           
#     sql_stm = """
# connect to pr_sail user {0} using {1};

# load client from {2} of del 
# modified by
# delprioritychar
# noheader
# messages {3}/error_load_{4}_{5}.txt
# replace into {4}.{5} 
# nonrecoverable;

# runstats on table {4}.{5} with distribution and detailed indexes all;
# connect reset;
#     """.format(db2_conn.login,db2_conn.password,csv_file,local_dir,schema,table)

#     with open(sql_file_name, 'w') as text_file:
#          text_file.write(sql_stm)
#     logging.info("nrda_load sql file is generated at: {0}".format(sql_file_name))
#     #os.system(". /usr/local/airflow/sqllib/db2profile & db2 -tvmf {0} | tee {1}/log_{2}_{3}.txt".format(sql_file_name,output_msg_dir,schema,table))
#     subprocess.call(['bash', '-c', '. /usr/local/airflow/sqllib/db2profile && db2 -tvmf {0} -z {1}/log_load_{2}_{3}.txt'.format(sql_file_name,local_dir,schema,table)])
 