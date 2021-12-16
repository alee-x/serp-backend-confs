import os 
import subprocess
import logging
from datetime import datetime

def run_sql_db2(db2_login,db2_password,error_msg_dir,output_msg_dir,**context):
    ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']
    target_label = ledger["attributes"]["target_label_name"].format(label = ledger['label'], classification = ledger['classification'], version = ledger['version'])
    ts = datetime.today().strftime('%Y%m%dT%H%M%S')
    sql_file_name = os.path.join(os.sep, "tmp", "db2_{0}_{1}_{2}_{3}.sql".format(project_code,ledger['label'],ledger['version'],ts)) 
    print(ledger["attributes"]["sqlcodeblock"])
    schema_name = "base{0}t".format(project_code.lower())   
    # overwrite schema if ms_database_schema is provided in attributes
    if 'db2_database_schema' in ledger["attributes"]:
        schema_name = "base{0}t".format(ledger["attributes"]["db2_database_schema"].lower())
    sql_stm = """
connect to pr_sail user {0} using {1};
    """.format(db2_login,db2_password)

    #sql_stm = sql_stm + ledger["attributes"]["sqlcodeblock"].format(source_label = ledger['label'], target_label = target_label, source_table = ledger['location_details'], schema_name = schema_name)
    
    sql_cmd = """

INSERT INTO basejeff001t.file3temporaltest (id, alf, score) VALUES ('p1',1,11);
INSERT INTO basejeff001t.file3temporaltest (id, alf, score) VALUES ('p2',2,22);
INSERT INTO basejeff001t.file3temporaltest (id, alf, score) VALUES ('p3',3,33);
DELETE FROM basejeff001t.file3temporaltest WHERE id = 'p3';

/*
-- Insert
INSERT INTO {schema_name}.{target_label} (id, alf, score)
SELECT
    a.id, a.alf, a.score
FROM {source_table} a
    LEFT JOIN {schema_name}.{target_label} b ON a.id = b.id
WHERE b.id IS NULL;

-- Delete
DELETE FROM {schema_name}.{target_label} 
WHERE NOT EXISTS (
    SELECT 1
    FROM {source_table}
    WHERE {schema_name}.{target_label}.id = {source_table}.id
)
*/

    """.format(source_label = ledger['label'], target_label = target_label, source_table = ledger['location_details'], schema_name = schema_name)
    print(sql_cmd)
    sql_stm = sql_stm + sql_cmd

    sql_stm = sql_stm + """
connect reset;
    """

    with open(sql_file_name, 'w') as text_file:
         text_file.write(sql_stm)
    logging.info("run_sql_db2  sql file is generated at: {0}".format(sql_file_name))
    #os.system(". /usr/local/airflow/sqllib/db2profile & db2 -tvmf {0} | tee {1}/log_{2}_{3}.txt".format(sql_file_name,output_msg_dir,schema,table))
    subprocess.call(['bash', '-c', '. /usr/local/airflow/sqllib/db2profile && db2 -tvmf {0} -z {1}/log_{2}_{3}_{4}_{5}.txt'.format(sql_file_name,output_msg_dir,project_code,ledger['label'],ledger['version'],ts)])
    #house keeping
    os.remove(sql_file_name)