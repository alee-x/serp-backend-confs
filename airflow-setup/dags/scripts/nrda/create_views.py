import os 
import subprocess
import logging

def create_views(db2_login,db2_password,error_msg_dir,output_msg_dir,**context):
    ledgers = context['dag_run'].conf['ledgers']
    project_code = context['dag_run'].conf['project_code']    
    sail_schema = "sail{0}v".format(project_code.lower())
    # overwrite schema if ms_database_schema is provided in attributes
    if 'db2_database_schema' in ledgers[0]["attributes"]:
        sail_schema = "sail{0}v".format(ledgers[0]["attributes"]["db2_database_schema"].lower())
    # create_view_st = "connect to pr_sail user {0} using {1};\n".format(db2_login,db2_password)
    create_view_st = "connect to sail user {0} using {1};\n".format(db2_login,db2_password)
    sql_file_name = os.path.join(os.sep, "tmp", "create_views_{0}_{1}.sql".format(sail_schema,context['ds_nodash'])) 
    for ledger in ledgers:
        label = ledger["label"]
        classification = ledger["classification"]
        version = ledger["version"]
        base_table = ledger["location_details"]
        view_name = ledger["attributes"]["targettablename"].format(label = label, classification = classification, version = version)
        create_view_st += """
        DROP VIEW {0}.{1};
        CREATE VIEW {0}.{1} AS SELECT * FROM {2};
        """.format(sail_schema.upper(),view_name.upper(), base_table.upper())
    create_view_st += "connect reset;"
    # run create view
    #print("create view statement: {0}".format(create_view_st))


    with open(sql_file_name, 'w') as text_file:
         text_file.write(create_view_st)
    #logging.info(sql_stm)
    logging.info("load_load sql file is generated at: {0}".format(sql_file_name))

    # subprocess.call(['bash', '-c', '. /usr/local/airflow/sqllib/db2profile && db2 -tvmf {0} -z {1}/log_create_views_{2}_{3}.txt'.format(sql_file_name,output_msg_dir,sail_schema,context['ds_nodash'])])
    subprocess.call(['bash', '-c', '. /home/airflow/sqllib/db2profile && db2 -tvmf {0} -z {1}/log_create_views_{2}_{3}.txt'.format(sql_file_name,output_msg_dir,sail_schema,context['ds_nodash'])])
 