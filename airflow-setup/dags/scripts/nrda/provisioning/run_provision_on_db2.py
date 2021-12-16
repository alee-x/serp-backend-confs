import os 
import subprocess
import logging

def run_provision_on_db2(db2_login,db2_password,error_msg_dir,output_msg_dir,**context):
    ledgers = context['dag_run'].conf['ledgers']
    project_code = context['dag_run'].conf['project_code']    
    filtered_template_list = context['ti'].xcom_pull(key='filtered_template_list')

    create_st = "connect to pr_sail user {0} using {1};\n".format(db2_login,db2_password)
    sql_file_name = os.path.join(os.sep, "tmp", "provision_on_db2_{0}_{1}.sql".format(project_code.lower(),context['ds_nodash'])) 

    # loop through each chosen template and apply the sql on staging to create view or table
    for template in filtered_template_list:
        print(template)
        source_ledger = next((l for l in ledgers if l['label'].lower() == template["source_label"].lower() and l['classification'].lower() == template["source_classification"].lower()), None)
        table_or_view_name = template["source_label"]
        if source_ledger:
           table_or_view_name = source_ledger["attributes"]["target_label_name"].format(label = source_ledger['label'], classification = source_ledger['classification'], version = source_ledger['version'])
        if template["is_view"] == "true":
            view_schema = "sail{0}v".format(project_code.lower())
            # overwrite schema if ms_database_schema is provided in attributes
            if 'db2_database_schema' in ledgers[0]["attributes"]:
                view_schema = "sail{0}v".format(ledgers[0]["attributes"]["db2_database_schema"].lower())
            full_view_name = "{0}.{1}".format(view_schema,table_or_view_name)
            query = template["provision_sql_query"].format(schema = view_schema, label = source_ledger['label'], classification = source_ledger['classification'], version = source_ledger['version'])
            create_st += """
            DROP VIEW {0};
            CREATE VIEW {0} AS {1};
            """.format(full_view_name, query)
        else:
            base_schema = "base{0}t".format(project_code.lower())
            # overwrite schema if ms_database_schema is provided in attributes
            if 'db2_database_schema' in ledgers[0]["attributes"]:
                base_schema = "base{0}t".format(ledgers[0]["attributes"]["db2_database_schema"].lower())
            full_table_name = "{0}.{1}".format(base_schema,table_or_view_name)
            query = template["provision_sql_query"].format(schema = base_schema, label = source_ledger['label'], classification = source_ledger['classification'], version = source_ledger['version'])
            create_st += """
            DROP TABLE {0};
            CREATE TABLE {0} AS ({1}) WITH DATA;
            """.format(full_table_name, query)
    create_st += "connect reset;"
    # run create view - dev
    print("run provisioning on db2 statement: {0}".format(create_st))


    with open(sql_file_name, 'w') as text_file:
         text_file.write(create_st)
    #logging.info(sql_stm)
    logging.info("load_load sql file is generated at: {0}".format(sql_file_name))
    #os.system(". /usr/local/airflow/sqllib/db2profile & db2 -tvmf {0} | tee {1}/log_{2}_{3}.txt".format(sql_file_name,output_msg_dir,load_schema,table))
    subprocess.call(['bash', '-c', '. /usr/local/airflow/sqllib/db2profile && db2 -tvmf {0} -z {1}/log_provision_on_db2_{2}_{3}.txt'.format(sql_file_name,output_msg_dir,project_code.lower(),context['ds_nodash'])])
 