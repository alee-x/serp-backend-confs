import shutil
import os
import glob
from sqlalchemy import *
from sqlalchemy import MetaData
from airflow.hooks.base_hook import BaseHook

#######################################################################
# DEBUG = FALSE IF YOU DONT WANT THE LOCAL FILES TO BE DELETED        #
#######################################################################
DEBUG = True

def housekeeping(**context):    
    ledger_json_file_name = context['ti'].xcom_pull(key='ledger_json_file_name') 
    csv_toload_filename = context['ti'].xcom_pull(key='csv_toload_filename') 
    local_dir = context['ti'].xcom_pull(key='local_dir')
    ledger = context['dag_run'].conf['ledger']
    csv_toload_dir = "{0}/{1}.csv".format(local_dir,ledger["label"])
    sql_files = glob.glob(os.path.join(local_dir, "*.sql"))
    db_platform = ledger.get('attributes',{}).get('db_platform','attribute not set')
    print("db_platform: {0}".format(db_platform))

    os.remove(ledger_json_file_name)
    os.remove(csv_toload_filename)
    print("ledger json file {0} has been deleted".format(ledger_json_file_name))
    shutil.rmtree(csv_toload_dir)
    print("csv file to load dir '{0}' has been deleted".format(csv_toload_dir))

    for f in sql_files:
        os.remove(f)
        print("sql file '{0}' has been deleted".format(f))
    
    if DEBUG == False:        
        shutil.rmtree(local_dir)
        print("removed local dir '{0}'".format(local_dir))
    else:
        print("DEBUG MODE >>> LOCAL DIR NOT DELETED: {0}".format(local_dir))


    # Handling for "Layered Table Structure" flag on the DB loading task
    # If layeredtable is required, then LOAD and BASE will be used
    # If layeredtable not required, then LOAD will be deleted after loading and BASE will remain
    # Drop using sqlalchemy to avoid dialect issues
    layeredtable = ledger.get('attributes',{}).get('layeredtable','False')  # ledger["attributes"]["layeredtable"] returns from json as a string
    print("LayeredTableStructure flag = {0}".format(layeredtable))    
    if layeredtable.lower() == "false" and db_platform == "mssql":
        
        db_conn_id = ledger['attributes']['database_conn_id']
        db_name = ledger['attributes']['database_name']
        db_conn = BaseHook.get_connection(db_conn_id)

        # Alter connection string & driver for the relevant db engine        
        if db_platform == "mssql":
            driver = "?driver=ODBC+DRIVER+17+for+SQL+Server"  # TODO: Set this via an attribute?        
            connection_str = r'mssql+pyodbc://{0}:{1}@{2}/{3}{4}'.format(db_conn.login,db_conn.password,db_conn.host,db_name,driver)

        engine = create_engine(connection_str)
        
        project_code = context['dag_run'].conf['project_code']
        schema_name = "load{0}t".format(project_code.lower())
        table_name = ledger["attributes"]["targettablename"].format_map(Default(ledger)).format_map(Default(ledger["attributes"]))

        print("Delete the LOAD table for single layer load:  '{0}.{1}'".format(schema_name,table_name))

        try: 
            table_to_drop = Table(table_name, MetaData(), autoload=False, autoload_with=engine, schema=schema_name)
        except Exception as e:
            raise Exception("Error getting table_to_drop. Error: {0}".format(e))

        try:
            table_to_drop.drop(engine)
            print("Table [{0}].[{1}] dropped for single layer load".format(schema_name,table_name))
        except Exception as e:
            raise Exception("Error deleting LOAD table where layeredtable=FALSE. Error: {0}".format(e))   




class Default(dict):
    def __missing__(self, key):
        return key