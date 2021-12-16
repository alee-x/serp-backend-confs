import json
import os 
from subprocess import PIPE, Popen
import pyspark.sql
from os.path import expanduser, join, abspath
import sqlalchemy
from sqlalchemy import *
#from sqlalchemy.dialects import postgres
from pyspark.sql.types import *
from sqlalchemy.schema import CreateTable
from airflow.hooks.base_hook import BaseHook
import re
import ibm_db_sa
import ibm_db_dbi as db
from sqlalchemy.sql import text

def gen_base_temporal_f3(**context):
    
    ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']

    schema_name = "base{0}t".format(project_code.lower())
    # overwrite schema if ms_database_schema is provided in attributes
    if 'db2_database_schema' in ledger["attributes"]:
        schema_name = "base{0}t".format(ledger["attributes"]["db2_database_schema"].lower())

    target_label = ledger["attributes"]["target_label_name"].format(label = ledger['label'], classification = ledger['classification'], version = ledger['version'])    
    print(schema_name)
    print(target_label)
    # schema_name = "basejeff001t"
    # table_name = "file3temporaltest"
    # target_label = "basejeff001t.file3temporaltest"

        
    db2_conn = BaseHook.get_connection('db2_prsail_conn')    
    engine = create_engine("db2+ibm_db://{0}:{1}@db2.database.ukserp.ac.uk:60070/PR_SAIL;SECURITY=SSL;SSLCLIENTKEYSTOREDB=/prsail_keys/chi.kdb;SSLCLIENTKEYSTASH=/prsail_keys/chi.sth;".format(db2_conn.login,db2_conn.password))
    inspector = inspect(engine)
    
    #create schemas if needed
    if schema_name not in map(str.lower, inspector.get_schema_names()):
        engine.execute("create schema {0}".format(schema_name))
        print("Schema {0} has been created".format(schema_name))
    
    #check if table exists
    table_exists = False
    for tbl in inspector.get_table_names(schema=schema_name):
        if table_name.lower() in tbl.lower():            
            table_exists = True
            print("Table {0}.{1} already exists - Don't try to create".format(schema_name, table_name))


    #create table if not exists
    if table_exists == False:

        print('Creating temporal table with individual commands...')    

        create_table_sql_db2_str = """
CREATE TABLE {target_label} (
  id varchar(64) NOT NULL, 
  alf int NOT NULL, 
  score int NOT NULL, 
  sys_start    TIMESTAMP(12) NOT NULL GENERATED ALWAYS AS ROW BEGIN,
  sys_end      TIMESTAMP(12) NOT NULL GENERATED ALWAYS AS ROW END,
  ts_id        TIMESTAMP(12) NOT NULL GENERATED ALWAYS AS TRANSACTION START ID,
  PERIOD SYSTEM_TIME (sys_start, sys_end)
)  
ORGANIZE BY ROW
DATA CAPTURE NONE 
IN BASEADHC_PD_DATA
DISTRIBUTE BY HASH (id);
        """.format(target_label = target_label, schema_name = schema_name, table_name = table_name)
        print(create_table_sql_db2_str)
        engine.execute(text(create_table_sql_db2_str), multi=True) # To action multi-line statement


        create_table_sql_db2_str = """
ALTER TABLE {target_label} ADD CONSTRAINT pk_{schema_name}_{table_name}_id PRIMARY KEY (id);
        """.format(target_label = target_label, schema_name = schema_name, table_name = table_name)
        print(create_table_sql_db2_str)
        engine.execute(text(create_table_sql_db2_str))


        create_table_sql_db2_str = """
CREATE TABLE {target_label}_history 
LIKE {target_label} 
ORGANIZE BY ROW
IN BASEADHC_PD_DATA;
        """.format(target_label = target_label, schema_name = schema_name, table_name = table_name)
        print(create_table_sql_db2_str)
        engine.execute(text(create_table_sql_db2_str), multi=True)


        create_table_sql_db2_str = """
ALTER TABLE {target_label} ADD VERSIONING USE HISTORY TABLE {target_label}_history;
        """.format(target_label = target_label, schema_name = schema_name, table_name = table_name)
        print(create_table_sql_db2_str)
        engine.execute(text(create_table_sql_db2_str))


        temporal_table_insert_str = """
/*
-- Insert
INSERT INTO {schema_name}.{target_label} (id, alf, score)
SELECT
    a.id, a.alf, a.score
FROM {source_table} a
    LEFT JOIN {schema_name}.{target_label} b ON a.id = b.id
WHERE b.id IS NULL;
*/
    """.format(source_label = ledger['label'], target_label = target_label, source_table = ledger['location_details'], schema_name = schema_name, table_name = table_name)
        print(temporal_table_insert_str)
        #engine.execute(text(temporal_table_insert_str))


        temporal_table_delete_str = """
/*
-- Delete
DELETE FROM {schema_name}.{target_label} 
WHERE NOT EXISTS (
    SELECT 1
    FROM {source_table}
    WHERE {schema_name}.{target_label}.id = {source_table}.id
)
*/
    """.format(source_label = ledger['label'], target_label = target_label, source_table = ledger['location_details'], schema_name = schema_name, table_name = table_name)
        print(temporal_table_delete_str)
        #engine.execute(text(temporal_table_delete_str))
