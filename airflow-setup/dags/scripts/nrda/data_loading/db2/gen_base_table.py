import json
import os 
from subprocess import PIPE, Popen
import pyspark.sql
from os.path import expanduser, join, abspath
import sqlalchemy
from sqlalchemy import *
from sqlalchemy.dialects import postgresql
from pyspark.sql.types import *
from sqlalchemy.schema import CreateTable
from airflow.hooks.base_hook import BaseHook
import re
import ibm_db_sa
import ibm_db_dbi as db

def gen_base_table(**context):
    ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']
    schema = json.loads(ledger["attributes"]["schema"])
    
    columns_names = []
    columns_types = []
    nullable_flags = []
    primary_key_flags = []
    enc_col_list = []
    enc_col_name_list = []

    if schema["return_info"]["schema_cols_to_enc"]:
        enc_col_list = schema["return_info"]["schema_cols_to_enc"]
        enc_col_name_list = [d['enced_col_name'].lower() for d in enc_col_list if d['enc_location'] == 'DB'] 
    schema_to_apply = schema["return_info"]["schema_to_apply"]
    schema_definition = schema["schema_definition"]
    schema_to_rename = schema["return_info"]["schema_cols_to_rename"]
    # rename for compare
    if schema_to_rename:
         for i in schema_definition["fieldDef"]:
                if i["column_Name"] in schema_to_rename:
                    i["column_Name"] = schema_to_rename[i["column_Name"]]
    
    schemaToApply = StructType.fromJson(schema_to_apply)
    for item in schemaToApply:        
        nullable_flags.append(item.nullable)
        primary_key_flags.append(False)
        to_enc_item = next((d for d in enc_col_list if d['enced_col_name'].lower() == item.name.lower() and d['enc_location'] == 'DB'), None)
        if to_enc_item:
            print('columns to enc: {0}'.format(to_enc_item))
            if to_enc_item['enc_type'] == 'HCP':
                columns_types.append(Integer)
            else:
                columns_types.append(BigInteger)
            columns_names.append(re.sub('\_e$', '', item.name.lower()) + "_e")
        else:
            columns_names.append(item.name.lower())
            # StringType or BooleanType --> VARCHAR(max_length)
            # BooleanType in case of 'False' or 0
            if isinstance(item.dataType, StringType):
                max_len = [i for i in schema_definition['fieldDef'] if i['column_Name'].lower() == item.name.lower()][0]['length']
                if max_len is not None and max_len > 32704:  # varchar limit on db2, need to use CLOB instead
                    columns_types.append(String(2200))
                else:
                    columns_types.append(String(max_len if max_len is not None and max_len > 0 else 10))
            elif isinstance(item.dataType, BooleanType):
                columns_types.append(String(6)) # typical boolean issue on db2 - 1,0,f,t,true,false? varchar(6) is a workaroud
            elif isinstance(item.dataType, ByteType) or isinstance(item.dataType, ShortType):
                columns_types.append(SmallInteger)
            elif isinstance(item.dataType, IntegerType):
                columns_types.append(Integer)    
            elif isinstance(item.dataType, LongType):
                columns_types.append(BigInteger)  
            elif isinstance(item.dataType, FloatType) or isinstance(item.dataType, DoubleType):
                columns_types.append(Float)
            elif isinstance(item.dataType, DecimalType):
                columns_types.append(DECIMAL(item.dataType.precision, item.dataType.scale))
            elif isinstance(item.dataType, BinaryType):
                columns_types.append(LargeBinary)
            elif isinstance(item.dataType, TimestampType):
                columns_types.append(DateTime)
            elif isinstance(item.dataType, DateType):
                columns_types.append(Date)
            else:
                columns_types.append(CLOB)

    columns_names.append("avail_from_dt")
    columns_types.append(Date)
    nullable_flags.append(item.nullable)
    primary_key_flags.append(False)
    
    base_schema_prefix = "base"
    # check if QACK is used instead    
    if("load_to_qack" in ledger["attributes"]):
        base_schema_prefix="qack"    
    schema_name = "{0}{1}t".format(base_schema_prefix,project_code.lower())
    # overwrite schema if ms_database_schema is provided in attributes
    if 'db2_database_schema' in ledger["attributes"]:
        schema_name = "{0}{1}t".format(base_schema_prefix,ledger["attributes"]["db2_database_schema"].lower())
    # check if schema is overwritten    
    if("schema_to_overwrite" in ledger["attributes"]):
        schema_name=ledger["attributes"]["schema_to_overwrite"]
    # check if tablescapce is overwritten    
    table_space = "BASEADHCT_PD_DATA"
    if("tablespace_to_overwrite" in ledger["attributes"]):
        table_space = ledger["attributes"]["tablespace_to_overwrite"]

    table_name = "nrdav2_{0}_{1}".format(ledger['label'],ledger['version'])
    if 'versionenabled' in ledger["attributes"]:
        if ledger["attributes"]["versionenabled"].lower() == 'false':
            table_name = "nrdav2_{0}".format(ledger['label'])
            
    meta = MetaData() 
    tab_to_load = Table(table_name, meta,schema=schema_name,
                *(Column(column_name, column_type,
                        primary_key=primary_key_flag,
                        nullable=nullable_flag)
                for column_name,
                    column_type,
                    primary_key_flag,
                    nullable_flag in zip(columns_names,
                                            columns_types,
                                            primary_key_flags,
                                            nullable_flags)))
    create_table_sql_str="{0}".format(CreateTable(tab_to_load).compile(dialect=ibm_db_sa.dialect()))
    create_table_sql_db2_str = "{0} IN {1};".format(create_table_sql_str.strip().replace('"', ''), table_space) 

    db2_conn = BaseHook.get_connection('db2_prsail_conn')    
    engine = create_engine("db2+ibm_db://{0}:{1}@db2.database.ukserp.ac.uk:60070/PR_SAIL;SECURITY=SSL;SSLCLIENTKEYSTOREDB=/prsail_keys/chi.kdb;SSLCLIENTKEYSTASH=/prsail_keys/chi.sth;".format(db2_conn.login,db2_conn.password))
    inspector = inspect(engine)
    
    #create schemas if needed
    if schema_name not in map(str.lower, inspector.get_schema_names()):
        engine.execute("create schema {0}".format(schema_name))
        print("Schema {0} has been created".format(schema_name))
    
    #drop table if it exists
    for tbl in inspector.get_table_names(schema=schema_name):
        if table_name.lower() in tbl.lower():
            engine.execute("drop table {0}.{1}".format(schema_name, table_name))
            print("Table {0}.{1} has been dropped".format(schema_name, table_name))
    
    #create table
    engine.execute(create_table_sql_db2_str)
    print(create_table_sql_db2_str)
    local_dir = context['ti'].xcom_pull(key='local_dir')
    base_col_file = os.path.join(local_dir, "base_col_{0}_{1}.txt".format(schema_name,table_name)) 
    print("Columns file:  {0}".format(base_col_file))
    strList = create_table_sql_db2_str.split('\n')
    with open(base_col_file, 'w') as text_file:        
        for item in strList[1:-2]:
            col_name = item.lower().strip().split()[0]
            if re.sub('\_e$', '', col_name) not in enc_col_name_list:
                #print("main."+col_name)
                text_file.write("main."+col_name+",\n")
            else:
                #print("temp_key_{0}.{0}".format(col_name))
                text_file.write("temp_key_{0}.{0},\n".format(col_name))