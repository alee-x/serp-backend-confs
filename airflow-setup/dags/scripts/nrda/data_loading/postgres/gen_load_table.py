import json
import os 
from subprocess import PIPE, Popen
import pyspark.sql
from os.path import expanduser, join, abspath
import sqlalchemy
from sqlalchemy import *
import airflow
from sqlalchemy.dialects import postgresql
# from sqlalchemy.dialects import mssql
from pyspark.sql.types import *
from sqlalchemy.schema import CreateTable
from airflow.hooks.base_hook import BaseHook

#import ibm_db_sa
#import ibm_db_dbi as db
import psycopg2


def gen_load_table(**context):
    ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']
    schema = json.loads(ledger["attributes"]["schema"])
   
    columns_names = []
    columns_types = []
    nullable_flags = []
    primary_key_flags = []
    enc_col_list = []
    # if context['ti'].xcom_pull(key='schema_cols_to_enc'):
    #     enc_col_list = context['ti'].xcom_pull(key='schema_cols_to_enc')
    # schema_to_apply = context['ti'].xcom_pull(key='schema_to_apply')
    # schema_definition = context['ti'].xcom_pull(key='schema_definition')

    # schema = StructType.fromJson(schema_to_apply)
    # schema_definition = StructType.fromJson(schema_definition)

    if schema['return_info']['schema_cols_to_enc']:
        enc_col_list = schema['return_info']['schema_cols_to_enc']
    schema_to_apply = schema["return_info"]["schema_to_apply"]
    schema_definition = schema["schema_definition"]
    schema_to_rename = schema["return_info"]["schema_cols_to_rename"]
    
    #print(schema_definition)

    # rename for compare
    if schema_to_rename:
         for i in schema_definition["fieldDef"]:
                if i["column_Name"] in schema_to_rename:
                    i["column_Name"] = schema_to_rename[i["column_Name"]]
    
    schemaToApply = StructType.fromJson(schema_to_apply)

    # add col def for nrdav2_loading_rowid
    columns_names.append("nrdav2_loading_rowid")
    columns_types.append(BigInteger)
    nullable_flags.append(False)
    primary_key_flags.append(False)    # not sure if nrdav2_loading_rowid should be treated as primary key in load? really for Rob to answer

    for item in schemaToApply:
        print(item)      
        columns_names.append(item.name.lower())
        nullable_flags.append(item.nullable)
        primary_key_flags.append(False)

        to_enc_item = next((d for d in enc_col_list if d['enced_col_name'].lower() == item.name.lower() and d['enc_location'] == 'DB'), None)
        if to_enc_item:
            print('columns to enc: {0}'.format(to_enc_item))
            if to_enc_item['enc_type'].upper() == 'KEY50':
                columns_types.append(CHAR(50))
            elif to_enc_item['enc_type'] == 'HCP':
                columns_types.append(CHAR(12))
            else:
                columns_types.append(CHAR(30))  #todo: need to refine
        
        else:
            # StringType or BooleanType --> VARCHAR(max_length)
            # BooleanType in case of 'False' or 0
            if isinstance(item.dataType, StringType) or isinstance(item.dataType, BooleanType):
                max_len = 30
                for i in schema_definition['fieldDef']:
                    col_name = i['column_Name']

                    if i['dest_Col_Name'] is not '' and i['dest_Col_Name'] is not None:
                        col_name = i['dest_Col_Name']

                    #print(col_name)
                    #print(item.name)

                    # col_name = i['dest_Col_Name'] if i['dest_Col_Name'] is not None or i['dest_Col_Name'] is not '' else i['col_Name']
                    if col_name.lower() == item.name.lower():
                        max_len = i['length']
                        print("max len: ", max_len)
                        break
                
                if max_len <= 8000:
                    columns_types.append(String(max_len))
                else:
                    columns_types.append(String(None))  # Create as varchar(max)

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

    schema_name = "load{0}t".format(project_code.lower())
    # overwrite schema if ms_database_schema is provided in attributes
    if 'pg_database_schema' in ledger["attributes"]:
        schema_name = "load{0}t".format(ledger["attributes"]["pg_database_schema"].lower())
    table_name = "nrdav2_{0}_{1}".format(ledger['label'],ledger['version'].replace('-',''))
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
    #create_table_sql_str="{0}".format(CreateTable(tab_to_load).compile(dialect=ibm_db_sa.dialect()))
    create_table_sql_str="{0}".format(CreateTable(tab_to_load).compile(dialect=postgresql.dialect()))
    #create_table_sql_mssql_str = "{0} IN LOADADHCT_PD_DATA;".format(create_table_sql_str.strip().replace('"', '')) 
    create_table_sql_pg_str = create_table_sql_str.strip().replace('"', '')


    ###########################################################################
    # Moved index creation to run xref so they are created after data loaded  #
    ###########################################################################

    db_conn_id = ledger['attributes']['pg_database_conn_id']
    db_name = ledger['attributes']['pg_database_name']
    #db_conn_id = 'postgres_test_69.23'
    #db_name = 'pg_load_test'

    pg_conn = BaseHook.get_connection(db_conn_id)
    
    connection_str = r'postgresql://{0}:{1}@{2}:{4}/{3}'.format(pg_conn.login,pg_conn.password,pg_conn.host,db_name,pg_conn.port)
    engine = create_engine(connection_str)
    inspector = inspect(engine)
    
    #create schemas if needed
    if schema_name not in map(str.lower, inspector.get_schema_names()):
        engine.execute("create schema {0}".format(schema_name))
        print("Schema {0} has been created".format(schema_name))
    
    #drop table if it exists
    for tbl in inspector.get_table_names(schema=schema_name):
        if table_name.lower() == tbl.lower():
            engine.execute("drop table {0}.{1}".format(schema_name, table_name))
            print("Table {0}.{1} has been dropped".format(schema_name, table_name))
    
    #create table
    engine.execute(create_table_sql_pg_str)
    print(create_table_sql_pg_str)
    
    #Add column names to text file for use in run_base_load_without_postprocess
    local_dir = context['ti'].xcom_pull(key='local_dir')
    load_col_file = os.path.join(os.sep, local_dir, "load_col_{0}_{1}.txt".format(schema_name,table_name))
    strList = create_table_sql_pg_str.split('\n')
    with open(load_col_file, 'w') as text_file: 
        for item in strList[1:-1]:
            col_name = item.lower().strip().split()[0]
            if col_name.lower() != 'nrdav2_loading_rowid':
                text_file.write('"'+col_name+'",\n')
