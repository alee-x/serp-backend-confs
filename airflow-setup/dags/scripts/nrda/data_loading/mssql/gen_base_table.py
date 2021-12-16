import json
import os 
from subprocess import PIPE, Popen
import pyspark.sql
from os.path import expanduser, join, abspath
import sqlalchemy
import airflow
from sqlalchemy import *
#from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import mssql
from pyspark.sql.types import *
from sqlalchemy.schema import CreateTable
from airflow.hooks.base_hook import BaseHook
import re
#import ibm_db_sa
#import ibm_db_dbi as db
import pyodbc

def gen_base_table(**context):
    ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code'].lower()
    schema = json.loads(ledger["attributes"]["schema"])

    columns_names = []
    columns_types = []
    nullable_flags = []
    primary_key_flags = []
    enc_col_list = []
    enc_col_name_list = []
    if schema["return_info"]["schema_cols_to_enc"]:
        enc_col_list = schema["return_info"]["schema_cols_to_enc"]
        enc_col_name_list = [d['enced_col_name'].lower() for d in enc_col_list if d['enc_location'].lower() == 'db'] 

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
        to_enc_item = next((d for d in enc_col_list if d['enced_col_name'].lower() == item.name.lower() and d['enc_location'].lower() == 'db'), None)

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
            print(item.name + " " + str(item.dataType))
            if isinstance(item.dataType, StringType) or isinstance(item.dataType, BooleanType):
                max_len = 30
                for i in schema_definition['fieldDef']:
                    col_name = i['column_Name']

                    if i['dest_Col_Name'] is not '' and i['dest_Col_Name'] is not None:
                        col_name = i['dest_Col_Name']
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

    columns_names.append("avail_from_dt")
    columns_types.append(Date)
    nullable_flags.append(item.nullable)
    primary_key_flags.append(False)

    #####################
    # QACK STUFF?????
    #####################

    schema_name = "base{0}t".format(project_code.lower())
    # overwrite schema if ms_database_schema is provided in attributes
    if 'ms_database_schema' in ledger["attributes"]:
        schema_name = ledger["attributes"]["ms_database_schema"].lower()

    #table_name = "nrdav2_{0}_{1}".format(ledger['label'],ledger['version'])
    table_name = ledger["attributes"]["targettablename"].format_map(Default(ledger)).format_map(Default(ledger["attributes"]))
    print("targettablename: ",table_name)
    # metadata = json.loads(ledger["attributes"]["schema"])
    # if metadata['schema_definition']['destinationTablename']:
    #     table_name = table_name.format_map(Default(destinationtablename=metadata['schema_definition']['destinationTablename'])).format_map(Default(ledger)).format_map(Default(ledger["attributes"]))
    #     print("destinationtablename: ",table_name)

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
    #create_table_sql_db2_str = "{0} IN BASEADHCT_PD_DATA;".format(create_table_sql_str.strip().replace('"', '')) 
    create_table_sql_str="{0}".format(CreateTable(tab_to_load).compile(dialect=mssql.dialect()))
    create_table_sql_mssql_str = create_table_sql_str.strip().replace('"', '')

    db_conn_id = ledger['attributes']['ms_database_conn_id']
    db_name = ledger['attributes']['ms_database_name']
    
    mssql_conn = BaseHook.get_connection(db_conn_id)
    
    print("server: ",mssql_conn.host)  
    print("database: ", db_name)
    
    connection_str = r'mssql+pyodbc://{0}:{1}@{2}/{3}?driver=ODBC+DRIVER+17+for+SQL+Server'.format(mssql_conn.login,mssql_conn.password,mssql_conn.host,db_name)
    
    engine = create_engine(connection_str)
    inspector = inspect(engine)
    
    #create schemas if needed
    if schema_name.lower() not in map(str.lower, inspector.get_schema_names()):
        cmd = "create schema [{0}]".format(schema_name)
        print(cmd)
        engine.execute(cmd)
        print("Schema {0} has been created".format(schema_name))
    
    #drop table if it exists
    for tbl in inspector.get_table_names(schema=schema_name):
        if table_name.lower() == tbl.lower():
            cmd = "drop table [{0}].[{1}]".format(schema_name, table_name)
            print(cmd)
            engine.execute(cmd)
            print("Table [{0}].[{1}] has been dropped".format(schema_name, table_name))
    
    #create table
    print(create_table_sql_mssql_str)
    engine.execute(create_table_sql_mssql_str)
    
    print("enc_col_list")
    print(enc_col_list)
    print("enc_col_name_list")
    print(enc_col_name_list)

    local_dir = context['ti'].xcom_pull(key='local_dir')

    base_col_file = os.path.join(os.sep, local_dir, "base_col_{0}_{1}.txt".format(schema_name,table_name)) 
    #base_col_file = os.path.join("C:\\test\\Runtest", "base_col_{0}_{1}.txt".format(schema_name,table_name)) 
    strList = create_table_sql_mssql_str.split('\n')
    with open(base_col_file, 'w') as text_file:        
        for item in strList[1:-2]:
            col_name = item.lower().strip().split()[0]
            if re.sub('\_e$', '', col_name) not in enc_col_name_list:
                print("main."+col_name)
                text_file.write("main.["+col_name+"],\n")
            else:
                print("temp_key_{0}.{0}".format(col_name))
                text_file.write("temp_key_{0}.[{0}],\n".format(col_name))

    # Make similar list for column names of target table - without the alias prefix - so the INSERT INTO has the right columns in the right order
    base_col_insert_file = os.path.join(os.sep, local_dir, "base_col_insert_{0}_{1}.txt".format(schema_name,table_name))
    with open(base_col_insert_file, 'w') as text_file: 
        for item in strList[1:-2]:
            col_name = item.lower().strip().split()[0]
            text_file.write("["+col_name+"],\n")



class Default(dict):
    def __missing__(self, key):
        return key
