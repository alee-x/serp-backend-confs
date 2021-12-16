import sys
import os
import boto3
import json
from botocore.client import Config
from botocore.exceptions import ClientError
import sqlalchemy
from sqlalchemy import *
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable
import psycopg2

import logging
# Logger
logger = logging.getLogger(__name__)
logging.basicConfig(format='%(module)s %(levelname)s: %(message)s',level=logging.INFO) 

if __name__ == "__main__":

    for arg in sys.argv[1:]:
        logger.info("var: " + arg)


    bucket_name = 'serp-sail'
    dag_run_id = 'dataset1_workflow3_ledger98312'
    project_code = 'cvst'
    # bucket_name = sys.argv[1]
    # dag_run_id = sys.argv[2]
    # project_code = sys.argv[3]
    # ledger_json_s3_path = sys.argv[4]
    ledger_json_s3_path = '{0}/jobs/{1}/ledger.json'.format(project_code, dag_run_id)



    #DEBUG code
    # for a in os.environ:
    #     logger.info('Var: {0} - Value: {1}'.format(a, os.getenv(a)))
    # logger.info("all done")
    
    s3_client  = boto3.client('s3',aws_access_key_id = 'admin', aws_secret_access_key = 'MyP4ssW0rd', endpoint_url = 'http://127.0.0.1:9000', config=Config(signature_version='s3v4'))
    ledger_obj = s3_client.get_object(Bucket=bucket_name, Key=ledger_json_s3_path)
    ledger = json.loads(ledger_obj['Body'].read())
    schema = json.loads(ledger["attributes"]["schema"])

    # download csv file
    csv_s3_key =  '{0}/jobs/{1}/data.csv'.format(project_code, dag_run_id)
    local_csv_path = '/tmp/data.csv'
    logger.info("start downloading csv file " + csv_s3_key)
    res = s3_client.list_objects_v2(Bucket=bucket_name,Prefix=csv_s3_key)
    if "Contents" in res:
        for obj in res['Contents']:
            if obj['Key'].endswith('.csv'):
                s3_client.download_file(bucket_name, obj['Key'], local_csv_path)
                logger.info("{0} has been downloaded to {1}".format(obj['Key'], local_csv_path))

    columns_names = []
    columns_types = []
    nullable_flags = []
    primary_key_flags = []
    enc_col_list = []

    if schema['return_info']['schema_cols_to_enc']:
        enc_col_list = schema['return_info']['schema_cols_to_enc']
    schema_to_apply = schema["return_info"]["schema_to_apply"]
    schema_definition = schema["schema_definition"]
    schema_to_rename = schema["return_info"]["schema_cols_to_rename"]

    # rename for compare
    if schema_to_rename:
         for i in schema_definition["fieldDef"]:
                if i["column_Name"] in schema_to_rename:
                    i["column_Name"] = schema_to_rename[i["column_Name"]]

    # add col def for nrdav2_loading_rowid
    columns_names.append("nrdav2_loading_rowid")
    columns_types.append(BigInteger)
    nullable_flags.append(False)
    primary_key_flags.append(False) 

    for item in schema_to_apply['fields']:
        # logger.info(item)
        columns_names.append(item['name'].lower())
        nullable_flags.append(item['nullable'])
        primary_key_flags.append(False)

        to_enc_item = next((d for d in enc_col_list if d['enced_col_name'].lower() == item['name'].lower() and d['enc_location'] == 'DB'), None)
        if to_enc_item:
            logger.info('columns to enc: {0}'.format(to_enc_item))
            if to_enc_item['enc_type'].upper() == 'KEY50':
                columns_types.append(CHAR(50))
            elif to_enc_item['enc_type'] == 'HCP':
                columns_types.append(CHAR(12))
            else:
                columns_types.append(CHAR(30))  #todo: need to refine
        else:
            # StringType or BooleanType --> VARCHAR(max_length)
            # BooleanType in case of 'False' or 0
            if item['type'] in ('string', 'boolean'):
                max_len = 30
                for i in schema_definition['fieldDef']:
                    col_name = i['column_Name']

                    if i['dest_Col_Name'] != '' and i['dest_Col_Name'] is not None:
                        col_name = i['dest_Col_Name']

                    if col_name.lower() == item['name'].lower():
                        max_len = i['length']
                        # logger.info("max len: ", max_len)
                        break
                
                if max_len <= 8000:
                    columns_types.append(String(max_len))
                else:
                    columns_types.append(String(None))  # Create as varchar(max)

            elif item['type'] in ('byte', 'short'):
                columns_types.append(SmallInteger)
            elif item['type'] == 'integer':
                columns_types.append(Integer)    
            elif item['type'] == 'long':
                columns_types.append(BigInteger)  
            elif item['type'] in ('float', 'double'):
                columns_types.append(Float)
            elif item['type'].startswith('decimal'):
                pre_scal_str_list = item['type'][8:-1].split(',')
                columns_types.append(DECIMAL(int(pre_scal_str_list[0]), int(pre_scal_str_list[1])))
            elif item['type'] == 'binary':
                columns_types.append(LargeBinary)
            elif item['type'] == 'timestamp':
                columns_types.append(DateTime)
            elif item['type'] == 'date':
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


    # db_login_name = os.getenv('PG_DB_USERNAME')
    # db_login_passowrd = os.getenv('PG_DB_PASSWORD')
    # db_host = os.getenv('PG_DB_HOST')
    # db_dbname = os.getenv('PG_DB_DBNAME')
    # db_port = os.getenv('PG_DB_PORT')
    db_login_name = 'admin'
    db_login_passowrd = 'MyP4ssW0rd'
    db_host = '127.0.0.1'
    db_dbname = 'SAIL'
    db_port = 5438

    
    connection_str = r'postgresql://{0}:{1}@{2}:{4}/{3}'.format(db_login_name,db_login_passowrd,db_host,db_dbname,db_port)
    engine = create_engine(connection_str)
    inspector = inspect(engine)

    #create schemas if needed
    if schema_name not in map(str.lower, inspector.get_schema_names()):
        engine.execute("create schema {0}".format(schema_name))
        logger.info("Schema {0} has been created".format(schema_name))

    #drop table if it exists
    for tbl in inspector.get_table_names(schema=schema_name):
        if table_name.lower() == tbl.lower():
            engine.execute("drop table {0}.{1}".format(schema_name, table_name))
            logger.info("Table {0}.{1} has been dropped".format(schema_name, table_name))

    #create table
    engine.execute(create_table_sql_pg_str)
    logger.info(create_table_sql_pg_str)

    #Add column names to text file for use in run_base_load_without_postprocess
    load_col_file = os.path.join(os.sep, "tmp", "load_col.txt")
    strList = create_table_sql_pg_str.split('\n')
    with open(load_col_file, 'w') as text_file: 
        for item in strList[1:-1]:
            col_name = item.lower().strip().split()[0]
            if col_name.lower() != 'nrdav2_loading_rowid':
                text_file.write('"'+col_name+'",\n')
    s3_load_col_file_path = '{0}/jobs/{1}/load_col.txt'.format(project_code, dag_run_id)
    s3_client.upload_file(load_col_file, bucket_name,s3_load_col_file_path)
    logger.info("load_col.txt file has been uploaded to {0}/{1}".format(bucket_name,s3_load_col_file_path))
    #load csv to the created table
    #todo: https://github.com/psycopg/psycopg2/issues/1294
    conn = psycopg2.connect("host={0} dbname={1} user={2} password={3} port={4}".format(db_host, db_dbname, db_login_name, db_login_passowrd, db_port))
    cur = conn.cursor()
    cur.execute('SET search_path TO {0}, public'.format(schema_name))
    print(local_csv_path)
    with open(local_csv_path, 'r') as f:
        cur.copy_from(f, table_name, sep=',',null='') #https://www.psycopg.org/docs/cursor.html#cursor.copy_from

    conn.commit()
    logger.info("csv file has been loaded to pg")