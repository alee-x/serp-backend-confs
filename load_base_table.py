import sys
import os
import boto3
import subprocess
import json
from botocore.client import Config
from botocore.exceptions import ClientError
import sqlalchemy
from sqlalchemy import *
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable
import psycopg2
import re
import logging
# Logger
logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(module)s %(levelname)s: %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO) 

if __name__ == "__main__":

    for arg in sys.argv[1:]:
        logger.info("var: " + arg)

    bucket_name = sys.argv[1]
    dag_run_id = sys.argv[2]
    project_code = sys.argv[3]
    # ledger_json_s3_path = sys.argv[4]
    ledger_json_s3_path = '{0}/jobs/{1}/ledger.json'.format(project_code, dag_run_id)

    #DEBUG code
    # for a in os.environ:
    #     logger.info('Var: {0} - Value: {1}'.format(a, os.getenv(a)))
    # logger.info("all done")

    s3_client  = boto3.client('s3',aws_access_key_id = os.getenv('S3_ACCESS_KEY'), aws_secret_access_key = os.getenv('S3_ACCESS_SECRET'), endpoint_url = os.getenv('S3_ENDPOINT'), config=Config(signature_version='s3v4'))
    ledger_obj = s3_client.get_object(Bucket=bucket_name, Key=ledger_json_s3_path)
    ledger = json.loads(ledger_obj['Body'].read())
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

    for item in schema_to_apply['fields']:
        nullable_flags.append(item['nullable'])
        primary_key_flags.append(False)

        to_enc_item = next((d for d in enc_col_list if d['enced_col_name'].lower() == item['name'].lower() and d['enc_location'].lower() == 'db'), None)
        if to_enc_item:
            logger.info('columns to enc: {0}'.format(to_enc_item))
            
            if to_enc_item['enc_type'].upper() == 'HCP':
                columns_types.append(Integer)
            else:
                columns_types.append(BigInteger)
            columns_names.append(re.sub('\_e$', '', item['name'].lower()) + "_e")
        else:
            # StringType or BooleanType --> VARCHAR(max_length)
            # BooleanType in case of 'False' or 0
            columns_names.append(item['name'].lower())
            if item['type'] in ('string', 'boolean'):
                max_len = 30
                for i in schema_definition['fieldDef']:
                    col_name = i['column_Name']

                    if i['dest_Col_Name'] != '' and i['dest_Col_Name'] is not None:
                        col_name = i['dest_Col_Name']

                    if col_name.lower() == item['name'].lower():
                        max_len = i['length']
                        logger.info("max len: {0}".format(max_len))
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

    columns_names.append("avail_from_dt")
    columns_types.append(Date)
    nullable_flags.append(item['nullable'])
    primary_key_flags.append(False)

    schema_name = "base{0}t".format(project_code.lower())
    # overwrite schema if ms_database_schema is provided in attributes
    if 'pg_database_schema' in ledger["attributes"]:
        schema_name = "base{0}t".format(ledger["attributes"]["pg_database_schema"].lower())
    # check if schema is overwritten    
    if("schema_to_overwrite" in ledger["attributes"]):
        schema_name=ledger["attributes"]["schema_to_overwrite"]

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

    create_table_sql_str="{0}".format(CreateTable(tab_to_load).compile(dialect=postgresql.dialect()))
    create_table_sql_pg_str = create_table_sql_str.strip().replace('"', '')

    db_login_name = os.getenv('PG_DB_USERNAME')
    db_login_passowrd = os.getenv('PG_DB_PASSWORD')
    db_host = os.getenv('PG_DB_HOST')
    db_dbname = os.getenv('PG_DB_DBNAME')
    db_port = os.getenv('PG_DB_PORT')
    
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
    logger.info("enc_col_list: {0}".format(enc_col_list))
    logger.info("enc_col_name_list: {0}".format(enc_col_name_list))

    #Add column names to text file for use in run_base_load_without_postprocess
    base_col_file = os.path.join(os.sep, "tmp", "base_col.txt")
    strList = create_table_sql_pg_str.split('\n')
    with open(base_col_file, 'w') as text_file:        
        for item in strList[1:-2]:
            col_name = item.lower().strip().split()[0]
            if re.sub('\_e$', '', col_name) not in enc_col_name_list:
                logger.info("main."+col_name)
                text_file.write("main.["+col_name+"],\n")
            else:
                logger.info("temp_key_{0}.{0}".format(col_name))
                text_file.write("temp_key_{0}.[{0}],\n".format(col_name))

    s3_base_col_file_path = '{0}/jobs/{1}/base_col.txt'.format(project_code, dag_run_id)
    s3_client.upload_file(base_col_file, bucket_name,s3_base_col_file_path)
    logger.info("base_col.txt file has been uploaded to {0}/{1}".format(bucket_name,s3_base_col_file_path))

    # Make similar list for column names of target table - without the alias prefix - so the INSERT INTO has the right columns in the right order
    base_col_insert_file =os.path.join(os.sep, "tmp", "base_col_insert.txt")
    with open(base_col_insert_file, 'w') as text_file: 
        for item in strList[1:-2]:
            col_name = item.lower().strip().split()[0]
            text_file.write("["+col_name+"],\n")

    s3_base_col_insert_file_path = '{0}/jobs/{1}/base_col_insert.txt'.format(project_code, dag_run_id)
    s3_client.upload_file(base_col_insert_file, bucket_name,s3_base_col_insert_file_path)
    logger.info("base_col_insert.txt file has been uploaded to {0}/{1}".format(bucket_name,s3_base_col_insert_file_path))

    # load to base

    load_schema = "load{0}t".format(project_code.lower())
    if 'pg_database_schema' in ledger["attributes"]:
        load_schema = "load{0}t".format(ledger["attributes"]["pg_database_schema"].lower())

    sql_file_name =os.path.join(os.sep, "tmp", "base_load.sql")
    s3_load_col_file_path = '{0}/jobs/{1}/load_col.txt'.format(project_code, dag_run_id)
    col_file_obj = s3_client.get_object(Bucket=bucket_name, Key=s3_load_col_file_path)
    load_col_str = col_file_obj['Body'].read()
    logger.info(load_col_str.decode("utf-8"))
    
    sql_stm = """
SET client_min_messages TO NOTICE;
DO LANGUAGE plpgsql $$
<<mainblock>>
DECLARE 
li_LoopNum bigint := 0;
li_BatchStart bigint := 0;
li_BatchEnd bigint := 0;
li_BatchSize int := 0;
li_RowsToUpdate bigint;
li_RowsRemaining bigint := 0;
li_RowsToEncrypt bigint := 0;
ldt_RunStart timestamp;
ldt_BatchStart timestamp;
lv_Msg varchar(500);

BEGIN

-- Batch the insert to avoid transaction log issues

li_LoopNum := 0;
li_BatchStart := 0;
li_BatchEnd := 0;

li_BatchSize := 1000000;
li_RowsToUpdate := ( SELECT MAX(nrdav2_loading_rowid) FROM "{2}"."{1}" );
li_RowsRemaining := li_RowsToUpdate;
ldt_RunStart := clock_timestamp();

-- Insert in batches

while li_RowsRemaining > 0 loop

		li_LoopNum := li_LoopNum + 1;
		lv_Msg := CONCAT('Loop: ', CAST(li_LoopNum AS char(3)), ', Rows remaining: ', CAST(li_RowsRemaining AS varchar(20)), '/', CAST(li_RowsToUpdate AS varchar(20)), ', TimeStart: ', TO_CHAR(clock_timestamp(), 'YYYY-MM-DD HH:MI:SS'));
		raise notice '%', lv_Msg;
		ldt_BatchStart := clock_timestamp();
		li_BatchEnd := li_BatchStart + li_BatchSize;

        --BEGIN TRANSACTION;

        lv_Msg := CONCAT('INSERT TO ', ' {0} ', ' {1} ', ' SELECT ', ' [column list] ', TO_CHAR(ldt_RunStart, 'YYYY-MM-DD HH:MI:SS'), ' FROM [{2}].[{1}] WHERE [nrdav2_loading_rowid] > ', CAST(li_BatchStart AS varchar(10)), ' AND [nrdav2_loading_rowid] <= ', CAST(li_BatchEnd AS varchar(10)), '--');
        raise notice '%', lv_Msg;

        -- Can't do SELECT * now because LOAD table contains ID column [nrdav2_loading_rowid]

        begin                   -- use begin...end to allow exception to rollback the failed iteration without transaction error

            INSERT INTO "{0}"."{1}" 
            SELECT {3} ldt_RunStart         
            FROM "{2}"."{1}"
            WHERE "nrdav2_loading_rowid" > li_BatchStart AND "nrdav2_loading_rowid" <= li_BatchEnd;

 		exception when others then
			raise notice '%', '<<BASE_LOAD_FAILURE>>';
            raise notice 'ROLLBACK REACHED IN INSERT - Loop %', li_LoopNum;
            raise notice 'FAIL POINT: Exception details: % %', SQLERRM, SQLSTATE;
			EXIT mainblock;	

		end;

        raise notice 'Commit loop %', li_LoopNum;
        COMMIT;

		li_RowsRemaining := (li_RowsToUpdate - li_BatchEnd);
		li_BatchStart := li_BatchEnd;
		li_BatchEnd := 0;
		lv_Msg := CONCAT('Batch time (s): ', CAST(extract(epoch from (ldt_BatchStart - clock_timestamp())) AS varchar(20)));
		raise notice '%', lv_Msg;

end loop;

lv_Msg := CONCAT('Run time (s): ', CAST(extract(epoch from (ldt_RunStart - clock_timestamp())) AS varchar(20)));
raise notice '%', lv_Msg;
    
raise notice '%', '<<BASE_LOAD_SUCCESS>>';

END
$$;	

    """.format(schema_name.lower(),table_name.lower(),load_schema.lower(),load_col_str.decode("utf-8"))

    logger.info(sql_stm)
    success_msg = "<<BASE_LOAD_SUCCESS>>"

    with open(sql_file_name, 'w') as text_file:
         text_file.write(sql_stm)

    sql_output = os.path.join(os.sep, "tmp", "base_load_output.txt")

    db_login_name = os.getenv('PG_DB_USERNAME')
    db_login_passowrd = os.getenv('PG_DB_PASSWORD')
    db_host = os.getenv('PG_DB_HOST')
    db_dbname = os.getenv('PG_DB_DBNAME')
    db_port = os.getenv('PG_DB_PORT')

    sql_stm = 'psql -d "host={4} port={6} user={0} password={1} dbname={5}" -f {2} -o {3}'.format(db_login_name,db_login_passowrd,sql_file_name,sql_output,db_host,db_dbname,db_port)

    retval = subprocess.Popen(['bash', '-c', sql_stm], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    retval.wait()
    (stdout, stderr) = retval.communicate()
    
    notices = stderr.decode("utf-8")
    logger.info("NOTICE: " + notices)

    if success_msg not in notices:
        logger.info("fail")
        raise ValueError("SQL failed")

    if retval.returncode != 0:
        logger.info(notices)
    else:
        logger.info("success")