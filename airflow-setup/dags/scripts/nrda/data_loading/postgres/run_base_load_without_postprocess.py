import os 
import subprocess
import logging
import psycopg2

from airflow.hooks.base_hook import BaseHook

def run_base_load_without_postprocess(**context):
    ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']

    local_dir = context['ti'].xcom_pull(key='local_dir')

    load_schema = "load{0}t".format(project_code.lower())

    base_schema_prefix = "base"
    # check if QACK is used instead    
    if("load_to_qack" in ledger["attributes"]):
        base_schema_prefix="qack"    
    base_schema = "{0}{1}t".format(base_schema_prefix,project_code.lower())
    # overwrite schema if ms_database_schema is provided in attributes
    if 'pg_database_schema' in ledger["attributes"]:
        load_schema = "load{0}t".format(ledger["attributes"]["pg_database_schema"].lower())
        base_schema = "{0}{1}t".format(base_schema_prefix,ledger["attributes"]["pg_database_schema"].lower())
    # check if schema is overwritten    
    if("schema_to_overwrite" in ledger["attributes"]):
        schema=ledger["attributes"]["schema_to_overwrite"] #todo: need to revisit to see if {base/qack}{code}t format is needed

    table = "nrdav2_{0}_{1}".format(ledger['label'],ledger['version'].replace('-',''))
    sql_file_name = os.path.join(os.sep, local_dir, "{0}_{1}.sql".format(base_schema,table)) 

    load_col_file = os.path.join(os.sep, local_dir, "load_col_{0}_{1}.txt".format(load_schema,table)) 
    load_col_str = open(load_col_file, "r").read()

    sql_stm = """

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

    """.format(base_schema.lower(),table.lower(),load_schema.lower(),load_col_str)

    success_msg = "<<BASE_LOAD_SUCCESS>>"

#     sql_stm = sql_stm + """
# DO $$
# BEGIN
# raise notice '%', '{0}';
# END
# $$
# LANGUAGE plpgsql;
#     """.format(success_msg)

    with open(sql_file_name, 'w') as text_file:
         text_file.write(sql_stm)


    sql_output = os.path.join(local_dir,"base_load_output.txt")

    db_conn_id = ledger['attributes']['pg_database_conn_id']
    db_name = ledger['attributes']['pg_database_name']
    #db_conn_id = 'postgres_test_69.23'
    #db_name = 'pg_load_test'

    pg_conn = BaseHook.get_connection(db_conn_id)
    
    # try:
    #     print(sql_stm)
    #     conn = psycopg2.connect("host={0} dbname={1} user={2} password={3}".format(pg_conn.host, 'pg_load_test', pg_conn.login,pg_conn.password))
    #     cur = conn.cursor()
    #     cur.execute(sql_stm)
    #     conn.commit()
    # except Exception as err:    
    #     cursor = None
    #     print ("\npsycopg2 error:", err)

    # with open(sql_output, 'w') as f:
    #     for item in conn.notices:
    #         f.write("%s\n" % item)


    #sql_stm = '/opt/mssql-tools/bin/sqlcmd -S {4} -U {0} -P {1} -d {5} -i {2} -o {3}  -t 1800 -m-1 -p 1 -X 1'.format(mssql_conn.login,mssql_conn.password,sql_file_name,sql_output,mssql_conn.host,db_name)
    sql_stm = 'psql -d "host={4} port={6} user={0} password={1} dbname={5}" -f {2} -o {3}'.format(pg_conn.login,pg_conn.password,sql_file_name,sql_output,pg_conn.host,db_name,pg_conn.port)
    
    print(sql_stm)

    retval = subprocess.Popen(['bash', '-c', sql_stm], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    retval.wait()
    (stdout, stderr) = retval.communicate()
    
    notices = stderr.decode("utf-8")
    print(notices)

    if success_msg not in notices:
        print("fail")
        raise ValueError("SQL failed")

    if retval.returncode != 0:
        print(notices)
    else:
        print("success")

