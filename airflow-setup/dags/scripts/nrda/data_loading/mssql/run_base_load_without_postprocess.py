import os 
import subprocess
import logging
import json

from airflow.hooks.base_hook import BaseHook

def run_base_load_without_postprocess(**context):
    ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']

    local_dir = context['ti'].xcom_pull(key='local_dir')

    load_schema = "load{0}t".format(project_code.lower())
    # overwrite schema if ms_database_schema is provided in attributes
    if 'ms_database_schema' in ledger["attributes"]:
        load_schema = "load{0}t".format(ledger["attributes"]["ms_database_schema"].lower())

    base_schema_prefix = "base"
    # check if QACK is used instead    
    if("load_to_qack" in ledger["attributes"]):
        base_schema_prefix="qack"    
    base_schema = "{0}{1}t".format(base_schema_prefix,project_code.lower())
    # overwrite schema if ms_database_schema is provided in attributes
    if 'ms_database_schema' in ledger["attributes"]:
        base_schema = ledger["attributes"]["ms_database_schema"].lower()

    # check if schema is overwritten    
    if("schema_to_overwrite" in ledger["attributes"]):
        schema=ledger["attributes"]["schema_to_overwrite"] #todo: need to revisit to see if {base/qack}{code}t format is needed

    #table = "nrdav2_{0}_{1}".format(ledger['label'],ledger['version'])
    table = ledger["attributes"]["targettablename"].format_map(Default(ledger)).format_map(Default(ledger["attributes"]))
    print("targettablename: ",table)
    # metadata = json.loads(ledger["attributes"]["schema"])
    # if metadata['schema_definition']['destinationTablename']:
    #     table = table.format_map(Default(destinationtablename=metadata['schema_definition']['destinationTablename'])).format_map(Default(ledger)).format_map(Default(ledger["attributes"]))        
    #     print("destinationtablename: ",table)


    sql_file_name = os.path.join(os.sep, local_dir, "{0}_{1}.sql".format(base_schema,table)) 

    load_col_file = os.path.join(os.sep, local_dir, "load_col_{0}_{1}.txt".format(load_schema,table)) 
    load_col_str = open(load_col_file, "r").read()

    sql_stm = """

-- Batch the insert to avoid transaction log issues

DECLARE @li_LoopNum bigint;
DECLARE @li_BatchStart bigint;
DECLARE @li_BatchEnd bigint;
DECLARE @li_BatchSize int;
DECLARE @li_RowsToUpdate bigint;
DECLARE @li_RowsRemaining bigint;
DECLARE @ldt_RunStart datetime2;
DECLARE @ldt_BatchStart datetime2;
DECLARE @lv_Msg varchar(500);

SET @li_LoopNum = 0;
SET @li_BatchStart = 0;
SET @li_BatchEnd = 0;

SET @li_BatchSize = 1000000;
SET @li_RowsToUpdate = ( SELECT MAX(nrdav2_loading_rowid) FROM [{2}].[{1}] );
SET @li_RowsRemaining = @li_RowsToUpdate;
SET @ldt_RunStart = GetDate();

-- Insert in batches

WHILE @li_RowsRemaining > 0
	BEGIN

		SET @li_LoopNum = @li_LoopNum + 1
		SET @lv_Msg = 'Loop: ' + CAST(@li_LoopNum AS char(3)) + ', Rows remaining: ' + CAST(@li_RowsRemaining AS varchar) + '/' + CAST(@li_RowsToUpdate AS varchar) + ', TimeStart: ' + CONVERT(varchar, GetDate(), 108)
		RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT
		SET @ldt_BatchStart = GetDate();
		SET @li_BatchEnd = @li_BatchStart + @li_BatchSize

        BEGIN TRANSACTION;

        SET @lv_Msg = 'INSERT TO [{0}].[{1}] SELECT {3} @ldt_RunStart FROM [{2}].[{1}] WHERE [nrdav2_loading_rowid] > ' + CAST(@li_BatchStart AS varchar) + ' AND [nrdav2_loading_rowid] <= ' + CAST(@li_BatchEnd AS varchar) + ';'
        RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT
        
        -- Can't do SELECT * now because LOAD table contains ID column [nrdav2_loading_rowid]
        INSERT INTO [{0}].[{1}] 
        SELECT {3} @ldt_RunStart         
        FROM [{2}].[{1}]
        WHERE [nrdav2_loading_rowid] > @li_BatchStart AND [nrdav2_loading_rowid] <= @li_BatchEnd;

		IF @@ERROR <> 0
			BEGIN 
				RAISERROR ('ROLLBACK REACHED IN INSERT', 10, 1) WITH NOWAIT
				ROLLBACK TRANSACTION
				--TRUNCATE TABLE [{0}].[{1}]
				PRINT('<<BASE_LOAD_FAILURE>>')
				RETURN
			END
		ELSE
			COMMIT TRANSACTION;

		SET @li_RowsRemaining = (@li_RowsToUpdate - @li_BatchEnd);
		SET @li_BatchStart = @li_BatchEnd
		SET @li_BatchEnd = 0
		SET @lv_Msg = 'Batch time (s): ' + CAST(DateDiff(second, @ldt_BatchStart, GetDate()) AS varchar) + ' - hh:mm:ss: ' +  CONVERT(varchar, DATEADD(ss, DateDiff(second, @ldt_BatchStart, GetDate()), 0), 108); 
		RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT
		PRINT('')

    END

    """.format(base_schema,table,load_schema,load_col_str)

    success_msg = "<<BASE_LOAD_SUCCESS>>"

    sql_stm = sql_stm + """
PRINT('{0}')
    """.format(success_msg)

    with open(sql_file_name, 'w') as text_file:
         text_file.write(sql_stm)

#     sql_stm = """ 
# /opt/mssql-tools/bin/bcp {4}.{5} IN {2} -S {6} -U {0} -P {1} -d {7} -t , -r "0x0a" -c -h TABLOCK -b 50000 -e {3}/log_{4}_{5}.txt 
#     """.format()
    # -r 0x0a
    print(sql_stm)

    sql_output = os.path.join(local_dir,"base_load_output.txt")

    db_conn_id = ledger['attributes']['ms_database_conn_id']
    db_name = ledger['attributes']['ms_database_name']
    
    mssql_conn = BaseHook.get_connection(db_conn_id)
    
    sql_stm = '/opt/mssql-tools/bin/sqlcmd -S {4} -U {0} -P {1} -d {5} -i {2} -o {3}  -t 1800 -m-1 -p 1 -X 1'.format(mssql_conn.login,mssql_conn.password,sql_file_name,sql_output,mssql_conn.host,db_name)

    # retval = subprocess.run(["cmd", "/c", sql_stm], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    
    print(sql_stm.replace("-P "+mssql_conn.password, "-P "+"xxxx"))

    retval = subprocess.Popen(['bash', '-c', sql_stm], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    retval.wait()
    (stdout, stderr) = retval.communicate()

    with open(sql_output, 'r') as output:
        output_str = output.read()
        print(output_str)
        if success_msg not in output_str:
            print("fail")
            raise ValueError("SQL failed")

    if retval.returncode != 0:
        print(stderr)
    else:
        print("success")



class Default(dict):
    def __missing__(self, key):
        return key
