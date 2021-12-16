import os 
import subprocess
import logging
import time
import json
import requests

from airflow.hooks.base_hook import BaseHook
from airflow import AirflowException

import sys
currentdir = os.path.dirname(os.path.realpath(__file__))

sys.path.append(currentdir)

from mssql_helpers import validateSchemaTableColumns,get_enc_col_list_information,checkInvalidCharacters

def run_base_load(**context):
    ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']
    
    db_conn_id = ledger['attributes']['ms_database_conn_id']
    db_name = ledger['attributes']['ms_database_name']


    driver = "{ODBC Driver 17 for SQL Server}"  # TODO: Set this via an attribute?

    schemaDef = json.loads(ledger["attributes"]["schema"])
    enc_col_list = schemaDef["return_info"]["schema_cols_to_enc"]
    if enc_col_list is None:
        enc_col_list = []
 
    index_col_list = schemaDef["return_info"]["schema_cols_to_index"]
    if index_col_list is None:
        index_col_list = []


    print("enc_col_list")
    print(enc_col_list)
    print("index_col_list")
    print(index_col_list)

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
        base_schema=ledger["attributes"]["schema_to_overwrite"] #todo: need to revisit to see if {base/qack}{code}t format is needed
    #base_schema = "base{0}t".format(project_code.lower())

    #table = "nrdav2_{0}_{1}".format(ledger['label'],ledger['version'])
    table = ledger["attributes"]["targettablename"].format_map(Default(ledger)).format_map(Default(ledger["attributes"]))
    print("targettablename: ",table)
    # metadata = json.loads(ledger["attributes"]["schema"])
    # if metadata['schema_definition']['destinationTablename']:
    #     table = table.format_map(Default(destinationtablename=metadata['schema_definition']['destinationTablename'])).format_map(Default(ledger)).format_map(Default(ledger["attributes"]))        
    #     print("destinationtablename: ",table)


    local_dir = context['ti'].xcom_pull(key='local_dir')
    sql_file_name = os.path.join(os.sep, local_dir, "{0}_{1}.sql".format(base_schema,table)) 


    # Validate schema/table/column names before creating dynamic sql
    col_name_list = []
    for col in enc_col_list:
        if col['enc_location'].lower() == "db":
            if col['enced_col_name'] is None or col['enced_col_name'] is "":
                col_name_list.append(col['col_name']+"_e")
            else:
                col_name_list.append(col['enced_col_name']+"_e")
    try:
        validateSchemaTableColumns(base_schema,table,col_name_list,db_conn_id,db_name,driver)
    except Exception as e:
        raise Exception(e)

    index_col_list = schemaDef["return_info"]["schema_cols_to_index"]
    if index_col_list is None:
        index_col_list = []
        ### Testing ###
        #index_col_list.append(["id"])


    
    sql_stm = ""


    # Get extra details for enc_col_list
    enc_col_details = get_enc_col_list_information(enc_col_list, "details")


    for col in enc_col_details:

        # Only process columns where ( enc_location == DB )
        if col['enc_location'].lower() != "db":
            continue                # Ignore current column and continue to next

        # Check target column names cleansed
        try:
            checkInvalidCharacters(col['colname'])
        except Exception as e:
            raise ValueError("Error in run_base_load() check target column names", e)

        case_sensitivity = col.get('case_sensitivity', 'ci').lower()
        if case_sensitivity.lower() == "cs":
            collation_for_case = " COLLATE LATIN1_GENERAL_CS_AS"
        else:
            collation_for_case = ""

        if col['action'].lower() == 'encrypt':     

            sql_stm = sql_stm + """

PRINT('Create temp table for each column needing key encryption. [{0}] [{1}]');
CREATE TABLE [#tmp_{0}] (
   [{0}] CHAR({1}),
   [{0}_e] BIGINT,
   [{0}_e_ori] VARBINARY(256),
   [nrdav2_tmp_{0}_row_id] BIGINT IDENTITY(1,1) CONSTRAINT [PK_tmp_{0}_rowid] PRIMARY KEY CLUSTERED
);

            """.format(col['colname'], col['keyColSizeOverride'])

        elif col['action'].lower() == 'substitute':
            
            sql_stm = sql_stm + """

PRINT('Create temp table for each column needing key substitution. [{0}] [{1}]');
CREATE TABLE [#tmp_{0}] (
   [{0}] VARCHAR({1}){2},
   [{0}_e] BIGINT,
   [nrdav2_tmp_{0}_row_id] BIGINT IDENTITY(1,1) CONSTRAINT [PK_tmp_{0}_rowid] PRIMARY KEY CLUSTERED
);

            """.format(col['colname'], col['keyColSizeOverride'], collation_for_case)


    sql_stm = sql_stm + """
DECLARE @lv_Salt varchar(255);
    """

    for col in enc_col_details:

        print("{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8}".format(col['action'], col['colname'], col['enc_location'], col['encryption'], col['key'], col['keyColSizeOverride'], col['location'], col['subTable'], col['substColPrefix']))

        enced_col_name = col['colname']

        # Only process columns where ( enc_location == DB )
        if col['enc_location'].lower() != "db":
            continue                # Ignore current column and continue to next        


        # Pull salt value from table to use in variable
        if col['location'].lower() == 'central':
            projLookup = 'CENTRAL'
            substLookup = 'CENTRAL'
        else:
            projLookup = project_code.upper()
            substLookup = col['subTable'].upper()

        # Handle case_sensitivity 
        case_sensitivity = col.get('case_sensitivity', 'ci').lower()        
        colname_case_adjusted = ""
        collation_for_case = ""
        if case_sensitivity == "cs":
            colname_case_adjusted = f"[{enced_col_name}]"
            collation_for_case = " COLLATE LATIN1_GENERAL_CS_AS"
        elif case_sensitivity == "ci":
            colname_case_adjusted = f"LOWER([{enced_col_name}])"
            collation_for_case = ""


        if col['action'].lower() == 'encrypt':

            sql_stm = sql_stm + """        
PRINT('--------------------------------------------------------------------------------------------------')

SET @lv_Salt = ''
SELECT @lv_Salt = ISNULL(SALT,'') FROM basectrlt.SUBSTITUTION WHERE PROJECT_CODE = '{0}' AND SUBSTITUTION_TABLE = '{1}'
IF @@ROWCOUNT <> 1
    BEGIN
        RAISERROR ('ERROR - SUBSTITUTION record not found [{0}]/[{1}]', 16, 1);
        RETURN
    END
IF @lv_Salt = ''
    BEGIN
        RAISERROR ('ERROR - SUBSTITUTION table SALT value is empty', 16, 1);
        RETURN
    END
PRINT('SALT:' + @lv_Salt) 
            """.format(projLookup,substLookup)

            sql_stm = sql_stm + """

PRINT('Get unique values to be encrypted. [#tmp_{0}] [{6}]');
INSERT INTO [#tmp_{0}] ([{0}])
SELECT DISTINCT 
	{4}{5}	
FROM [{1}].[{2}] a;

UPDATE [#tmp_{0}] SET [{0}_e_ori] = HASHBYTES('SHA2_512', CONCAT(@lv_Salt,CAST([{0}] AS char({3}))));

CREATE INDEX [ix_tmp_{0}_{0}_e_ori] ON [#tmp_{0}] ([{0}_e_ori]);

CREATE INDEX [ix_tmp_{0}] ON [#tmp_{0}] ([{0}]);                   -- TODO: check if this has been done elsewhere 

            """.format(enced_col_name, load_schema, table, col['keyColSizeOverride'], colname_case_adjusted, collation_for_case, case_sensitivity)


        elif col['action'] == 'substitute':


            sql_stm = sql_stm + """

PRINT('--------------------------------------------------------------------------------------------------')

PRINT('Get unique values to be substituted. [#tmp_{0}] [{5}]');
INSERT INTO #tmp_{0} ([{0}])
SELECT DISTINCT 
	{3}{4}
FROM [{1}].[{2}] a;

CREATE INDEX ix_tmp_{0} ON #tmp_{0} ([{0}]);                   -- TODO: check if this has been done elsewhere 

            """.format(enced_col_name, load_schema, table, colname_case_adjusted, collation_for_case, case_sensitivity)



    sql_stm = sql_stm + """

PRINT('--------------------------------------------------------------------------------------------------')

-- Prepare for batch processing the updates to the temp tables for each encrypted column

DECLARE @li_LoopNum bigint;
DECLARE @li_BatchStart bigint;
DECLARE @li_BatchEnd bigint;
DECLARE @li_BatchSize int;
DECLARE @li_RowsToUpdate bigint;
DECLARE @li_RowsRemaining bigint;
DECLARE @ldt_RunStart datetime2;
DECLARE @ldt_BatchStart datetime2;
DECLARE @lv_Msg varchar(500);

    """



    for col in enc_col_details:

        if col['enc_location'].lower() != "db":
            continue                # Ignore column and continue to next    
        

        if col['location'].lower()  == "central":
            substitutionTable = col['subTableName'].upper()          # Set substitution table to default to KEY50/ALF/HCP/etc.                   
        else:
            substitutionTable = "{0}_{1}".format(project_code, col['subTableName']).upper()   # Override encryption variable to PROJECTNAME + SUBSTITUTION TABLE               


        if col['action'].lower() == "encrypt":
            sql_stm = sql_stm + """

PRINT('--------------------------------------------------------------------------------------------------')

-- PRINT('[#tmp_{0}] - ENCRYPT - Batch update the #tmp_{0}.{0}_e value with bigint sequence from {2}_xref.{2}_e');

SET @li_LoopNum = 0;
SET @li_BatchStart = 0;
SET @li_BatchEnd = 0;

SET @li_BatchSize = 500000;
SET @li_RowsToUpdate = ( SELECT MAX([nrdav2_tmp_{0}_row_id]) FROM [#tmp_{0}] );
SET @li_RowsRemaining = @li_RowsToUpdate;
SET @ldt_RunStart = GetDate();

WHILE @li_RowsRemaining > 0
	BEGIN

		SET @li_LoopNum = @li_LoopNum + 1
		SET @lv_Msg = 'Loop: ' + CAST(@li_LoopNum AS char(3)) + ', Rows remaining: ' + CAST(@li_RowsRemaining AS varchar) + '/' + CAST(@li_RowsToUpdate AS varchar) + ', TimeStart: ' + CONVERT(varchar, GetDate(), 108)
		RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT
		SET @ldt_BatchStart = GetDate();

        SET @li_BatchEnd = @li_BatchStart + @li_BatchSize
		SET @lv_Msg = 'Update #tmp_{0}.{0}_e with {2}_xref.{2}_e - Batch where RowID > ' + CAST(@li_BatchStart AS varchar) + ' AND RowID <= ' +  CAST(@li_BatchEnd AS varchar)
		RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT

        BEGIN TRANSACTION;

        PRINT('Merge the existing keys into the list. [#tmp_{0}]');
        MERGE
        INTO [#tmp_{0}] tgt
        USING ( 
            select [{1}_e], [{1}_e_ori]
            from  basectrlt.[{2}_xref]		
            ) src
                ON tgt.[{0}_e_ori] = src.[{1}_e_ori]
        WHEN MATCHED AND ( tgt.[nrdav2_tmp_{0}_row_id] > @li_BatchStart AND tgt.[nrdav2_tmp_{0}_row_id] <= @li_BatchEnd )
        THEN UPDATE SET tgt.[{0}_e] = src.[{1}_e];

		IF @@ERROR <> 0
			BEGIN 
				RAISERROR ('ROLLBACK REACHED IN UPDATE', 10, 1) WITH NOWAIT
				ROLLBACK TRANSACTION 
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

            """.format(col['colname'], col['substColPrefix'], substitutionTable, project_code.lower())
        
        elif col['action'].lower() == "substitute":

            sql_stm = sql_stm + """

PRINT('--------------------------------------------------------------------------------------------------')

-- PRINT('[#tmp_{0}] - SUBSTITUTE - Batch update the #tmp_{0}.{0}_e value with bigint sequence from {2}_xref.{2}_e');

SET @li_LoopNum = 0;
SET @li_BatchStart = 0;
SET @li_BatchEnd = 0;

SET @li_BatchSize = 500000;
SET @li_RowsToUpdate = ( SELECT MAX([nrdav2_tmp_{0}_row_id]) FROM [#tmp_{0}] );
SET @li_RowsRemaining = @li_RowsToUpdate;
SET @ldt_RunStart = GetDate();

WHILE @li_RowsRemaining > 0
	BEGIN

		SET @li_LoopNum = @li_LoopNum + 1
		SET @lv_Msg = 'Loop: ' + CAST(@li_LoopNum AS char(3)) + ', Rows remaining: ' + CAST(@li_RowsRemaining AS varchar) + '/' + CAST(@li_RowsToUpdate AS varchar) + ', TimeStart: ' + CONVERT(varchar, GetDate(), 108)
		RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT
		SET @ldt_BatchStart = GetDate();

        SET @li_BatchEnd = @li_BatchStart + @li_BatchSize
		SET @lv_Msg = 'Update #tmp_{0}.{0}_e with {2}_xref.{2}_e - Batch where RowID > ' + CAST(@li_BatchStart AS varchar) + ' AND RowID <= ' +  CAST(@li_BatchEnd AS varchar)
		RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT

        BEGIN TRANSACTION;

        PRINT('Merge the existing keys into the list. [#tmp_{0}]');
        MERGE
        INTO [#tmp_{0}] tgt
        USING ( 
            select [{1}_e], [{1}_e_ori]
            from  basectrlt.[{2}_xref]		
            ) src
                ON tgt.[{0}] = src.[{1}_e_ori]
        WHEN MATCHED AND ( tgt.[nrdav2_tmp_{0}_row_id] > @li_BatchStart AND tgt.[nrdav2_tmp_{0}_row_id] <= @li_BatchEnd )
        THEN UPDATE SET tgt.[{0}_e] = src.[{1}_e];

		IF @@ERROR <> 0
			BEGIN 
				RAISERROR ('ROLLBACK REACHED IN UPDATE', 10, 1) WITH NOWAIT
				ROLLBACK TRANSACTION 
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

            """.format(col['colname'], col['substColPrefix'], substitutionTable)





    base_col_file = os.path.join(os.sep, local_dir, "base_col_{0}_{1}.txt".format(base_schema,table)) 
    base_col_str = open(base_col_file, "r").read()

    base_col_insert_file = os.path.join(os.sep, local_dir, "base_col_insert_{0}_{1}.txt".format(base_schema,table)) 
    base_col_insert_str = open(base_col_insert_file, "r").read()



    sql_stm = sql_stm + """

PRINT('Load the data including the key encryption - [{3}].[{2}]');

-- Prepare for batch processing the INSERT TO BASE LAYER

SET @li_LoopNum = 0;
SET @li_BatchStart = 0
SET @li_BatchEnd = 0

SET @li_BatchSize = 1000000;
SET @li_RowsToUpdate = ( SELECT MAX([nrdav2_loading_rowid]) FROM [{1}].[{2}] );
SET @li_RowsRemaining = @li_RowsToUpdate;
SET @ldt_RunStart = GetDate();

WHILE @li_RowsRemaining > 0
	BEGIN

		SET @li_LoopNum = @li_LoopNum + 1
		SET @lv_Msg = 'Loop: ' + CAST(@li_LoopNum AS char(3)) + ', Rows remaining: ' + CAST(@li_RowsRemaining AS varchar) + '/' + CAST(@li_RowsToUpdate AS varchar) + ', TimeStart: ' + CONVERT(varchar, GetDate(), 108)
		RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT
		SET @ldt_BatchStart = GetDate();

		SET @li_BatchEnd = @li_BatchStart + @li_BatchSize
		SET @lv_Msg = 'Insert into  [{3}].[{2}] - Batch where nrdav2_loading_rowid > ' + CAST(@li_BatchStart AS varchar) + ' AND nrdav2_loading_rowid <= ' +  CAST(@li_BatchEnd AS varchar)
		RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT

        BEGIN TRANSACTION;

        INSERT INTO [{3}].[{2}] (
        {4}
        [avail_from_dt]
        )
        SELECT 
        {0}
        GETDATE()
        FROM [{1}].[{2}] main
        """.format(base_col_str.strip(), load_schema, table, base_schema, base_col_insert_str.strip())

    for col in enc_col_details:
        if col['enc_location'].lower() != "db":
            continue                # Ignore current column and continue to next

        case_sensitivity = col.get('case_sensitivity', 'ci').lower()
        if case_sensitivity.lower() == "cs":
            collation_for_case = " COLLATE LATIN1_GENERAL_CS_AS"
        else:
            collation_for_case = ""

        sql_stm = sql_stm + """
            LEFT JOIN [#tmp_{0}] [temp_key_{0}_e] ON [temp_key_{0}_e].[{0}] = [main].[{0}]{1}
        """.format(col['colname'], collation_for_case)

    sql_stm = sql_stm + """
        WHERE main.nrdav2_loading_rowid > @li_BatchStart AND main.nrdav2_loading_rowid < = @li_BatchEnd;

        IF @@ERROR <> 0
            BEGIN 
                RAISERROR ('ROLLBACK REACHED IN INSERT', 10, 1) WITH NOWAIT
                ROLLBACK TRANSACTION 
                PRINT('<<BASE_LOAD_FAILURE>>')
                RETURN
            END
        ELSE
            COMMIT TRANSACTION;

		SET @li_RowsRemaining = @li_RowsToUpdate - @li_BatchEnd;
		SET @li_BatchStart = @li_BatchEnd
		SET @li_BatchEnd = 0
		SET @lv_Msg = 'Batch time (s): ' + CAST(DateDiff(second, @ldt_BatchStart, GetDate()) AS varchar) + ' - hh:mm:ss: ' +  CONVERT(varchar, DATEADD(ss, DateDiff(second, @ldt_BatchStart, GetDate()), 0), 108); 
		RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT
		PRINT('')

	END

    PRINT 'Runtime (s): ' + CAST(DateDiff(second, @ldt_RunStart, GetDate()) AS varchar) + ' - hh:mm:ss: ' +  CONVERT(varchar, DATEADD(ss, DateDiff(second, @ldt_RunStart, GetDate()), 0), 108); 

    """

    enc_col_name_list = [d['enced_col_name'].lower() for d in enc_col_list if d['enc_location'].lower() == 'db'] 

    print("enc col name list")
    print(enc_col_name_list)

    index_col_list_e = []       # Prepare a blank list to hold the renamed index types

    

    for index_type in index_col_list:		# For each index type - primary and secondary
        index_col_e = []											# Prepare a blank list to hold the index column names
        for colname in index_type:				# For each column in an index type
            if colname.lower() in enc_col_name_list:							# If the column is in the list of encrypted columns
                colname = colname + "_e"									# Change the name with _e suffix
            index_col_e.append(colname) 								# Add to the list of index column names
        index_col_list_e.append(index_col_e) 						# Add to the list of index types

    print("index_col_list_e")
    print(index_col_list_e)

    ix_seq = 0
    for ix in index_col_list_e:                   # For each index type - primary and secondary
        if len(ix) > 0:                             # If one or more column names are given
            ix_seq = ix_seq + 1                     # Sequence for index name
            colNamesQuoted = '[{0}]'.format('], ['.join(ix))      # Convert list of column names into a string. Double quote each item and comma between
            colNames = '_'.join(ix)

            sql_stm = sql_stm + """

PRINT('Create index for column IX_{3}_{4}');
-- Drop if indexes already exist
IF EXISTS ( SELECT 1 FROM sys.indexes WHERE name = 'IX_{3}_{4}' AND OBJECT_ID = OBJECT_ID('[{0}].[{1}]') )
    DROP INDEX [{0}].[{1}].IX_{3}_{4};

CREATE INDEX IX_{3}_{4} ON [{0}].[{1}] ({2});

            """.format(base_schema,table,colNamesQuoted,ix_seq,colNames)                   # For each index type - primary and secondary


    for col in enc_col_list:
        if col['enc_location'].lower() == "db":            
            sql_stm = sql_stm + """
DROP TABLE #tmp_{0};
            """.format(col['enced_col_name']) 


    success_msg = "<<BASE_LOAD_SUCCESS>>"

    sql_stm = sql_stm + """
PRINT('{0}')
    """.format(success_msg)

    with open(sql_file_name, 'w') as text_file:
         text_file.write(sql_stm)

    #logging.info(sql_stm)
    logging.info("base_load sql file is generated at: {0}".format(sql_file_name))
    #os.system(". /usr/local/airflow/sqllib/db2profile & db2 -tvmf {0} | tee {1}/log_{2}_{3}.txt".format(sql_file_name,output_msg_dir,base_schema,table))
    #subprocess.call(['bash', '-c', '. /usr/local/airflow/sqllib/db2profile && db2 +c -tvmf {0} -z {1}/log_{2}_{3}.txt'.format(sql_file_name,output_msg_dir,base_schema,table)])

    print("SQL to run:")
    print(sql_stm)
    
    ## TODO - Pull values from vault
    sql_output = os.path.join(local_dir,"base_load_output.txt")

    db_conn_id = ledger['attributes']['ms_database_conn_id']
    db_name = ledger['attributes']['ms_database_name']
    
    mssql_conn = BaseHook.get_connection(db_conn_id)

    sql_stm = '/opt/mssql-tools/bin/sqlcmd -S {4} -U {0} -P {1} -d {5} -i {2} -o {3}  -t 1800 -m-1 -p 1 -X 1'.format(mssql_conn.login,mssql_conn.password,sql_file_name,sql_output,mssql_conn.host,db_name)

    # retval = subprocess.run(["cmd", "/c", sql_stm], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    
    print(sql_stm.replace("-P "+mssql_conn.password, "-P "+"xxxx"))

    print("Starting base load...")
    start = time.process_time()

    retval = subprocess.Popen(['bash', '-c', sql_stm], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # retval.wait()
    (stdout, stderr) = retval.communicate()

    print("TimeTaken: ", time.process_time() - start)

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

