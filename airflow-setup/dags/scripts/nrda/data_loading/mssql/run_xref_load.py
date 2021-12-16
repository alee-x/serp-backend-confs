import sys
import os 
import subprocess
import logging
import time
import json
import requests
from airflow.hooks.base_hook import BaseHook

currentdir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(currentdir)

from mssql_helpers import validateSchemaTableColumns
from mssql_helpers import get_enc_col_list_information
from mssql_helpers import get_substitution_info
from mssql_helpers import checkInvalidCharacters
from xref_template_mssql import print_xref_for_encrypt
from xref_template_mssql import print_xref_for_substitute
from xref_template_mssql import create_user_defined_encryption_table
from xref_template_mssql import create_user_defined_substitution_table
from xref_template_mssql import create_central_encryption_table
from xref_template_mssql import create_central_substitution_table
from xref_template_mssql import print_substitution_table_lookup

parentdir = os.path.dirname(os.path.dirname(currentdir))

sys.path.append(parentdir)
from helpers import get_encryption_key_size

def run_xref_load(**context):
    ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']
    schemaDef = json.loads(ledger["attributes"]["schema"])
    enc_col_list = schemaDef["return_info"]["schema_cols_to_enc"]
    if enc_col_list is None:
        enc_col_list = []


    db_conn_id = ledger['attributes']['ms_database_conn_id']
    db_name = ledger['attributes']['ms_database_name']
    
    
    driver = "{ODBC Driver 17 for SQL Server}"  # TODO: Set this via an attribute?

    schema = "load{0}t".format(project_code.lower())
    # overwrite schema if ms_database_schema is provided in attributes
    if 'ms_database_schema' in ledger["attributes"]:
        schema = "load{0}t".format(ledger["attributes"]["ms_database_schema"].lower())
    
    #table = "nrdav2_{0}_{1}".format(ledger['label'],ledger['version'])
    table = ledger["attributes"]["targettablename"].format_map(Default(ledger)).format_map(Default(ledger["attributes"]))
    print("targettablename: ",table)
    # metadata = json.loads(ledger["attributes"]["schema"])
    # if metadata['schema_definition']['destinationTablename']:
    #     table = table.format_map(Default(destinationtablename=metadata['schema_definition']['destinationTablename'])).format_map(Default(ledger)).format_map(Default(ledger["attributes"]))        
    #     print("destinationtablename: ",table)

    local_dir = context['ti'].xcom_pull(key='local_dir')
    sql_file_name = os.path.join(os.sep, local_dir, "{0}_xref_{1}_.sql".format(schema,table)) 


    print(enc_col_list)
    print(ledger)


    # Validate schema/table/column names before creating dynamic sql
    col_name_list = [d['enced_col_name'].lower() for d in enc_col_list if d['enc_location'].lower() == "db"]
    try:
        validateSchemaTableColumns(schema,table,col_name_list,db_conn_id,db_name,driver)
    except Exception as e:
        raise Exception(e)


    # Get a summary of enc_col_list to help processing
    try:
        enc_col_actions = get_enc_col_list_information(enc_col_list, "actions")
    except Exception as e:
        raise Exception(e)
    if enc_col_actions is None:
        enc_col_actions = {}

    # Get all columns from enc_col_list
    try:
        enc_col_details = get_enc_col_list_information(enc_col_list, "details")
    except Exception as e:
        raise Exception(e)
    if enc_col_details is None:
        enc_col_details = {}


    xref_stm = ""


    # Prepare the LOAD table with indexes for each column to substitute/encrypt
    for col in enc_col_details:

         # Set an index on each column which needs substitution/encryption in the DB
        if col['enc_location'].lower() == "db":
            col_name = col['colname']

            xref_stm = xref_stm + """
IF NOT EXISTS ( SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('{0}.{1}') AND name='{0}_{1}_{2}_I1' )
	BEGIN
        PRINT('Create indexes on load table for column: {2}');
        PRINT(CONVERT(varchar(24), GetDate(), 121));
        CREATE INDEX [{0}_{1}_{2}_I1] ON [{0}].[{1}] ([{2}]);
        PRINT(CONVERT(varchar(24), GetDate(), 121));
    END
            """.format(schema,table,col_name)

    # Set an index for the rowid column
    xref_stm = xref_stm + """

IF NOT EXISTS ( SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('{0}.{1}') AND name='PK_{0}_{1}_nrdav2_loading_rowid' )
	BEGIN
        PRINT('Create PK clustered index on load table for the sequential row id [nrdav2_loading_rowid]');
        PRINT(CONVERT(varchar(24), GetDate(), 121));
        ALTER TABLE [{0}].[{1}] ADD CONSTRAINT [PK_{0}_{1}_nrdav2_loading_rowid] PRIMARY KEY CLUSTERED ([nrdav2_loading_rowid] ASC);
        PRINT(CONVERT(varchar(24), GetDate(), 121));
    END

    """.format(schema,table)


    # Sort to process in logical order. Sort by: action (encrypt/substitute), location (central/decentralised), subTable (key/alf/DCLT04)
    enc_col_details = sorted(enc_col_details, key=lambda k: (k['action'].lower(), k['location'].lower(), k['subTable'].lower())) 


    # Create a temp table for each unique substitution table
    enc_tables = []
    sub_tables = []
    enc_tables_central = []
    sub_tables_central = []
    for col in enc_col_details:
        if col['enc_location'].lower() == "db": 
            if col['action'].lower() == "encrypt" and col['location'].lower() == "decentralised" and col['subTableName'].lower() not in enc_tables:
                tmpStr = create_user_defined_encryption_table(project_code, col['subTable'], col['case_sensitivity'])    # Create temp table to do ENCRYPT, contains ASYM column
                xref_stm = xref_stm + tmpStr
                enc_tables.append(col['subTableName'].lower())
            elif col['action'].lower() == "substitute" and col['location'].lower() == "decentralised" and col['subTableName'].lower() not in sub_tables:
                tmpStr = create_user_defined_substitution_table(project_code, col['subTable'], col['case_sensitivity'])  # Create temp table to do SUBSTITUTE
                xref_stm = xref_stm + tmpStr
                sub_tables.append(col['subTableName'].lower())
            elif col['action'].lower() == "substitute" and col['location'].lower() == "central" and col['subTableName'].lower() not in sub_tables_central:
                tmpStr = create_central_substitution_table(col['subTable'], col['case_sensitivity'])  # Create temp table to do SUBSTITUTE centrally
                xref_stm = xref_stm + tmpStr
                sub_tables_central.append(col['subTableName'].lower())
            elif col['action'].lower() == "encrypt" and col['location'].lower() == "central" and col['subTableName'].lower() not in enc_tables_central:
                tmpStr = create_central_encryption_table(col['subTable'], col['case_sensitivity'])    # Create temp table to do ENCRYPT centrally
                xref_stm = xref_stm + tmpStr
                enc_tables_central.append(col['subTableName'].lower())


    # Declarations
    xref_stm = xref_stm + """
DECLARE @li_RowsToEncrypt bigint = 0;
DECLARE @li_RowsRemaining bigint = 0;
DECLARE @ldt_RunStart datetime2;
DECLARE @li_LoopNum bigint = 0;
DECLARE @lv_Msg varchar(500);
DECLARE @ldt_BatchStart datetime2;
DECLARE @li_BatchStart bigint = 0;
DECLARE @li_BatchEnd bigint = 0;
DECLARE @li_BatchSize int = 500000;
DECLARE @lv_Salt varchar(255);
    """


    subTablesCreated = []

    for action in enc_col_actions:                              # encrypt or substitute

        # All substitution tables which will be used.  Append central and decentralised together. 
        subTablesForAction = {}
        # If a requirement for central and/or decentralised exists, hold a list of tables
        if 'central' in enc_col_actions[action]:
            subTablesForAction['central'] = enc_col_actions[action]['central']              # Substitution tables CENTRAL    
        if 'decentralised' in enc_col_actions[action]:
            subTablesForAction['decentralised'] = enc_col_actions[action]['decentralised']  # Substitution tables DECENTRALISED


        xref_stm = xref_stm + """
PRINT('--------------------- {0} ---------------------')
        """.format(action.upper())


        # Handle all substitution tables (central and decentralised) which need encryption
        location = ""
        for location in subTablesForAction:                    # central or decentralised
        
            xref_stm = xref_stm + """
PRINT('---------------- {0} ----------------')
            """.format(location.upper())


            # If centralised is included, only need one substitution table lookup
            if action.lower() == 'encrypt' and location.lower() == 'central':
                tmpStr = print_substitution_table_lookup('CENTRAL','CENTRAL')
                xref_stm = xref_stm + tmpStr



            subTable = ""
            for subTable in subTablesForAction[location]:      # key / alf / hcp 
                print(subTable)

                # Check substitution names cleansed
                try:
                    checkInvalidCharacters(subTable)
                except Exception as e:
                    raise ValueError("Error in run_xref_load() check substitution names cleansed", e)
                
                for subTableCase in subTablesForAction[location][subTable]:   # ci / cs /     # The case_sensitivity value is in the subTable name
                    print(subTableCase)


                    xref_stm = xref_stm + """
PRINT('----------- {0} -----------')
                    """.format((subTable + " | " + action + " | " + subTableCase).upper())


                    enc_col_list_types = subTablesForAction[location][subTable][subTableCase]
                    #print( "{0} | {1}".format(subTable,enc_col_list_types) )


                    # for each encryption type
                    for key in enc_col_list_types:                  # key15 / key30 / key50
                
                        encryption = ""
                        keyColSizeOverride = ""                    
                        
                        print('----------')


                        # Set the char(x) size for the encryption
                        print(key)
                        if key.lower() in ["key","key15","key30","key50","alf","ralf","hcp"]:
                            res = get_encryption_key_size(key)
                            encryption = res[0]
                            keyColSizeOverride = res[1]
                        else:
                            keyColSizeOverride = 256    # Use char(256)  
           

                        # Set the centralised/decentralised table name
                        if action.lower() == 'encrypt':
                            abbr = "enc"
                        elif action.lower() == 'substitute':
                            abbr = "sub"
                        else:
                            raise ValueError("Error in run_xref_load(), unknown action", action.lower())                        
                            
                        if location.lower() == "central":
                            #substitutionTable = subTable.upper()          # Set substitution table to default to KEY50/ALF/HCP/etc.  
                            substitutionTable = "{0}_{1}_{2}".format(subTable, abbr, subTableCase).upper()                 
                            substColPrefix = subTable.upper()
                        else:
                            substitutionTable = "{0}_{1}_{2}_{3}".format(project_code, subTable, abbr, subTableCase).upper()   # Override encryption variable to PROJECTNAME + SUBSTITUTION TABLE               
                            substColPrefix = "ID"
                        print("substitutionTable: {0}".format(substitutionTable))
                        print("substColPrefix: {0}".format(substColPrefix))


                        # Set the asymKeyID - dependent on the encryption type
                        if location.lower() == "central" and key.lower() in ["key","key15","key30","key50","alf","ralf","hcp"]:
                            asymKeyID = "NRDA2_A_KeyTest"
                        else:
                            asymKeyID = "NRDA2_A_{0}".format(substitutionTable)       # NRDA2_A_KeyTest
                        print("AsymKeyID: " + asymKeyID)

                        # Pull salt value from table to use in variable
                        if location.lower() == 'central':
                            projLookup = 'CENTRAL'
                            substLookup = 'CENTRAL'
                        else:
                            projLookup = project_code.upper()
                            substLookup = subTable.upper()


                        xref_stm = xref_stm + """        
PRINT('---- {3} ----')

PRINT('{0} | {1} | {2} | {3} | {4} | {5} | {6} | {7}') 

                        """.format(action, location, subTable, key.upper(), projLookup, substLookup, substitutionTable, subTableCase)


                        # If decentralised, add a substitution table lookup for each key
                        if location.lower() != 'central' and action.lower() == 'encrypt':           # If decentralised encryption
                            tmpStr = print_substitution_table_lookup(projLookup,substLookup)
                            xref_stm = xref_stm + tmpStr

                            # Set or confirm entry in substitution table
                            if action.lower() == 'encrypt':
                                try:
                                    subst_result = get_substitution_info(project_code.upper(),subTable.upper(),key.upper(),db_conn_id,db_name,driver,substitutionTable.upper()) # GETS or CREATES a substitution record
                                    print(subst_result)  
                                except Exception as e:
                                    raise Exception(e)
                                

                        if action.lower() == 'encrypt':

                            xref_stm = xref_stm + """ 
PRINT('Prepare empty encryption [#tmp{4}_{2}] table for {4} ({2})');
SELECT TOP 0 *
INTO [#tmp{4}_{2}]                                                          -- DROP TABLE [#tmp{4}_{2}]               
FROM ( select TOP 0 [{5}_E_ORI], CAST('' AS char({3})) AS [{5}_U] from basectrlt.[{4}_xref] ) qry;
                            """.format(encryption,substLookup,key,keyColSizeOverride,substitutionTable,substColPrefix)
                        
                        elif action.lower() == 'substitute' and subTable.lower() not in subTablesCreated:

                            subTablesCreated.append(subTable.lower())

                            xref_stm = xref_stm + """ 
PRINT('Prepare empty substitution [#tmp{1}] table');
SELECT TOP 0 *
INTO [#tmp{1}]                                          -- DROP TABLE [#tmp{1}]
FROM ( select TOP 0 [{0}_E_ORI] from basectrlt.[{1}_xref] ) qry;
                            """.format(substColPrefix,substitutionTable)




                        # Standardise the Key names to uppercase to match the lookup
                        enc_col_list_types_lower = {}
                        for col in enc_col_list_types[key]:
                            enc_col_list_types_lower[col['colname'].lower()] = col

                        # Get the columns to be encrypted with this encryption type
                        for colname, col_detail in enc_col_list_types_lower.items():    
                        
                            case_sensitivity = col_detail.get('case_sensitivity', 'ci').lower()
                            print(colname, case_sensitivity)

                            if case_sensitivity == "cs":
                                colname_case_adjusted = f"[{colname}]"
                                collation_for_case = " COLLATE LATIN1_GENERAL_CS_AS"
                            elif case_sensitivity == "ci":
                                colname_case_adjusted = f"LOWER([{colname}])"
                                collation_for_case = ""

                            xref_stm = xref_stm + """
PRINT('-- Column: {0} --')
                            """.format(colname.upper())

                            if action.lower() == 'encrypt':

                                xref_stm = xref_stm + """
PRINT('Add key values to encrypt - [{0}]');
INSERT INTO [#tmp{5}_{4}] ([{6}_U])
SELECT DISTINCT 	
	{7}{8}        -- Will be set with or without LOWER() to prevent duplication when "aaa" and "AAA" become different hashes
FROM [{1}].[{2}]
WHERE [{0}] <> ''
	or [{0}] is not null
ORDER BY 1;

UPDATE [#tmp{5}_{4}] SET [{6}_E_ORI] = HASHBYTES('SHA2_512', CONCAT(@lv_Salt,CAST([{6}_U] AS char({3}))));

                                """.format(colname,schema,table,keyColSizeOverride,key,substitutionTable,substColPrefix,colname_case_adjusted,collation_for_case)

                            elif action.lower() == 'substitute':
                            
                                xref_stm = xref_stm + """

SET @ldt_BatchStart = GetDate();
SET @lv_Msg = 'Begin insert to [#tmp{5}] for [{0}] - ' + CONVERT(varchar, @ldt_BatchStart, 120)
RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT

PRINT('Add key values to substitute - [{0}]');
INSERT INTO [#tmp{5}] ([{4}_E_ORI])
SELECT DISTINCT 
	{6}{7}
FROM [{1}].[{2}]
WHERE [{0}] <> ''
	or [{0}] is not null
ORDER BY 1;

SET @lv_Msg = 'End insert [{0}] to [#tmp{5}].[{4}_E_ORI] - Duration (s): ' + CAST(DateDiff(second, @ldt_BatchStart, GetDate()) AS varchar)
RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT

                                """.format(colname,schema,table,subTable.upper(),substColPrefix,substitutionTable,colname_case_adjusted,collation_for_case)


                        # Add the sql for temp tables and batch loops
                        # Last action in foreach KEY loop
                        if action.lower() == 'encrypt':
                            tmpStr = print_xref_for_encrypt(colname,schema,table,asymKeyID,encryption,subTable,keyColSizeOverride,key,substitutionTable,substColPrefix)
                            xref_stm = xref_stm + tmpStr

                
                # Last action in foreach subTable loop
                if action.lower() == 'substitute':
                    tmpStr = print_xref_for_substitute(subTable.upper(), substColPrefix, substitutionTable, colname)
                    xref_stm = xref_stm + tmpStr




# Add after the encryption loops
    xref_stm = xref_stm + """

PRINT('--------------------------------------------------------------------------------------------------')

PRINT '<<XREF_LOAD_SUCCESS>>';

RETURN

PRINT('--------------------------------------------------------------------------------------------------')

ExitLoops:

PRINT 'Runtime (s): ' + CAST(DateDiff(second, @ldt_RunStart, GetDate()) AS varchar);
PRINT('Exit and flag as failed');
PRINT '<<XREF_LOAD_FAILED>>';

    """



    print("sql statement file: ", sql_file_name)

#### remove later
    print(xref_stm)

    with open(sql_file_name, 'w') as text_file:
         text_file.write(xref_stm)
    #logging.info(xref_stm)
    logging.info("load_xref sql file is generated at: {0}".format(sql_file_name))
    #os.system(". /usr/local/airflow/sqllib/db2profile & db2 -tvmf {0} | tee {1}/log_{2}_xref_{3}.txt".format(sql_file_name,output_msg_dir,schema,table))
    #subprocess.call(['bash', '-c', '. /usr/local/airflow/sqllib/db2profile && db2 +c -tvmf {0} -z {1}/log_{2}_{3}.txt'.format(sql_file_name,output_msg_dir,schema,table)])

    sql_output = os.path.join(local_dir,"log_xref_output_{0}_{1}.txt".format(schema,table))

    print("sql output file: ", sql_output)

    #db_conn_id = ledger['attributes']['database_conn_id']
    #db_name = ledger['attributes']['database_name']

    mssql_conn = BaseHook.get_connection(db_conn_id)

    ## TODO ## Change the timeout value "-t" to something appropriate ## For testing, 1800 seconds = 30 mins, 10800 seconds = 3 hours, 43200 = 12h
    sql_stm = '/opt/mssql-tools/bin/sqlcmd -S {4} -U {0} -P {1} -d {5} -i {2} -o {3}  -t 43200 -m-1 -p 1 -X 1'.format(mssql_conn.login,mssql_conn.password,sql_file_name,sql_output,mssql_conn.host,db_name)

    # retval = subprocess.run(["cmd", "/c", sql_stm], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    
    print("Starting xref load...")
    start = time.time()

    retval = subprocess.Popen(['bash', '-c', sql_stm]) #, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    retval.wait()
    # (stdout, stderr) = retval.communicate()

    print("TimeTaken: ", time.time() - start)

    # print("stdout:")
    # print(stdout)

    success_msg = "<<XREF_LOAD_SUCCESS>>"

    with open(sql_output, 'r') as output:
        output_str = output.read()
        print(output_str)
        if success_msg not in output_str:
            print("fail")
            raise ValueError("SQL failed")

    if retval.returncode != 0:
        print("error")
        # print(stderr)
    else:
        print("success")



class Default(dict):
    def __missing__(self, key):
        return key
