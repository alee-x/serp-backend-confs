from airflow.hooks.base_hook import BaseHook

import sys
import os
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(os.path.dirname(currentdir))

sys.path.append(parentdir)
from helpers import get_encryption_key_size
from helpers import create_password

def validateSchemaTableColumns(schema,table,col_name_list,db_conn_id,db_name,driver):
    
    # Check if schema name, tablename and column names in col_name_list are valid by comparing them to the database

    import pyodbc
    print(db_conn_id)
    cn = BaseHook.get_connection(db_conn_id)            
    conn = pyodbc.connect('DRIVER={0};server={1},{2};database={3};uid={4};pwd={5}'.format( driver, cn.host, cn.port, db_name, cn.login, cn.password ) )
    cursor = conn.cursor()


    # check schema exists

    sql = '''\
SELECT * FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?
    '''
    params = (schema)
    
    try:
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        #print("Rows for schema: " + str(len(rows)))
        if len(rows) == 1 and rows[0].SCHEMA_NAME == schema:
            print("schema verified: {0}".format(schema))
        else:
            raise Exception("Incorrect schema info from mssql: {0}".format(schema)) 
    except Exception as e:
        conn.close()
        print(e)
        raise Exception("Error verifying table info from mssql", "validateSchemaTableColumns", e) 


    # check table exists

    sql = '''\
SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    '''
    params = (schema,table)
    
    try:
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        #print("Rows for table: " + str(len(rows)))  
        if len(rows) == 1 and rows[0].TABLE_NAME == table:
            print("table verified: {0}".format(table))
        else:
            raise Exception("Incorrect table info from mssql: {0}".format(table)) 
    except Exception as e:
        conn.close()
        print(e)
        raise Exception("Error verifying table info from mssql", "validateSchemaTableColumns", e) 

    
    # check columns exist
    columnsFromDB = []

    sql = '''\
SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    '''
    params = (schema,table)
    
    try:
        cursor.execute(sql, params)        
        rows = cursor.fetchall()
        #print("Rows for columns: " + str(len(rows)))
        #if rows:  
        #    print(rows)
        for row in rows:
            columnsFromDB.append(row[0].lower())
    except Exception as e:
        conn.close()
        print(e)
        raise Exception("Error verifying table info from mssql", "validateSchemaTableColumns", e) 

    conn.close()
    

    # Check column names from DS match the DB
    columnsInDB = []
    columnsNotInDB = []
    for col_name in col_name_list:

        if col_name.lower() in columnsFromDB:
            columnsInDB.append(col_name)
            print("Column in DB: {0}".format(col_name))            
        else:
            columnsNotInDB.append(col_name)
            print("Column NOT in DB: {0}".format(col_name))

    if len(columnsNotInDB) > 0:
        raise Exception("Columns in enc_col_list but not in DB: [{0}]".format(",".join(columnsNotInDB)), "validateSchemaTableColumns")             
    elif len(columnsInDB) == len(col_name_list):
        return True
    else:
        raise Exception("Unexpected result", "validateSchemaTableColumns")




def get_enc_col_list_information(enc_col_list, result_set=""):

    enc_col_list_types = {}
    enc_col_list_details = []

    for col in enc_col_list:
        
        enc_location = col['enc_location']
        if enc_location.lower() != "db":
            continue                # Ignore current column and continue to next

        enced_col_key = col['enc_type']
        enced_col_key = enced_col_key.strip().lower()   # Stops multiples if different case

        # Use replacement name if column needs to be renamed
        if col['enced_col_name'] is None or col['enced_col_name'] is "":
            colname = col['col_name']
        else:
            colname = col['enced_col_name']

        # Check column names are cleansed
        try:
            checkInvalidCharacters(colname)
        except Exception as e:
            raise ValueError("Error in get_enc_col_list_information() for colname", e)


        # Get action details from enced_col_key
        action = ""
        location = ""
        subTable = ""
        key = ""
        case_sensitivity = ""
        encryption = ""
        keyColSizeOverride = ""
        substColPrefix = ""
        subTableName = ""        

        try:
            res = get_action_details(enced_col_key)
            action = res[0]
            location = res[1]
            subTable = res[2]
            key = res[3]
            case_sensitivity = res[4]
            encryption = res[5]
            keyColSizeOverride = res[6]
            substColPrefix = res[7]
            subTableName = res[8]
        except Exception as e:
            raise ValueError("Error in get_actions_from_enc_col_list() for enced_col_key", e)

        print("[{0}] | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8} | {9} | [{10}]".format(enced_col_key.upper(), action, location, subTable, key, case_sensitivity, encryption, keyColSizeOverride, substColPrefix, enc_location, subTableName))


        # Add column info to "details" list in case details are being returned instead of "actions"
        col_detail = {'colname': colname, 'subTable': subTable, 'action': action, 'location': location, 'key': key, 'enced_col_key': enced_col_key, 'enc_location': enc_location, 'encryption': encryption, 'keyColSizeOverride': keyColSizeOverride, 'substColPrefix': substColPrefix, 'case_sensitivity': case_sensitivity, 'subTableName': subTableName}
        enc_col_list_details.append(col_detail)


        # Prepare new dict
        if action not in enc_col_list_types:
            enc_col_list_types[action] = {}
        
        if location not in enc_col_list_types[action]:
            enc_col_list_types[action][location] = {}
        
        if subTable not in enc_col_list_types[action][location]:
            enc_col_list_types[action][location][subTable] = {}

        if case_sensitivity not in enc_col_list_types[action][location][subTable]:
            enc_col_list_types[action][location][subTable][case_sensitivity] = {} 

        if key not in enc_col_list_types[action][location][subTable][case_sensitivity]:
            enc_col_list_types[action][location][subTable][case_sensitivity][key] = []

        if colname not in enc_col_list_types[action][location][subTable][case_sensitivity][key]:
            enc_col_list_types[action][location][subTable][case_sensitivity][key].append(col_detail)

        # Validate that "Central" for substitution table can contain key, alf ralf, etc.  
        # Other substitution tables can only contain ONE encryption type, and it must match what has already been used in the DB
        if action.lower() == "encrypt" and location.lower() != "central":
            if len(enc_col_list_types[action][location][subTable][case_sensitivity]) > 1:
                    print("ERROR - can't have more than one key type for a named substitution table [{0}] ".format(sub))


    # return column details or actions summary
    if result_set == "details":
        return enc_col_list_details

    elif result_set == "actions":
        return enc_col_list_types

    else:
        raise ValueError("ERROR: Invalid result_set value: [{0}]".format(result_set.upper()))

# End get_enc_col_list_information




def checkInvalidCharacters(str):
   
    ## Only allow a-Z 0-9 underscore and single dash ??
    import re
    matches = re.search('[^\w\.]', str)     # Match a single character not present in the list.  \w is equal to [a-zA-Z0-9_], and additional space

    if matches:
        raise ValueError("Disallowed character found in string: [{0}].  Allowable characters: a-Z0-9_".format(str))

    return False




def get_action_details(enced_col_key):

    # Check substitution names are cleansed
    try:
        checkInvalidCharacters(enced_col_key)
    except Exception as e:
        raise ValueError("Error in get_actions_from_enc_col_list() for substitution", e)

    action = ""
    location = ""
    subTable = ""
    key = ""
    case_sensitivity = ""

    # The enced_col_key structure is 3 parts, (ENC/SUB)(Key type)(Case Sensitive/Insensitive) e.g. ENC.ALF.CS, ENC.HCP.CS, ENC.KEY.CS
    # May be 1 part: KEY50
    # May be 2 parts: ENC.KEY50

    keylist = enced_col_key.lower().split(".")    
    print(keylist)

    if len(keylist)==1:     # Must be one of ["key","key15","key30","key50","alf","ralf","hcp"]

        if keylist[0] in ["key","key15","key30","key50","alf","ralf","hcp"]:            # encrypt centralised for backwards compatibility
            action = "encrypt"
            location = "central"
            key = keylist[0]
            case_sensitivity = "cs"
        else:
            raise ValueError("ERROR: Invalid keylist: [{0}]".format(enced_col_key.upper()))

    elif len(keylist)==2:   # Only allow if first two positions (enc/sub) and (key)

        if keylist[0] in ["enc","sub"]:

            if keylist[0] == "enc":

                if keylist[1] in ["key","key15","key30","key50","alf","ralf","hcp"]:        # Encrypt centralised
                    action = "encrypt"
                    location = "central"
                    key = keylist[1]
                    case_sensitivity = "cs"
                else:                                                                       # Encrypt decentralised
                    action = "encrypt"
                    location = "decentralised"
                    key = "RSA_4096"
                    subTable = keylist[1]
                    case_sensitivity = "ci"

            elif keylist[0] == "sub":

                key = keylist[1]
                case_sensitivity = "ci"

                if keylist[1] in ["key","key15","key30","key50","alf","ralf","hcp"]:         # Substitute centralised
                    action = "substitute"
                    location = "central"
                    key = keylist[1]
                    case_sensitivity = "ci"
                else:
                    action = "substitute"                                                    # Substitute decentralised
                    location = "decentralised"
                    subTable = keylist[1]
                    key = keylist[1]
                    case_sensitivity = "ci"

            else:
                raise ValueError("ERROR: Invalid keylist: [{0}]".format(enced_col_key.upper()))            

        else:
            raise ValueError("ERROR: Invalid keylist: [{0}]".format(enced_col_key.upper()))
        
    elif len(keylist)==3:

        if keylist[0] == "enc":
            action = "encrypt"
        elif keylist[0] == "sub":
            action = "substitute"
        else:
            raise ValueError("ERROR: Invalid action value: [{0}]".format(keylist[0].upper()))

        if keylist[1] in ["key","key15","key30","key50","alf","ralf","hcp"]:
            location = "central"
            key = keylist[1]
        else: 
            location = "decentralised"
            key = keylist[1]
            subTable = keylist[1]

        if keylist[2] == "cs":
            case_sensitivity = "cs"
        elif keylist[2] == "ci":
            case_sensitivity = "ci"
        else:
            raise ValueError("ERROR: Invalid case_sensitivity value: [{0}]".format(keylist[2].upper()))

    else:
        raise ValueError("ERROR: Invalid keylist: [{0}]".format(enced_col_key.upper()))

   
    
    if key.lower() in ["key","key15","key30","key50","alf","ralf","hcp"]: 
        if key.lower()[0:3] == "key":
            subTable = "key"
        else:
            subTable = key.lower()


    if subTable is None or subTable == "":
        raise Exception("ERROR: Subtable must contain a value")



    encryption = ""
    keyColSizeOverride = ""
            
    if key.lower() in ["key","key15","key30","key50","alf","ralf","hcp"]:
        res = get_encryption_key_size(key)
        encryption = res[0]
        keyColSizeOverride = res[1]
    else:
        keyColSizeOverride = 256    # Use char(256) 
        encryption = key


    # Set the substitution/encryption column prefix

    substColPrefix = ""

    if location.lower() == "central":
        substColPrefix = subTable.upper()
    else:
        substColPrefix = "ID"

    
    # Set the table name to use for the subTable
    subTableName = ""
    if action == "encrypt":
        subTableName = subTable + "_enc_" + case_sensitivity
    elif action == "substitute":
        subTableName = subTable + "_sub_" + case_sensitivity
    

    return action, location, subTable, key, case_sensitivity, encryption, keyColSizeOverride, substColPrefix, subTableName



def get_substitution_info(project_code,subst,enc_type,db_conn_id,db_name,driver,substitutionTable):
    
    try:
        import pyodbc
        print(db_conn_id)
        cn = BaseHook.get_connection(db_conn_id)            
        conn = pyodbc.connect('DRIVER={0};server={1},{2};database={3};uid={4};pwd={5}'.format( driver, cn.host, cn.port, db_name, cn.login, cn.password ) )
        #print(conn)
        cursor = conn.cursor()
    except Exception as e:
        raise ValueError("Error in get_substitution_info()", "CONNECT", e)
    
    # Check if entry exists for this PROJECT_CODE + SUBSTITUTION_TABLE
    try:
        strSelect = "SELECT * FROM basectrlt.SUBSTITUTION WHERE PROJECT_CODE = ? AND SUBSTITUTION_TABLE = ?;"
        cursor.execute(strSelect, project_code, substitutionTable)    
        row = cursor.fetchone()         # fetchall()
        #print(row)
    except Exception as e:
        conn.close()
        raise ValueError("Error in get_substitution_info()", "SELECT", e)
   
    
    if row:
        subst_record_already_existed = True      # Record found
        return subst_record_already_existed, row
    else:
        subst_record_already_existed = False     # Record not found. Insert one.
        salt = create_password(15)
        enc_key = create_password(30)                
        try:
            strInsert = "INSERT INTO basectrlt.SUBSTITUTION (PROJECT_CODE, SUBSTITUTION_TABLE, ENC_TYPE, SALT, ENCRYPTION_KEY, DATE_CREATED, DATE_UPDATED) VALUES (?,?,?,?,?,GETDATE(),GETDATE());"       
            cursor.execute(strInsert, project_code, substitutionTable, enc_type, salt, enc_key)
            record_id = cursor.execute('SELECT @@IDENTITY AS id;').fetchone()[0]
            print(record_id)
        except Exception as e:
            conn.rollback()
            conn.close()
            raise ValueError("Error in get_substitution_info()", "INSERT", e)

    if record_id > 0:
        try:
            row = cursor.execute('SELECT * FROM basectrlt.SUBSTITUTION WHERE ROWID = ?', record_id).fetchone()
            if row == None:
                raise ValueError("Failed to retrieve substitution record by ID", record_id)
        except Exception as e:
            conn.rollback()
            conn.close()
            raise ValueError("Error in get_substitution_info()", "VERIFY")

    
    # When the subst record is created, create a corresponding key unless it's one of the central types
    
    if subst.lower() != "central":             #if enc_type.lower() not in ["key","key30","key50","alf","ralf","hcp"]:

        if enc_type.lower() in ["key","key15","key30","key50","alf","ralf","hcp"]:      # If not central, but a central (key/alf/etc.) type has been set, create a new certificate
            enc_type = 'RSA_4096'

        strCreateKey = """
    IF NOT EXISTS ( SELECT * FROM sys.asymmetric_keys WHERE name = 'NRDA2_A_{4}' )
        BEGIN
            CREATE ASYMMETRIC KEY [NRDA2_A_{4}]
            WITH ALGORITHM = {2}
            ENCRYPTION BY PASSWORD = '{3}';
        END
        """.format(project_code,subst,enc_type,enc_key,substitutionTable)

        #print("Key:  NRDA2_A_{0}_{1}     Algorithm: {2}     Pwd: {3}".format(project_code,subst,enc_type,enc_key))

        try:
            cursor.execute(strCreateKey)
        except Exception as e:
                conn.rollback()
                conn.close()
                raise ValueError("Error in get_substitution_info()", "CREATE KEY", e)             


    print("commit")
    conn.commit()
    conn.close()
    
    return subst_record_already_existed, row
