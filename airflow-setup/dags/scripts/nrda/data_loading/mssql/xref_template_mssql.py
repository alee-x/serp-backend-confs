
def create_user_defined_substitution_table(project_code,subst,case_sensitivity):

    # Check if user defined substitution table already exists: basectrlt.PROJCODE_SUBSTVAL_subst_xref
    # Else create table and sequence
    action = "sub"

    if case_sensitivity.lower() == "cs":
        collation_for_case = " COLLATE LATIN1_GENERAL_CS_AS"
    else:
        collation_for_case = ""

    xref_stm = """

PRINT('--------------------------------------------------------------------------------------------------')

-- Prepare user defined substitution XREF table/sequence [{0}_{1}_{2}_{3}_xref] 

IF OBJECT_ID('basectrlt.[{0}_{1}_{2}_{3}_xref]', 'U') IS NULL
	BEGIN
        PRINT('CREATE TABLE basectrlt.[{0}_{1}_{2}_{3}_xref]'); 
		CREATE TABLE basectrlt.[{0}_{1}_{2}_{3}_xref] (			                    -- DROP TABLE basectrlt.[{0}_{1}_{2}_{3}_xref]
			[ID_E] BIGINT NOT NULL,
			[ID_E_ORI] VARCHAR(256){4},
			AVAIL_FROM_DATE DATETIME2,	-- Date inserted
			CONSTRAINT [PK_{0}_{1}_{2}_{3}_xref_ID_E] PRIMARY KEY CLUSTERED ([ID_E])
		);
		CREATE UNIQUE NONCLUSTERED INDEX [IXU_{0}_{1}_{2}_{3}_XREF_ID_E_ORI] ON basectrlt.[{0}_{1}_{2}_{3}_xref] ( [ID_E_ORI] );
	END

IF NOT EXISTS (SELECT * FROM sys.sequences WHERE name = '{0}_{1}_{2}_{3}_seq')
	BEGIN
        PRINT('CREATE SEQUENCE basectrls.[{0}_{1}_{2}_{3}_seq]'); 
        CREATE SEQUENCE basectrls.[{0}_{1}_{2}_{3}_seq] AS BIGINT START WITH 1 INCREMENT BY 1;      -- DROP SEQUENCE basectrls.[{0}_{1}_{2}_{3}_seq]
	END

-- ENDS Prepare XREF table/sequence for [{0}_{1}_{2}_{3}_xref] substitution

PRINT('--------------------------------------------------------------------------------------------------')

    """.format(project_code.upper(),subst.upper(),action.upper(),case_sensitivity.upper(),collation_for_case.upper())

    return xref_stm






def create_user_defined_encryption_table(project_code,subst,case_sensitivity):

    # Check if user defined encryption table already exists: basectrlt.PROJCODE_SUBSTVAL_subst_xref
    # Else create table and sequence
    action = "enc"
    xref_stm = """

PRINT('--------------------------------------------------------------------------------------------------')

-- Prepare user defined encryption XREF table/sequence [{0}_{1}_{2}_{3}_xref] 

IF OBJECT_ID('basectrlt.[{0}_{1}_{2}_{3}_xref]', 'U') IS NULL
	BEGIN
        PRINT('CREATE TABLE basectrlt.[{0}_{1}_{2}_{3}_xref]'); 
		CREATE TABLE basectrlt.[{0}_{1}_{2}_{3}_xref] (			                    -- DROP TABLE basectrlt.[{0}_{1}_{2}_{3}_xref]
			[ID_E] BIGINT NOT NULL,
			[ID_E_ORI] VARBINARY(64),
			[ID_E_ASYM] VARBINARY(8000),
			AVAIL_FROM_DATE DATETIME2,	-- Date inserted
			CONSTRAINT [PK_{0}_{1}_{2}_{3}_xref_ID_E] PRIMARY KEY CLUSTERED ([ID_E])
		);
		CREATE UNIQUE NONCLUSTERED INDEX [IXU_{0}_{1}_{2}_{3}_XREF_ID_E_ORI] ON basectrlt.[{0}_{1}_{2}_{3}_xref] ( [ID_E_ORI] );
	END

IF NOT EXISTS (SELECT * FROM sys.sequences WHERE name = '{0}_{1}_{2}_{3}_seq')
	BEGIN
        PRINT('CREATE SEQUENCE basectrls.[{0}_{1}_{2}_{3}_seq]'); 
        CREATE SEQUENCE basectrls.[{0}_{1}_{2}_{3}_seq] AS BIGINT START WITH 1 INCREMENT BY 1;      -- DROP SEQUENCE basectrls.[{0}_{1}_{2}_{3}_seq]
	END

-- ENDS Prepare XREF table/sequence for [{0}_{1}_{2}_{3}_xref] encryption

PRINT('--------------------------------------------------------------------------------------------------')

    """.format(project_code.upper(),subst.upper(),action.upper(),case_sensitivity.upper())

    return xref_stm




def create_central_substitution_table(subst,case_sensitivity):

    # Check if central substitution table already exists: basectrlt.HCP_SUB_CS_xref
    # Else create table and sequence
    action = "sub"

    if case_sensitivity.lower() == "cs":
        collation_for_case = " COLLATE LATIN1_GENERAL_CS_AS"
    else:
        collation_for_case = ""

    xref_stm = """

PRINT('--------------------------------------------------------------------------------------------------')

-- Prepare central substitution XREF table/sequence [{0}_{1}_{2}_xref] 

IF OBJECT_ID('basectrlt.[{0}_{1}_{2}_xref]', 'U') IS NULL
	BEGIN
        PRINT('CREATE TABLE basectrlt.[{0}_{1}_{2}_xref]'); 
		CREATE TABLE basectrlt.[{0}_{1}_{2}_xref] (			                    -- DROP TABLE basectrlt.[{0}_{1}_{2}_xref]
			[{0}_E] BIGINT NOT NULL,
			[{0}_E_ORI] VARCHAR(256){3},
			AVAIL_FROM_DATE DATETIME2,	-- Date inserted
			CONSTRAINT [PK_{0}_{1}_{2}_xref_{0}_E] PRIMARY KEY CLUSTERED ([{0}_E])
		);
		CREATE UNIQUE NONCLUSTERED INDEX [IXU_{0}_{1}_{2}_XREF_ID_E_ORI] ON basectrlt.[{0}_{1}_{2}_xref] ( [{0}_E_ORI] );
	END

IF NOT EXISTS (SELECT * FROM sys.sequences WHERE name = '{0}_{1}_{2}_seq')
	BEGIN
        PRINT('CREATE SEQUENCE basectrls.[{0}_{1}_{2}_seq]'); 
        CREATE SEQUENCE basectrls.[{0}_{1}_{2}_seq] AS BIGINT START WITH 1 INCREMENT BY 1;      -- DROP SEQUENCE basectrls.[{0}_{1}_{2}_seq]
	END

-- ENDS Prepare XREF table/sequence for [{0}_{1}_{2}_xref] substitution

PRINT('--------------------------------------------------------------------------------------------------')

    """.format(subst.upper(),action.upper(),case_sensitivity.upper(),collation_for_case.upper())

    return xref_stm




def create_central_encryption_table(subst,case_sensitivity):

    # Check if central substitution table already exists: basectrlt.HCP_SUB_CS_xref
    # Else create table and sequence
    action = "enc"
    xref_stm = """

PRINT('--------------------------------------------------------------------------------------------------')

-- Prepare central encryption XREF table/sequence [{0}_{1}_{2}_xref] 

IF OBJECT_ID('basectrlt.[{0}_{1}_{2}_xref]', 'U') IS NULL
	BEGIN
        PRINT('CREATE TABLE basectrlt.[{0}_{1}_{2}_xref]'); 
		CREATE TABLE basectrlt.[{0}_{1}_{2}_xref] (			                    -- DROP TABLE basectrlt.[{0}_{1}_{2}_xref]
			[{0}_E] BIGINT NOT NULL,
			[{0}_E_ORI] VARBINARY(64),
			[{0}_E_ASYM] VARBINARY(8000),
			AVAIL_FROM_DATE DATETIME2,	-- Date inserted
			CONSTRAINT [PK_{0}_{1}_{2}_xref_{0}_E] PRIMARY KEY CLUSTERED ([{0}_E])
		);
		CREATE UNIQUE NONCLUSTERED INDEX [IXU_{0}_{1}_{2}_XREF_ID_E_ORI] ON basectrlt.[{0}_{1}_{2}_xref] ( [{0}_E_ORI] );
	END

IF NOT EXISTS (SELECT * FROM sys.sequences WHERE name = '{0}_{1}_{2}_seq')
	BEGIN
        PRINT('CREATE SEQUENCE basectrls.[{0}_{1}_{2}_seq]'); 
        CREATE SEQUENCE basectrls.[{0}_{1}_{2}_seq] AS BIGINT START WITH 1 INCREMENT BY 1;      -- DROP SEQUENCE basectrls.[{0}_{1}_{2}_seq]
	END

-- ENDS Prepare XREF table/sequence for [{0}_{1}_{2}_xref] encryption

PRINT('--------------------------------------------------------------------------------------------------')

    """.format(subst.upper(),action.upper(),case_sensitivity.upper())

    return xref_stm




def print_substitution_table_lookup(projLookup,substLookup):

    xref_stm = """  
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
PRINT('{0} {1} - SALT:' + @lv_Salt) 
    """.format(projLookup,substLookup)

    return xref_stm




def print_xref_for_encrypt(colname,schema,table,asymKeyID,encryption,subTable,keyColSizeOverride,key,substitutionTable,substColPrefix):

    xref_stm = """
PRINT('Add index to [#tmp{8}_{7}] for join with [{8}_xref]');
CREATE INDEX [ix_tmp{8}_{7}_e_ori] ON [#tmp{8}_{7}] ([{9}_E_ORI]);

PRINT('Prepare empty #tmp{8}_{7}a table for new + asym values');
SELECT TOP 0 IDENTITY(INT,1,1) AS ID, *
INTO [#tmp{8}_{7}_a]                                        -- DROP TABLE [#tmp{8}_{7}_a]
FROM ( select TOP 1 {9}_E_ORI, CAST('' AS char({6})) AS {9}_U, {9}_E_ASYM from basectrlt.[{8}_xref] ) qry;


PRINT('Add new key values to #tmp{8}_{7}_a');
INSERT INTO [#tmp{8}_{7}_a] ({9}_E_ORI, {9}_U)
SELECT DISTINCT a.[{9}_E_ORI], a.[{9}_U]
FROM [#tmp{8}_{7}] a
WHERE NOT EXISTS (	SELECT b.[{9}_e_ori]
					FROM basectrlt.[{8}_xref] b
					WHERE a.[{9}_e_ori] = b.[{9}_e_ori]
					);


PRINT('Create clustered index on [#tmp{8}_{7}_a] to speed up batching');
CREATE CLUSTERED INDEX [ix_tmp{8}_{7}_a_id] ON [#tmp{8}_{7}_a] ([ID]);

PRINT('Drop table [#tmp{8}_{7}] as no longer needed');
DROP TABLE [#tmp{8}_{7}];               -- SELECT * FROM [#tmp{8}_{7}];



PRINT('Add asym values to [#tmp{8}_{7}_a]');

SET @li_RowsToEncrypt = ( SELECT MAX(ID) FROM [#tmp{8}_{7}_a] );
SET @li_RowsRemaining = @li_RowsToEncrypt;
SET @ldt_RunStart = GetDate();
SET @li_BatchStart = 0;


WHILE @li_RowsRemaining > 0
	BEGIN
			
		SET @li_LoopNum = @li_LoopNum + 1
        SET @ldt_BatchStart = GetDate();
		SET @lv_Msg = 'Loop: ' + CAST(@li_LoopNum AS char(3)) + ', Rows remaining: ' + CAST(@li_RowsRemaining AS varchar) + '/' + CAST(@li_RowsToEncrypt AS varchar) + ', TimeStart: ' + CONVERT(varchar, GetDate(), 108)
		RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT
		
		SET @li_BatchEnd = @li_BatchStart + @li_BatchSize
		SET @lv_Msg = 'Encrypt batch where ID > ' + CAST(@li_BatchStart AS varchar) + ' AND ID <= ' +  CAST(@li_BatchEnd AS varchar)
		RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT
		
        IF NOT EXISTS ( SELECT * FROM sys.asymmetric_keys WHERE [name] = '{3}' )
            BEGIN
                RAISERROR ('ERROR - ASYMKEY_ID not found: [{3}]', 16, 1);
                RETURN
            END

		BEGIN TRANSACTION;

        -- SELECT * FROM #tmp{8}_{7}_a
        UPDATE [#tmp{8}_{7}_a] SET [{9}_E_ASYM] = EncryptByAsymKey(AsymKey_ID('{3}'), CAST({9}_U AS char({6}))) WHERE ID > @li_BatchStart AND ID <= @li_BatchEnd;

		IF @@ERROR <> 0
			BEGIN 
				RAISERROR ('ROLLBACK ON UPDATE', 10, 1) WITH NOWAIT
				ROLLBACK TRANSACTION 
				GOTO ExitLoops;
			END
		        
        INSERT INTO basectrlt.[{8}_xref] ([{9}_E], [{9}_E_ORI], [{9}_E_ASYM], AVAIL_FROM_DATE)
		SELECT
			next value for basectrls.{8}_seq
			, a.{9}_E_ORI
			, a.{9}_E_ASYM
			, current_timestamp
		FROM [#tmp{8}_{7}_a] a
		WHERE ID > @li_BatchStart AND ID <= @li_BatchEnd;

		IF @@ERROR <> 0
			BEGIN 
				RAISERROR ('ROLLBACK ON INSERT', 10, 1) WITH NOWAIT
				ROLLBACK TRANSACTION 
				GOTO ExitLoops;
			END
		ELSE
            COMMIT TRANSACTION;

		SET @li_RowsRemaining = @li_RowsToEncrypt - @li_BatchEnd
		SET @li_BatchStart = @li_BatchEnd
		SET @li_BatchEnd = 0
		SET @lv_Msg = 'Batch time (s): ' + CAST(DateDiff(second, @ldt_BatchStart, GetDate()) AS varchar)
		RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT
		PRINT('')
	END

PRINT 'Runtime (s): ' + CAST(DateDiff(second, @ldt_RunStart, GetDate()) AS varchar);
PRINT('Drop table [#tmp{8}_{7}_a] as no longer needed');
DROP TABLE [#tmp{8}_{7}_a];             -- SELECT * FROM [#tmp{8}_{7}_a];


    """.format(colname,schema,table,asymKeyID,encryption,subTable.upper(),keyColSizeOverride,key,substitutionTable,substColPrefix)

    return xref_stm




def print_xref_for_substitute(subTable, substColPrefix, substitutionTable, colname):

    xref_stm = """

SET @lv_Msg = 'Add index to [#tmp{2}] for join with {2}_xref - ' + CONVERT(varchar, GetDate(), 120)
RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT
PRINT('Add index to [#tmp{2}] for join with {2}_xref');
CREATE INDEX [ix_tmp{2}_{1}_e_ori] ON [#tmp{2}] ([{1}_E_ORI]);

SET @lv_Msg = 'Prepare empty #tmp{2}_a table for new values - ' + CONVERT(varchar, GetDate(), 120)
RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT
PRINT('Prepare empty #tmp{2}_a table for new values');
SELECT TOP 0 IDENTITY(BIGINT,1,1) AS ID, *
INTO [#tmp{2}_a]
FROM ( select TOP 1 [{1}_E_ORI] from basectrlt.[{2}_xref] ) qry;

SET @ldt_BatchStart = GetDate();
SET @lv_Msg = 'Add new key values to #tmp{2}_a - ' + CONVERT(varchar, @ldt_BatchStart, 120)
RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT
PRINT('Add new key values to #tmp{2}_a - ' + CONVERT(varchar, @ldt_BatchStart, 120))

INSERT INTO [#tmp{2}_a] ([{1}_E_ORI])
SELECT DISTINCT a.[{1}_E_ORI]       -- SELECT TOP 1000 *
FROM [#tmp{2}] a
WHERE NOT EXISTS (	SELECT b.[{1}_e_ori]
					FROM basectrlt.[{2}_xref] b
					WHERE a.[{1}_e_ori] = b.[{1}_e_ori]
					);

SET @lv_Msg = 'End insert to [#tmp{2}_a] - Duration (s): ' + CAST(DateDiff(second, @ldt_BatchStart, GetDate()) AS varchar)
RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT


SET @ldt_BatchStart = GetDate();
SET @lv_Msg = 'Create clustered index on #tmp{2}_a to speed up batching - ' + CONVERT(varchar, @ldt_BatchStart, 120)
RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT
PRINT('Create clustered index on #tmp{2}_a to speed up batching - ' + CONVERT(varchar, @ldt_BatchStart, 120));

CREATE CLUSTERED INDEX [ix_tmp{2}_a_ID] ON [#tmp{2}_a] ([ID]);

SET @lv_Msg = 'End Create clustered index on #tmp{2}_a - Duration (s): ' + CAST(DateDiff(second, @ldt_BatchStart, GetDate()) AS varchar)
RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT

SET @lv_Msg = 'Drop table #tmp{2} as no longer needed'
RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT
PRINT('Drop table #tmp{2} as no longer needed');
DROP TABLE [#tmp{2}];               -- SELECT * FROM [#tmp{2}];



PRINT('Add values to basectrlt.[{2}_xref]');

SET @li_RowsToEncrypt = ( SELECT MAX(ID) FROM [#tmp{2}_a] );
SET @li_RowsRemaining = @li_RowsToEncrypt;
SET @ldt_RunStart = GetDate();
SET @li_BatchStart = 0;

WHILE @li_RowsRemaining > 0
	BEGIN
			
		SET @li_LoopNum = @li_LoopNum + 1
        SET @ldt_BatchStart = GetDate();
		SET @lv_Msg = 'Loop: ' + CAST(@li_LoopNum AS char(3)) + ', Rows remaining: ' + CAST(@li_RowsRemaining AS varchar) + '/' + CAST(@li_RowsToEncrypt AS varchar) + ', TimeStart: ' + CONVERT(varchar, GetDate(), 108)
		RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT
		
		SET @li_BatchEnd = @li_BatchStart + @li_BatchSize
		SET @lv_Msg = 'Encrypt batch where ID > ' + CAST(@li_BatchStart AS varchar) + ' AND ID <= ' +  CAST(@li_BatchEnd AS varchar)
		RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT		       
		        
        BEGIN TRANSACTION;

        INSERT INTO basectrlt.[{2}_xref] ([{1}_E], [{1}_E_ORI], AVAIL_FROM_DATE)
		SELECT
			next value for basectrls.[{2}_seq]
			, a.[{1}_E_ORI]
			, current_timestamp
		FROM [#tmp{2}_a] a
		WHERE ID > @li_BatchStart AND ID <= @li_BatchEnd;

		IF @@ERROR <> 0
			BEGIN 
				RAISERROR ('ROLLBACK ON INSERT', 10, 1) WITH NOWAIT
				ROLLBACK TRANSACTION 
				GOTO ExitLoops;
			END
		ELSE
            COMMIT TRANSACTION;

		SET @li_RowsRemaining = @li_RowsToEncrypt - @li_BatchEnd
		SET @li_BatchStart = @li_BatchEnd
		SET @li_BatchEnd = 0
		SET @lv_Msg = 'Batch time (s): ' + CAST(DateDiff(second, @ldt_BatchStart, GetDate()) AS varchar)
		RAISERROR (@lv_Msg, 10, 1) WITH NOWAIT
		PRINT('')
	END

PRINT 'Runtime (s): ' + CAST(DateDiff(second, @ldt_RunStart, GetDate()) AS varchar);
PRINT('Drop table #tmp{0}_a as no longer needed');
DROP TABLE [#tmp{2}_a];             -- SELECT * FROM [#tmp{2}_a];

    """.format(subTable.upper(), substColPrefix, substitutionTable, colname)

    return xref_stm