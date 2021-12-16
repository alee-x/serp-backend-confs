import os 
import subprocess
import logging
from datetime import datetime

def run_sql_postgres(pg_server, pg_port, pg_database, pg_login, pg_password, error_msg_dir, output_msg_dir, **context):

    ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']
    target_label = ledger["attributes"]["target_label_name"].format(label = ledger['label'], version = ledger['version'], classification = ledger['classification'])
    ts = datetime.today().strftime('%Y%m%dT%H%M%S')
    sql_file_name = os.path.join(os.sep, "tmp", "pg_{0}_{1}_{2}_{3}.sql".format(project_code,ledger['label'],ledger['version'],ts)) 
    print(ledger)
    #print(ledger["attributes"]["sqlcodeblock"])
    schema_name = "base{0}t".format(project_code.lower())

    code_block = ledger["attributes"]["sqlcodeblock"].format(source_label = ledger['label'], target_label = target_label, source_table = ledger['location_details'], schema_name = schema_name)
    print(code_block)


    success_msg = "<<RUN_SQL_SUCCESS>>"

    # sql_stm = """{0}
    # PRINT('{1}')
    # """.format(code_block,success_msg)
    sql_stm = code_block

#     sql_stm = """
# DO LANGUAGE plpgsql $$
# <<MAINBLOCK>>
# BEGIN
# IF NOT EXISTS ( SELECT * FROM pg_catalog.pg_extension WHERE extname = 'temporal_tables' ) THEN
# CREATE EXTENSION temporal_tables;
# END IF;
# -- Main table
# IF NOT EXISTS ( SELECT * FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_name=LOWER('{target_label}') ) THEN
# CREATE TABLE {schema_name}.{target_label}
# (
#   id varchar(64) NOT NULL, 
#   alf int NOT NULL, 
#   score int NOT NULL, 
#   sys_period tstzrange NOT NULL
# );
# ALTER TABLE {schema_name}.{target_label} ADD CONSTRAINT pk_{target_label}_id PRIMARY KEY (id);
# CREATE INDEX ix_{target_label}_sys_period ON {schema_name}.{target_label} USING gist (sys_period);
# END IF;
# -- History table
# IF NOT EXISTS ( SELECT * FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_name=LOWER('{target_label}_history') ) THEN
# CREATE TABLE {schema_name}.{target_label}_history (LIKE {schema_name}.{target_label});
# CREATE INDEX ix_{target_label}_history_id ON {schema_name}.{target_label}_history (id); -- NOT unique
# CREATE INDEX ix_{target_label}_history_sys_period ON {schema_name}.{target_label}_history USING gist (sys_period);
# -- Trigger  --versioning(<system_period_column_name>, <history_table_name>, <adjust>)
# CREATE TRIGGER versioning_trigger_{target_label} 
# BEFORE INSERT OR UPDATE OR DELETE ON {schema_name}.{target_label} 
# FOR EACH ROW EXECUTE PROCEDURE public.versioning('sys_period', '{target_label}_history', true);
# END IF;
# -- View
# IF NOT EXISTS ( SELECT * FROM information_schema.views WHERE table_schema = '{schema_name}' AND table_name=LOWER('{target_label}_with_history') ) THEN
# CREATE VIEW {schema_name}.{target_label}_with_history AS
#     SELECT * FROM {schema_name}.{target_label}
#   UNION ALL
#     SELECT * FROM {schema_name}.{target_label}_history;
# END IF;
# -- Insert
# INSERT INTO {schema_name}.{target_label} (id, alf, score)
# SELECT
#     a.id, a.alf, a.score
# FROM {source_table} a
#     LEFT JOIN {schema_name}.{target_label} b ON a.id = b.id
# WHERE b.id IS NULL;
# -- Delete
# DELETE FROM {schema_name}.{target_label} 
# USING {schema_name}.{target_label} AS a
# LEFT OUTER JOIN {source_table} AS b ON a.id = b.id
# WHERE {target_label}.id = a.id AND b.id IS NULL;
# raise notice '%', '<<RUN_SQL_SUCCESS>>';
# exception when others then
#     raise notice '%', '<<RUN_SQL_FAILURE>>';
#     raise notice 'FAIL POINT: Exception details: % %', SQLERRM, SQLSTATE;
#     EXIT mainblock;
# END
# $$;
#     """.format(source_label = ledger['label'], target_label = target_label, source_table = ledger['location_details'], schema_name = schema_name)
#     print(sql_stm)

    # Hardoded test - pg_load_test has the temporal table / versioning extension installed
    pg_database = "data_load_test_rob"
    #pg_database = "pg_load_test"

    print(sql_file_name)
    with open(sql_file_name, 'w') as text_file:
        text_file.write(sql_stm)

    sql_output = os.path.join(output_msg_dir,"sql_output.txt")

    sql_stm = 'psql -d "host={4} port={6} user={0} password={1} dbname={5}" -f {2} -o {3}'.format(pg_login, pg_password, sql_file_name, sql_output, pg_server, pg_database, pg_port)
    print(sql_stm)

    retval = subprocess.Popen(['bash', '-c', sql_stm], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    retval.wait()
    (stdout, stderr) = retval.communicate()

    notices = stderr.decode("utf-8")
    print(notices)

    if success_msg not in notices:
        print("fail")
        raise ValueError("SQL failed")

    #house keeping
    os.remove(sql_file_name)

    if retval.returncode != 0:
        print(notices)
    else:
        print("success")

    print("stdout:")
    print(stdout)

    