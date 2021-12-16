import os 
import subprocess
import logging
import json
from airflow.hooks.base_hook import BaseHook

def run_base_load(**context):
    db2_conn = BaseHook.get_connection('db2_prsail_conn')
    local_dir = context['ti'].xcom_pull(key='local_dir')

    ledger = context['dag_run'].conf['ledger']
    schemaDef = json.loads(ledger["attributes"]["schema"])
    project_code = context['dag_run'].conf['project_code']
    enc_col_list = schemaDef["return_info"]["schema_cols_to_enc"]
    if enc_col_list is None:
        enc_col_list = []
    #cols for KEY encryption
    key_enc_items = [d for d in enc_col_list if (d['enc_type'].upper() == 'KEY50' or d['enc_type'].upper() == 'KEY30') and d['enc_location'].upper() == 'DB']
    #cols for HCP encryption
    hcp_enc_items = [d for d in enc_col_list if (d['enc_type'].upper() == 'HCP') and d['enc_location'].upper() == 'DB']
    #cols for ALF encryption
    alf_enc_items = [d for d in enc_col_list if (d['enc_type'].upper() == 'ALF') and d['enc_location'].upper() == 'DB']
    #cols for RALF encryption
    ralf_enc_items = [d for d in enc_col_list if (d['enc_type'].upper() == 'RALF') and d['enc_location'].upper() == 'DB']

    load_schema = "load{0}t".format(project_code.lower())

    base_schema_prefix = "base"
    # check if QACK is used instead    
    if("load_to_qack" in ledger["attributes"]):
        base_schema_prefix="qack"    
    base_schema = "{0}{1}t".format(base_schema_prefix,project_code.lower())
    # overwrite schema if ms_database_schema is provided in attributes
    if 'db2_database_schema' in ledger["attributes"]:
        load_schema = "load{0}t".format(ledger["attributes"]["db2_database_schema"].lower())
        base_schema = "{0}{1}t".format(base_schema_prefix,ledger["attributes"]["db2_database_schema"].lower())
    # check if schema is overwritten    
    if("schema_to_overwrite" in ledger["attributes"]):
        base_schema=ledger["attributes"]["schema_to_overwrite"] #todo: need to revisit to see if {base/qack}{code}t format is needed
    #base_schema = "base{0}t".format(project_code.lower())
    table = "nrdav2_{0}_{1}".format(ledger['label'],ledger['version'])
    if 'versionenabled' in ledger["attributes"]:
        if ledger["attributes"]["versionenabled"].lower() == 'false':
            table = "nrdav2_{0}".format(ledger['label'])
    sql_file_name = os.path.join(local_dir, "{0}_base_{1}.sql".format(base_schema,table)) 
           
    sql_stm = """
connect to pr_sail user {0} using {1};
""".format(db2_conn.login,db2_conn.password)


    #print("[key_enc_items] {0}".format(key_enc_items))
    #KEY encryption
    for col in key_enc_items:
        char_size = 30
        if col['enc_type'].upper() == 'KEY50':
            char_size = 50
        print("[KEY ENC] {0} - {1}".format(col['enced_col_name'],col['enc_type']))
        sql_stm = sql_stm + """
declare global temporary table tmp_{0} (
   {0} char({1}),
   {0}_e bigint,
   {0}_e_ori varchar(64) for bit data
) with replace ORGANIZE BY ROW on commit preserve rows not logged in usertemp_pd_data;
        """.format(col['enced_col_name'], char_size)

    for col in key_enc_items:
        char_size = 30
        if col['enc_type'].upper() == 'KEY50':
            char_size = 50
        sql_stm = sql_stm + """
insert into session.tmp_{0} (
   {0},
   {0}_e_ori
) select 
   DISTINCT {0},
   encrypt( Cast({0} as char({3})) ,(select pwd from basectrlt.sec) )
from {1}.{2} a;

commit;
        """.format(col['enced_col_name'],load_schema,table,char_size)


    for col in key_enc_items:
        sql_stm = sql_stm + """
merge into session.tmp_{0} tgt
using (select KEY_e
   ,KEY_e_ori
from  basectrlt.KEY_xref
) as src
on tgt.{0}_e_ori = src.KEY_e_ori
when matched
then update set tgt.{0}_e = src.KEY_e;

commit;
        """.format(col['enced_col_name'])

    #ALF encryption
    for col in alf_enc_items:
        char_size = 10        
        print("[ALF ENC] {0}".format(col['enced_col_name']))
        sql_stm = sql_stm + """
declare global temporary table tmp_{0} (
   {0} char({1}),
   {0}_e bigint,
   {0}_e_ori varchar(64) for bit data
) with replace ORGANIZE BY ROW on commit preserve rows not logged in usertemp_pd_data;
        """.format(col['enced_col_name'], char_size)

    for col in alf_enc_items:
        char_size = 10
        sql_stm = sql_stm + """
insert into session.tmp_{0} (
   {0},
   {0}_e_ori
) select 
   DISTINCT {0},
   encrypt( Cast({0} as char({3})) ,(select pwd from basectrlt.sec) )
from {1}.{2} a;

commit;
        """.format(col['enced_col_name'],load_schema,table,char_size)


    for col in alf_enc_items:
        sql_stm = sql_stm + """
merge into session.tmp_{0} tgt
using (select ALF_e
   ,ALF_e_ori
from  basectrlt.ALF_xref
) as src
on tgt.{0}_e_ori = src.ALF_e_ori
when matched
then update set tgt.{0}_e = src.ALF_e;

commit;
        """.format(col['enced_col_name'])

    #HCP encryption
    for col in hcp_enc_items:
        char_size = 12
        print("[HCP ENC] {0}".format(col['enced_col_name']))
        sql_stm = sql_stm + """
declare global temporary table tmp_{0} (
   {0} char({1}),
   {0}_e integer,
   {0}_e_ori varchar(64) for bit data
) with replace ORGANIZE BY ROW on commit preserve rows not logged in usertemp_pd_data;
        """.format(col['enced_col_name'], char_size)

    for col in hcp_enc_items:
        char_size = 12
        sql_stm = sql_stm + """
insert into session.tmp_{0} (
   {0},
   {0}_e_ori
) select 
   DISTINCT {0},
   encrypt( Cast({0} as char({3})) ,(select pwd from basectrlt.sec) )
from {1}.{2} a;

commit;
        """.format(col['enced_col_name'],load_schema,table,char_size)


    for col in hcp_enc_items:
        sql_stm = sql_stm + """
merge into session.tmp_{0} tgt
using (select hcp_e
   ,hcp_e_ori
from  basectrlt.hcp_xref
) as src
on tgt.{0}_e_ori = src.hcp_e_ori
when matched
then update set tgt.{0}_e = src.hcp_e;

commit;
        """.format(col['enced_col_name'])


    #RALF encryption
    for col in ralf_enc_items:
        char_size = 16        
        print("[RALF ENC] {0}".format(col['enced_col_name']))
        sql_stm = sql_stm + """
declare global temporary table tmp_{0} (
   {0} char({1}),
   {0}_e bigint,
   {0}_e_ori varchar(64) for bit data
) with replace ORGANIZE BY ROW on commit preserve rows not logged in usertemp_pd_data;
        """.format(col['enced_col_name'], char_size)

    for col in ralf_enc_items:
        char_size = 16
        sql_stm = sql_stm + """
insert into session.tmp_{0} (
   {0},
   {0}_e_ori
) select 
   DISTINCT {0},
   encrypt( Cast({0} as char({3})) ,(select pwd from basectrlt.sec) )
from {1}.{2} a;

commit;
        """.format(col['enced_col_name'],load_schema,table,char_size)

    for col in ralf_enc_items:
        sql_stm = sql_stm + """
merge into session.tmp_{0} tgt
using (select RALF_e
   ,RALF_e_ori
from  basectrlt.RALF_xref
) as src
on tgt.{0}_e_ori = src.RALF_e_ori
when matched
then update set tgt.{0}_e = src.RALF_e;

commit;
        """.format(col['enced_col_name'])




    base_col_file = os.path.join(local_dir, "base_col_{0}_{1}.txt".format(base_schema,table)) 
    base_col_str = open(base_col_file, "r").read()
    sql_stm = sql_stm + """
declare mycurs_1 cursor for select 
{0}current_date
from {1}.{2} main
        """.format(base_col_str,load_schema,table)

    for col in key_enc_items:
        sql_stm = sql_stm + """
left outer join session.tmp_{0} temp_key_{0}_e on temp_key_{0}_e.{0} = main.{0}
        """.format(col['enced_col_name'])
    for col in alf_enc_items:
        sql_stm = sql_stm + """
left outer join session.tmp_{0} temp_key_{0}_e on temp_key_{0}_e.{0} = main.{0}
        """.format(col['enced_col_name'])
    for col in hcp_enc_items:
        sql_stm = sql_stm + """
left outer join session.tmp_{0} temp_key_{0}_e on temp_key_{0}_e.{0} = main.{0}
        """.format(col['enced_col_name'])
    for col in ralf_enc_items:
        sql_stm = sql_stm + """
left outer join session.tmp_{0} temp_key_{0}_e on temp_key_{0}_e.{0} = main.{0}
        """.format(col['enced_col_name'])

    sql_stm = sql_stm + """
;
load from mycurs_1 of cursor messages {2}/error_base_{0}_{1}.txt replace into {0}.{1} nonrecoverable;
commit;
        """.format(base_schema,table,local_dir)
    index_counter = 0
    for i in range(len(key_enc_items)):
        index_counter = index_counter + 1
        sql_stm = sql_stm + "\ncreate index {0}.{1}_I{3} ON {0}.{1} ({2});".format(base_schema,table,key_enc_items[i]['enced_col_name']+"_e", index_counter)
    for i in range(len(alf_enc_items)):
        index_counter = index_counter + 1
        sql_stm = sql_stm + "\ncreate index {0}.{1}_I{3} ON {0}.{1} ({2});".format(base_schema,table,alf_enc_items[i]['enced_col_name']+"_e", index_counter)              
    for i in range(len(hcp_enc_items)):
        index_counter = index_counter + 1
        sql_stm = sql_stm + "\ncreate index {0}.{1}_I{3} ON {0}.{1} ({2});".format(base_schema,table,hcp_enc_items[i]['enced_col_name']+"_e", index_counter)                                
    for i in range(len(ralf_enc_items)):
        index_counter = index_counter + 1
        sql_stm = sql_stm + "\ncreate index {0}.{1}_I{3} ON {0}.{1} ({2});".format(base_schema,table,ralf_enc_items[i]['enced_col_name']+"_e", index_counter)              

    sql_stm = sql_stm + """
runstats on table {0}.{1} with distribution and detailed indexes all;
connect reset;
        """.format(base_schema,table)   

    with open(sql_file_name, 'w') as text_file:
         text_file.write(sql_stm)
    #logging.info(sql_stm)
    logging.info("base_load sql file is generated at: {0}".format(sql_file_name))
    #os.system(". /usr/local/airflow/sqllib/db2profile & db2 -tvmf {0} | tee {1}/log_{2}_{3}.txt".format(sql_file_name,output_msg_dir,base_schema,table))
    subprocess.call(['bash', '-c', '. /usr/local/airflow/sqllib/db2profile && db2 +c -tvmf {0} -z {1}/log_base_{2}_{3}.txt'.format(sql_file_name,local_dir,base_schema,table)])