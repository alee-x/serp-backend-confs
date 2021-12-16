import os 
import subprocess
import logging
import json
from airflow.hooks.base_hook import BaseHook

def run_xref_load(**context):
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

    schema = "load{0}t".format(project_code.lower())
    # overwrite schema if ms_database_schema is provided in attributes
    if 'db2_database_schema' in ledger["attributes"]:
        schema = "load{0}t".format(ledger["attributes"]["db2_database_schema"].lower())
        
    table = "nrdav2_{0}_{1}".format(ledger['label'],ledger['version'])
    if 'versionenabled' in ledger["attributes"]:
        if ledger["attributes"]["versionenabled"].lower() == 'false':
            table = "nrdav2_{0}".format(ledger['label'])
    sql_file_name = os.path.join(local_dir, "{0}_xref_{1}.sql".format(schema,table)) 
 
    xref_stm = """
connect to PR_SAIL user {0} using {1};
    """.format(db2_conn.login,db2_conn.password)
    
    #KEY encryption
    if key_enc_items:
        xref_stm = xref_stm + """
declare global temporary table tmp1
as (select KEY_e_ori from basectrlt.KEY_xref)
definition only with replace on commit preserve rows not logged in usertemp_sd_data;
    """
        for col in key_enc_items:
            char_size = 30
            if col['enc_type'].upper() == 'KEY50':
                char_size = 50
            print("[KEY ENC] {0} - {1}".format(col['enced_col_name'],col['enc_type']))
            xref_stm = xref_stm + """
insert into session.tmp1
select distinct encrypt(Cast({0} as char({3})),(select pwd from basectrlt.sec))
from {1}.{2}
where  {0} <> ''
or {0} is not null;
commit;
            """.format(col['enced_col_name'],schema,table,char_size)

        xref_stm = xref_stm + """
declare global temporary table tmp1a
as (select KEY_e_ori from basectrlt.KEY_xref)
definition only with replace on commit preserve rows not logged in usertemp_sd_data;

insert into session.tmp1a
select distinct a.KEY_e_ori
from session.tmp1 a
where not exists (select b.KEY_e_ori
                   from basectrlt.KEY_xref b
                   where a.KEY_e_ori = b.KEY_e_ori);
commit;

insert into basectrlt.KEY_xref
select next value for basectrls.KEY_e_seq
    ,a.KEY_e_ori
    ,current_timestamp
from session.tmp1a a;
commit;
        """

    #ALF encryption
    if alf_enc_items:
        xref_stm = xref_stm + """
declare global temporary table tmp3
as (select ALF_e_ori from basectrlt.ALF_xref)
definition only with replace on commit preserve rows not logged in usertemp_sd_data;
    """
        for col in alf_enc_items:
            char_size = 10
            print("[ALF ENC] {0}".format(col['enced_col_name']))
            xref_stm = xref_stm + """
insert into session.tmp3
select distinct encrypt(Cast({0} as char({3})),(select pwd from basectrlt.sec))
from {1}.{2}
where  {0} <> ''
or {0} is not null;
commit;
            """.format(col['enced_col_name'],schema,table,char_size)

        xref_stm = xref_stm + """
declare global temporary table tmp3a
as (select ALF_e_ori from basectrlt.ALF_xref)
definition only with replace on commit preserve rows not logged in usertemp_sd_data;

insert into session.tmp3a
select distinct a.ALF_e_ori
from session.tmp3 a
where not exists (select b.ALF_e_ori
                   from basectrlt.ALF_xref b
                   where a.ALF_e_ori = b.ALF_e_ori);
commit;

insert into basectrlt.ALF_xref
select next value for basectrls.ALF_e_seq
    ,a.ALF_e_ori
    ,current_timestamp
from session.tmp3a a;
commit;
        """

    #HCP encryption
    if hcp_enc_items:
        xref_stm = xref_stm + """
declare global temporary table tmp5
as (select hcp_e_ori from basectrlt.hcp_xref)
definition only with replace on commit preserve rows not logged in usertemp_sd_data
organize by row
;
    """
        for col in hcp_enc_items:
            char_size = 12
            print("[HCP ENC] {0}".format(col['enced_col_name']))
            xref_stm = xref_stm + """
insert into session.tmp5
select distinct encrypt(Cast({0} as char({3})),(select pwd from basectrlt.sec))
from {1}.{2}
where  {0} <> ''
or {0} is not null;
commit;
            """.format(col['enced_col_name'],schema,table,char_size)

        xref_stm = xref_stm + """
declare global temporary table tmp6
as (select hcp_e_ori from basectrlt.hcp_xref)
definition only with replace on commit preserve rows not logged in usertemp_sd_data
organize by row
;

insert into session.tmp6
select distinct a.hcp_e_ori
from   session.tmp5 a
where  not exists (select b.hcp_e_ori
                   from   basectrlt.hcp_xref b
                   where  a.hcp_e_ori = b.hcp_e_ori);
commit;

insert into basectrlt.hcp_xref
select 	next value for basectrls.hcp_e_seq
       ,a.hcp_e_ori
       ,current_timestamp
from   	session.tmp6 a;
commit;
        """

    #RALF encryption
    if ralf_enc_items:
        xref_stm = xref_stm + """
declare global temporary table tmp7
as (select RALF_e_ori from basectrlt.RALF_xref)
definition only with replace on commit preserve rows not logged in usertemp_sd_data;
    """
        for col in ralf_enc_items:
            char_size = 16
            print("[RALF ENC] {0}".format(col['enced_col_name']))
            xref_stm = xref_stm + """
insert into session.tmp7
select distinct encrypt(Cast({0} as char({3})),(select pwd from basectrlt.sec))
from {1}.{2}
where  {0} <> ''
or {0} is not null;
commit;
            """.format(col['enced_col_name'],schema,table,char_size)

        xref_stm = xref_stm + """
declare global temporary table tmp7a
as (select RALF_e_ori from basectrlt.RALF_xref)
definition only with replace on commit preserve rows not logged in usertemp_sd_data;

insert into session.tmp7a
select distinct a.RALF_e_ori
from session.tmp7 a
where not exists (select b.RALF_e_ori
                   from basectrlt.RALF_xref b
                   where a.RALF_e_ori = b.RALF_e_ori);
commit;

insert into basectrlt.RALF_xref
select next value for basectrls.RALF_e_seq
    ,a.RALF_e_ori
    ,current_timestamp
from session.tmp7a a;
commit;
        """
        
    xref_stm = xref_stm + """
connect reset;
    """

    with open(sql_file_name, 'w') as text_file:
         text_file.write(xref_stm)
    #logging.info(xref_stm)
    logging.info("load_xref sql file is generated at: {0}".format(sql_file_name))
    #os.system(". /usr/local/airflow/sqllib/db2profile & db2 -tvmf {0} | tee {1}/log_{2}_xref_{3}.txt".format(sql_file_name,output_msg_dir,schema,table))
    subprocess.call(['bash', '-c', '. /usr/local/airflow/sqllib/db2profile && db2 +c -tvmf {0} -z {1}/log_xref_{2}_{3}.txt'.format(sql_file_name,local_dir,schema,table)])
