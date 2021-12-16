import sys
import os
import json
import airflow
import glob
import shutil
import pandas as pd
import pyreadstat
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from subprocess import PIPE, Popen
from airflow.hooks.base_hook import BaseHook

# Would be cleaner to add the path to the PYTHONPATH variable
currentdir = os.path.dirname(os.path.realpath(__file__))
subdir = currentdir+'/scripts/nrda'

sys.path.append(subdir)

from data_loading.store_ledger_to_file import store_ledger_to_file

from rabbitmq_notifier import send_progress_message, send_failure_message, send_success_message

def housekeeping(**context):    
    ledger_json_file_name = context['ti'].xcom_pull(key='ledger_json_file_name') 
    local_file_path = context['ti'].xcom_pull(key='local_file_path') 
    converted_csv_path = context['ti'].xcom_pull(key='converted_csv_path') 
    local_dir = context['ti'].xcom_pull(key='local_dir')
    ledger = context['dag_run'].conf['ledger']

    os.remove(ledger_json_file_name)
    os.remove(local_file_path)
    os.remove(converted_csv_path)
    shutil.rmtree(local_dir)
    print("removed local dir '{0}'".format(local_dir))

def convert_file_to_csv(**context):
    ledger = context['dag_run'].conf['ledger']    
    local_dir = context['ti'].xcom_pull(key='local_dir')    
    # download the directory from hdfs
    put = Popen(["hdfs", "dfs", "-copyToLocal","-f", ledger["location_details"], local_dir], stdin=PIPE, bufsize=-1)
    put.communicate()
    local_file_path = "{0}/{1}".format(local_dir, os.path.basename(ledger["location_details"]))
    context['ti'].xcom_push(key='local_file_path', value=local_file_path)
    print("downloaded file path: {0}".format(local_file_path))
    converted_csv_path = "{0}.csv".format(os.path.splitext(local_file_path)[0])
    # convert to csv
    source_file_type = context['dag_run'].conf['ledger']["attributes"]["file_type"].lower()    

    if source_file_type == "spss":
        #df = pd.read_spss(local_file_path)
        df, meta = pyreadstat.read_sav(local_file_path)
        df.to_csv(converted_csv_path, index=False)

        with open(converted_csv_path+'-values.csv', 'w' ) as csvfile:
            csvfile.write("Field,Code,Meaning\n")
            for key1, value1 in meta.variable_value_labels.items():
                for key2, value2 in value1.items():
                    csvfile.write('"'+key1 + '",'+str(key2)+',"'+value2+'"\n')

        with open(converted_csv_path+'-description.csv', 'w' ) as csvfile2:
                csvfile2.write("Field,Meaning\n")
                for k1, v1 in meta.column_names_to_labels.items():
                    csvfile2.write('"'+k1 + '",'+str(v1)+'\n')

        print("SPSS file {0} has been converted to CSV {1}".format(local_file_path,converted_csv_path))

    if source_file_type == "stata":
        #df = pd.read_stata(local_file_path)
        df = pd.read_stata(local_file_path,convert_categoricals=False)
        sr = pd.io.stata.StataReader(local_file_path)
        vl = sr.value_labels()        
        sr.close()

        with open(converted_csv_path+'-values.csv', 'w' ) as csvfile:
            csvfile.write("Field,Code,Meaning\n")
            for key1, value1 in vl.items():
                for key2, value2 in value1.items():
                    csvfile.write('"'+key1 + '",'+str(key2)+',"'+value2+'"\n')

        df.to_csv(converted_csv_path, index=False)
        print("STATA file {0} has been converted to CSV {1}".format(local_file_path,converted_csv_path))

    # upload csv to hdfs
    if os.path.exists(converted_csv_path):
        context['ti'].xcom_push(key='converted_csv_path', value=converted_csv_path)
        csv_hdfs_dir = os.path.dirname(ledger["location_details"])

        put = Popen(["hdfs", "dfs", "-put", "-f", converted_csv_path, csv_hdfs_dir], stdin=PIPE, bufsize=-1)
        put.communicate()
        if os.path.exists(converted_csv_path+'-values.csv'):
            put2 = Popen(["hdfs", "dfs", "-put", "-f", converted_csv_path+'-values.csv', csv_hdfs_dir], stdin=PIPE, bufsize=-1)
            put2.communicate()
        if os.path.exists(converted_csv_path+'-description.csv'):
            put3 = Popen(["hdfs", "dfs", "-put", "-f", converted_csv_path+'-description.csv', csv_hdfs_dir], stdin=PIPE, bufsize=-1)
            put3.communicate()

        # prepare result legders
        result_legders = [{
            "classification": ledger["attributes"]["targetclassification"],
            "label": ledger["label"],
            "version": ledger["version"],
            "group_count": ledger["group_count"],
            "location": "HDFS",
            "location_details": "{0}/{1}".format(csv_hdfs_dir, os.path.basename(converted_csv_path)),
            "attributes":{
                "project": context['dag_run'].conf['project_code'].lower()
            }
        }]

        if os.path.exists(converted_csv_path+'-values.csv'):
            result_legders.append({
                "classification": 'V-'+ledger["attributes"]["targetclassification"],
                "label": ledger["label"]+"-values",
                "version": ledger["version"],
                "group_count": ledger["group_count"],
                "location": "HDFS",
                "location_details": "{0}/{1}".format(csv_hdfs_dir, os.path.basename(converted_csv_path+'-values.csv')),
                "attributes":{
                    "project": context['dag_run'].conf['project_code'].lower()
                }
            })

        if os.path.exists(converted_csv_path+'-description.csv'):
            result_legders.append({
                "classification": 'Def-'+ledger["attributes"]["targetclassification"],
                "label": ledger["label"]+"-values",
                "version": ledger["version"],
                "group_count": ledger["group_count"],
                "location": "HDFS",
                "location_details": "{0}/{1}".format(csv_hdfs_dir, os.path.basename(converted_csv_path+'-description.csv')),
                "attributes":{
                    "project": context['dag_run'].conf['project_code'].lower()
                }
            })

        print(json.dumps(result_legders))
        context['ti'].xcom_push(key='result_legders', value=result_legders)
    else:
        raise Exception("Converted csv {0} doesn't exist. This might be caused by failture to convert files to csv".format(converted_csv_path))    

default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "jeffrey@chi.swan.ac.uk",
    # "retries": 1,
    # "retry_delay": timedelta(minutes=20),
    "on_success_callback": send_progress_message,
    "on_failure_callback": send_failure_message
}


with DAG(dag_id="nrda_convert_files_to_csv", schedule_interval=None, default_args=default_args, catchup=False, max_active_runs=5, user_defined_macros={
    'json': json
}, tags=['working']) as dag:

    store_ledger_to_file = PythonOperator(
        task_id="store_ledger_to_file",
        python_callable=store_ledger_to_file,
        provide_context=True
    )

    convert_file_to_csv = PythonOperator(
        task_id="convert_file_to_csv",
        python_callable=convert_file_to_csv,
        provide_context=True
    )

    housekeeping = PythonOperator(
        task_id="housekeeping",
        python_callable=housekeeping,
        provide_context=True
    )

    send_message = PythonOperator(
        task_id="send_message",
        python_callable=send_success_message,
        provide_context=True,
        on_success_callback=None
    )


    store_ledger_to_file >> convert_file_to_csv >> housekeeping >> send_message
