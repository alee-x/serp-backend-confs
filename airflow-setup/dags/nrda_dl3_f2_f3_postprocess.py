import sys
import os
import json
import airflow
import glob
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from subprocess import PIPE, Popen
from airflow.hooks.base_hook import BaseHook

# Would be cleaner to add the path to the PYTHONPATH variable
currentdir = os.path.dirname(os.path.realpath(__file__))
subdir = currentdir+'/scripts/nrda'

sys.path.append(subdir)

from rabbitmq_notifier import send_progress_message, send_failure_message, send_success_message

# store payload to file to avoid issue of oversized arg for spark-submit
def store_ledgers_to_hdfs(**context):
    print("job task id: {0}".format(context['dag_run'].conf['jobtask_id']))
    ledgers = context['dag_run'].conf['ledgers']
    project_code = context['dag_run'].conf['project_code']   
    print(project_code)
    print("total ledgers: {0}".format(len(ledgers)))    
    
    # get f2 ledger
    f2_ledger = [i for i in ledgers if i['label'].lower() == 'f2'][0]
    print("f2 ledger: {0}".format(f2_ledger))
    # get f3 ledger
    f3_ledger = [i for i in ledgers if i['label'].lower() == 'f3'][0]
    print("f3 ledger: {0}".format(f3_ledger))
    # get f3-map ledger
    f3_map_ledger = [i for i in ledgers if i['label'].lower() == 'f3-map'][0]
    print("f3-map ledger: {0}".format(f3_map_ledger))

    # create hdfs folder for dl3 ledgers
    version = f2_ledger["version"]
    mkdir = Popen(["hdfs", "dfs", "-mkdir", "-p", os.path.join(os.sep,"stage", project_code, version,"dl3")], stdin=PIPE, bufsize=-1)
    mkdir.communicate()
    # upload f2 ledger to hdfs
    f2_ledger_json_hdfs_path = os.path.join(os.sep, "stage", project_code, version, "dl3", "f2_ledger.json")
    f2_ledger_json_local_path = os.path.join(os.sep, "tmp", "f2_ledger_{0}_{1}.json".format(project_code, version))
    with open(f2_ledger_json_local_path, 'w') as fp:
        json.dump(f2_ledger, fp)
    put = Popen(["hdfs", "dfs", "-put", "-f", f2_ledger_json_local_path, f2_ledger_json_hdfs_path], stdin=PIPE, bufsize=-1)
    put.communicate()
    os.remove(f2_ledger_json_local_path)
    context['ti'].xcom_push(key='f2_ledger_json_hdfs_path', value=f2_ledger_json_hdfs_path)

    # upload f3 ledger to hdfs
    f3_ledger_json_hdfs_path = os.path.join(os.sep, "stage", project_code, version, "dl3", "f3_ledger.json")
    f3_ledger_json_local_path = os.path.join(os.sep, "tmp", "f3_ledger_{0}_{1}.json".format(project_code, version))
    with open(f3_ledger_json_local_path, 'w') as fp:
        json.dump(f3_ledger, fp)
    put = Popen(["hdfs", "dfs", "-put", "-f", f3_ledger_json_local_path, f3_ledger_json_hdfs_path], stdin=PIPE, bufsize=-1)
    put.communicate()
    os.remove(f3_ledger_json_local_path)
    context['ti'].xcom_push(key='f3_ledger_json_hdfs_path', value=f3_ledger_json_hdfs_path)

    # upload f3-map ledger to hdfs
    f3_map_ledger_json_hdfs_path = os.path.join(os.sep, "stage", project_code, version, "dl3", "f3_map_ledger.json")
    f3_map_ledger_json_local_path = os.path.join(os.sep, "tmp", "f3_map_ledger_{0}_{1}.json".format(project_code, version))
    with open(f3_map_ledger_json_local_path, 'w') as fp:
        json.dump(f3_map_ledger, fp)
    put = Popen(["hdfs", "dfs", "-put", "-f", f3_map_ledger_json_local_path, f3_map_ledger_json_hdfs_path], stdin=PIPE, bufsize=-1)
    put.communicate()
    os.remove(f3_map_ledger_json_local_path)
    context['ti'].xcom_push(key='f3_map_ledger_json_hdfs_path', value=f3_map_ledger_json_hdfs_path)

def housekeeping(**context):    
    f2_ledger_json_hdfs_path = context['ti'].xcom_pull(key='f2_ledger_json_hdfs_path') 
    put = Popen(["hdfs", "dfs", "-rm", "-f", f2_ledger_json_hdfs_path], stdin=PIPE, bufsize=-1)
    put.communicate()
    print("ledger json file {0} has been deleted".format(f2_ledger_json_hdfs_path))

    f3_map_ledger_json_hdfs_path = context['ti'].xcom_pull(key='f3_map_ledger_json_hdfs_path') 
    put = Popen(["hdfs", "dfs", "-rm", "-f", f3_map_ledger_json_hdfs_path], stdin=PIPE, bufsize=-1)
    put.communicate()
    print("ledger json file {0} has been deleted".format(f3_map_ledger_json_hdfs_path))

    f3_map_ledger_json_hdfs_path = context['ti'].xcom_pull(key='f3_map_ledger_json_hdfs_path') 
    put = Popen(["hdfs", "dfs", "-rm", "-f", f3_map_ledger_json_hdfs_path], stdin=PIPE, bufsize=-1)
    put.communicate()
    print("ledger json file {0} has been deleted".format(f3_map_ledger_json_hdfs_path))
    

# return 1 result ledger
def prep_result_legders(**context):
    project_code = context['dag_run'].conf['project_code']       
    # get f3-map ledger
    f3_map_ledger = [i for i in context['dag_run'].conf['ledgers'] if i['label'].lower() == 'f3-map'][0]
    version = f3_map_ledger["version"]

    target_label = "F2"
    target_location_detail = os.path.join(os.sep, "stage", project_code, version, "dl3", "transformed_f2.parquet")

    if f3_map_ledger["attributes"]["usenewid"].lower()  == "false":
        target_label = "F3"
        target_location_detail = os.path.join(os.sep, "stage", project_code, version, "dl3", "transformed_f3.parquet")
    
    result_legders = [{
        "classification": f3_map_ledger["attributes"]["target_classification"],
        "label": target_label,
        "version": version,
        "group_count": f3_map_ledger["group_count"],
        "location": "HDFS-DB",
        "location_details": target_location_detail,
        "row_count": f3_map_ledger["row_count"],
        "attributes":{
            "project": project_code.lower()
        }
    }]
    print(json.dumps(result_legders))
    context['ti'].xcom_push(key='result_legders', value=result_legders)


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


with DAG(dag_id="nrda_dl3_f2_f3_postprocess", schedule_interval=None, default_args=default_args, catchup=False, max_active_runs=1, user_defined_macros={
    'json': json
}) as dag:

    store_ledgers_to_hdfs = PythonOperator(
        task_id="store_ledgers_to_hdfs",
        python_callable=store_ledgers_to_hdfs,
        provide_context=True
    )

    process_f2_f3_for_dl3 = SparkSubmitOperator(
        task_id="process_f2_f3_for_dl3",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/nrda/process_f2_f3_for_dl3.py",
        executor_memory="30g",
        executor_cores=9,
        application_args=["{{ dag_run.conf['project_code'] }}", "{{ ti.xcom_pull(key='f2_ledger_json_hdfs_path') }}", "{{ ti.xcom_pull(key='f3_ledger_json_hdfs_path') }}", "{{ ti.xcom_pull(key='f3_map_ledger_json_hdfs_path') }}", "{{ dag_run.conf['jobtask_id'] }}"],
        verbose=False
    )

    prep_result_legders = PythonOperator(
        task_id="prep_ledger",
        python_callable=prep_result_legders,
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
    store_ledgers_to_hdfs  >> process_f2_f3_for_dl3 >> prep_result_legders >> housekeeping >> send_message 