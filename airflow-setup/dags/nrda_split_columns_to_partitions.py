import sys
import os
import json
import airflow
import glob
import shutil
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

from data_loading.store_ledger_to_file import store_ledger_to_file
from rabbitmq_notifier import send_progress_message, send_failure_message, send_success_message


def prep_result_legders(**context):
    ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']
    local_dir = context['ti'].xcom_pull(key='local_dir')
    # get number of partitions
    partition_num_local = os.path.join(local_dir, "partition_num_{0}_{1}_{2}.txt".format(project_code,ledger["version"],ledger["label"]))
    print(partition_num_local)
    partition_num_str = open(partition_num_local, "r").read()
    partition_num = int(partition_num_str)

    target_label_tmemplate = ledger["attributes"]["target_label_name"].format(label = ledger["label"], classification = ledger["classification"], version = ledger["version"])
    result_legders = []
    for x in range(partition_num):
       result_legders.append({
        "classification": ledger["attributes"]["target_classification"],
        "label": "{0}_{1}".format(target_label_tmemplate, x+1),
        "version": ledger["version"],
        "group_count": ledger["group_count"],
        "row_count": ledger["row_count"],
        "location": "HDFS-DB",
        "location_details": os.path.join(os.sep, "stage", project_code, ledger["version"], "{0}_{1}.parquet".format(ledger["label"], x+1)),
        "attributes":{
            "source_ledger_id":  ledger["id"]
        }
    })

    print(json.dumps(result_legders))
    context['ti'].xcom_push(key='result_legders', value=result_legders)

def housekeeping(**context):
    local_dir = context['ti'].xcom_pull(key='local_dir')
    shutil.rmtree(local_dir)
    print("removed local dir '{0}'".format(local_dir))

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

with DAG(dag_id="nrda_split_columns_to_partitions", schedule_interval=None, default_args=default_args, catchup=False, max_active_runs=1, user_defined_macros={
    'json': json
}) as dag:

    store_ledger_to_file = PythonOperator(
        task_id="store_ledger_to_file",
        python_callable=store_ledger_to_file,
        provide_context=True
    )

    split_wide_source_file_to_small_partitions = SparkSubmitOperator(
        task_id="split_wide_source_file_to_small_partitions",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/nrda/split_wide_source_file_to_small_partitions.py",
        executor_memory="30g",
        executor_cores=9,
        application_args=["stage", "{{ dag_run.conf['project_code'] }}", "{{ ti.xcom_pull(key='ledger_json_file_name') }}", "{{ ti.xcom_pull(key='local_dir') }}"],
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


    store_ledger_to_file >> split_wide_source_file_to_small_partitions >> prep_result_legders >> housekeeping >> send_message