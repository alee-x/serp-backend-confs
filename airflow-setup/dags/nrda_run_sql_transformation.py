import sys
import os
import json
import airflow
import glob
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
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


def prep_result_legders(**context):
    souce_ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']   
    version = souce_ledger["version"]
    classification = souce_ledger["classification"]
    label = souce_ledger["label"]
    target_label = souce_ledger["attributes"]["target_label_name"].format(label = label, classification = classification, version = version)
    transformed_parquet_stage_hdfs_path = os.path.join(os.sep, "stage", project_code, version,"transformed","{0}.parquet".format(target_label))
     # get row count file
    rowcount_stage_hdfs_path = os.path.join(os.sep, "stage", project_code, version,"transformed", "rowcount_{0}.txt".format(label))
    rowcount_local = os.path.join(os.sep, "tmp", "rowcount_{0}_{1}_{2}_{3}.txt".format(project_code,version,"transformed",label))
    get = Popen(["hdfs", "dfs", "-get", rowcount_stage_hdfs_path, rowcount_local], stdin=PIPE, bufsize=-1)
    get.communicate()
    rowcount_str = open(rowcount_local, "r").read()
    os.remove(rowcount_local) 
    
    result_legders = [{
        "classification": souce_ledger["attributes"]["target_classification"],
        "label": target_label,
        "version": version,
        "group_count": souce_ledger["group_count"],
        "location": "HDFS-DB",
        "location_details": transformed_parquet_stage_hdfs_path,
        "row_count": int(rowcount_str),
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


with DAG(dag_id="nrda_run_sql_transformation", schedule_interval=None, default_args=default_args, catchup=False, max_active_runs=3, user_defined_macros={
    'json': json
}) as dag:

    run_sql_transformation = SparkSubmitOperator(
        task_id="run_sql_transformation",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/nrda/run_sql_transformation.py",
        executor_memory="30g",
        executor_cores=9,
        application_args=["{{ dag_run.conf['project_code'] }}", "{{ json.dumps(dag_run.conf['ledger']) }}"],
        verbose=False
    )

    prep_result_legders = PythonOperator(
        task_id="prep_ledger",
        python_callable=prep_result_legders,
        provide_context=True
    )

    send_message = PythonOperator(
        task_id="send_message",
        python_callable=send_success_message,
        provide_context=True,
        on_success_callback=None
    )
    run_sql_transformation >> prep_result_legders >> send_message 