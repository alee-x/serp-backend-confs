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

from create_views import create_views

from rabbitmq_notifier import send_progress_message, send_failure_message, send_success_message

def prep_result_legders(**context):
    ledgers = context['dag_run'].conf['ledgers']
    project_code = context['dag_run'].conf['project_code']    
    sail_schema = "sail{0}v".format(project_code.lower())
    result_legders = []
    for souce_ledger in ledgers:
        label = souce_ledger["label"]
        classification = souce_ledger["classification"]
        version = souce_ledger["version"]
        view_name = souce_ledger["attributes"]["targettablename"].format(label = label, classification = classification, version = version)
        full_view_name = sail_schema + "." + view_name
        result_ledger = {
            "classification": souce_ledger["attributes"]["targetclassification"],
            "label": label,
            "version": version,
            "group_count": souce_ledger["group_count"],
            "location": "Database",
            "location_details": full_view_name,
            "attributes":{
                "base_table": souce_ledger["location_details"]
            }
        }
        result_legders.append(result_ledger)

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

db2_conn = BaseHook.get_connection('db2_prsail_conn')

with DAG(dag_id="nrda_create_db2_view_pipeline", schedule_interval=None, default_args=default_args, catchup=False, max_active_runs=1, user_defined_macros={
    'json': json
}, tags=['working']) as dag:

    create_views = PythonOperator(
        task_id="create_views",
        python_callable=create_views,
        provide_context=True,
        pool='db2_run_pool',
        op_kwargs={"error_msg_dir": "/tmp",
                   "output_msg_dir": "/tmp",
                   "db2_login":db2_conn.login,
                   "db2_password":db2_conn.password}
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
    create_views >> prep_result_legders >> send_message 