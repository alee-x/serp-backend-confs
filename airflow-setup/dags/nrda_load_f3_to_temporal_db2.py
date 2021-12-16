import sys
import os
import json
import airflow
import glob
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from subprocess import PIPE, Popen
from airflow.hooks.base_hook import BaseHook

# Would be cleaner to add the path to the PYTHONPATH variable
currentdir = os.path.dirname(os.path.realpath(__file__))
subdir = currentdir+'/scripts/nrda'

sys.path.append(subdir)

from run_sql_db2 import run_sql_db2
from data_loading.db2.gen_base_temporal_f3 import gen_base_temporal_f3

from rabbitmq_notifier import send_progress_message, send_failure_message, send_success_message


def prep_result_legders(**context):
    souce_ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']   
    version = souce_ledger["version"]
    classification = souce_ledger["classification"]
    label = souce_ledger["label"]
    target_label = souce_ledger["attributes"]["target_label_name"].format(label = label, classification = classification, version = version)
   
    
    result_legders = [{
        "classification": souce_ledger["attributes"]["target_classification"],
        "label": target_label,
        "version": version,
        "group_count": souce_ledger["group_count"],
        "location": "Database",
        "location_details": target_label,
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

db2_conn = BaseHook.get_connection('db2_prsail_conn')

with DAG(dag_id="nrda_load_f3_to_temporal_db2", schedule_interval=None, default_args=default_args, catchup=False, max_active_runs=1, user_defined_macros={'json': json}) as dag:

    gen_base_temporal_f3 = PythonOperator(
        task_id="gen_base_temporal_f3",
        python_callable=gen_base_temporal_f3,
        provide_context=True,
        pool='db2_run_pool'
    )

    create_and_load_f3 = PythonOperator(
        task_id="create_and_load_f3",
        python_callable=run_sql_db2,
        provide_context=True,
        pool='db2_run_pool',
        op_kwargs={"error_msg_dir": "/tmp",
                   "output_msg_dir": "/tmp",
                   "db2_login":db2_conn.login,
                   "db2_password":db2_conn.password}
    )

    prep_result = PythonOperator(
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

    gen_base_temporal_f3 >> create_and_load_f3  >> prep_result >> send_message 
