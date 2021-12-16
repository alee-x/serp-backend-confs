import sys
import os
import json
import airflow
import requests
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from subprocess import PIPE, Popen

# Would be cleaner to add the path to the PYTHONPATH variable
currentdir = os.path.dirname(os.path.realpath(__file__))
subdir = currentdir+'/scripts/nrda'

sys.path.append(subdir)

from rabbitmq_notifier import send_progress_message, send_failure_message, send_success_message
# def print_hello(**context):
#     task_params = context['dag_run'].conf['project_code']
#     print('Hello world a with {}'.format(task_params))
#     print('location_details {}'.format(
#         context['dag_run'].conf['ledger']['location_details']))
#     if(context['dag_run'].conf['metadata']['schema_valid']):
#         print('schema is valid')
#     if not context['dag_run'].conf['metadata']['schema_cols_to_rename']:
#         print('schema_cols_to_rename is empty')
#     if not context['dag_run'].conf['metadata']['schema_cols_to_exclude']:
#         print('schema_cols_to_exclude is empty')



default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "jeffrey@chi.swan.ac.uk"
    # "retries": 1,
    # "retry_delay": timedelta(minutes=20),

}


with DAG(dag_id="nrda_staging_enc_test", schedule_interval=None, default_args=default_args, catchup=False, max_active_runs=1, user_defined_macros={
    'json': json
}) as dag:


    apply_encryption = SparkSubmitOperator(
        task_id="apply_encryption",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/nrda/encryption.py",
        conf={'spark.driver.maxResultSize' : '0'},
        verbose=False
    )

    
