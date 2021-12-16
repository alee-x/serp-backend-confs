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
from ftp_test import ftp_test
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


with DAG(dag_id="nrda_staging_pipeline_test", schedule_interval=None, default_args=default_args, catchup=False, max_active_runs=1, user_defined_macros={
    'json': json
}) as dag:


    # apply_encryption = SparkSubmitOperator(
    #     task_id="debug",
    #     conn_id="spark_conn",
    #     application="/usr/local/airflow/dags/scripts/nrda/debug.py",
    #     conf={'spark.driver.maxResultSize' : '0'},
    #     executor_memory="40g",
    #     executor_cores=6,
    #     verbose=False
    # )

    # apply_sed = BashOperator(
    #     task_id="apply_sed",
    #     bash_command="cat $source_file | grep '\"\"' | head -n 10 | sed 's/\\\\\"//g' > $target_file",
    #     env={'source_file': '/tmp/dataset1_workflow2_ledger98132_b2592a90/df_assessments.csv/part-00000-092f0c08-7912-43be-9535-5b9902d21aa9-c000.csv', 'target_file': '/tmp/test_jeffrey3.csv'},
    # )

    ftp_test = PythonOperator(
        task_id="ftp_test",
        python_callable=ftp_test,
        provide_context=True
    )

    
