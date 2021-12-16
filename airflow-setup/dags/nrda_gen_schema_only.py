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


def submit_schema_metrics(**context):

    airflow_oauth2_conn = BaseHook.get_connection('airflow_oauth2')
    nrda_backend_api_conn = BaseHook.get_connection('nrdav2_backend_api')

    token_url = airflow_oauth2_conn.host
    #client (application) credentials on keycloak
    client_id = airflow_oauth2_conn.login
    client_secret = airflow_oauth2_conn.password
    #step A, B - single call with client credentials as the basic auth header - will return access_token
    data = {'grant_type': 'client_credentials'}
    access_token_response = requests.post(token_url, data=data, verify=False, allow_redirects=False, auth=(client_id, client_secret))
    tokens = json.loads(access_token_response.text)
    print("access token: " + tokens['access_token'])

    ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']
    asset_name = ledger["label"]
    if "master_schema" in ledger["attributes"]:
        print("schema of {0} will be overwriten by {1}".format(ledger["label"],ledger["attributes"]["master_schema"]))
        asset_name = ledger["attributes"]["master_schema"]

    schema_check_api_url = "{0}/api/Schemas/{1}/CompareSchema".format(nrda_backend_api_conn.host, ledger['dataset_id'])
    schema = json.loads(open(os.path.join(os.sep, "tmp", "schema_{0}_{1}_{2}.json".format(project_code,ledger["version"],ledger["label"])), "r").read())
    char_dq_list = json.loads(open(os.path.join(os.sep, "tmp", "char_dq_{0}_{1}_{2}.json".format(project_code, ledger["version"], ledger["label"])), "r").read())
    num_dq_list = json.loads(open(os.path.join(os.sep, "tmp", "num_dq_{0}_{1}_{2}.json".format(project_code, ledger["version"], ledger["label"])), "r").read())
    dt_dq_list = json.loads(open(os.path.join(os.sep, "tmp", "dt_dq_{0}_{1}_{2}.json".format(project_code, ledger["version"], ledger["label"])), "r").read())

    request_payload = {'version': ledger["version"], 'asset_name': asset_name, 'schema_file': schema,
                       'metrics_char_results': char_dq_list, 'metrics_dt_results': dt_dq_list, 'metrics_num_results': num_dq_list,
                       'has_header':ledger["attributes"]["has_header"], 'delimiter':ledger["attributes"]["delimiter"],'charset':ledger["attributes"]["charset"],
                       'date_format':ledger["attributes"]["date_format"], 'timestamp_format':ledger["attributes"]["timestamp_format"]}
    print("Request payload: " + json.dumps(request_payload))
    
    api_call_headers = {'Authorization': 'Bearer ' + tokens['access_token']}
    api_call_response = requests.post(schema_check_api_url, json=request_payload, headers=api_call_headers, verify=False)
    print(api_call_response)
    metadata = api_call_response.json()
    print(metadata)

    if 'schema_valid' not in metadata:
        raise ValueError("Error response from Schema Compare")

    context['ti'].xcom_push(
        key='schema_valid', value=metadata['schema_valid'])
    if not metadata['schema_valid'] and metadata['column_feedback_list']:
        context['ti'].xcom_push(key='column_feedback_list', value=metadata['column_feedback_list'])

def is_schema_valid(**context):

    is_schema_valid = context['ti'].xcom_pull(key='schema_valid')
    if(is_schema_valid):
        return "send_message"
    else:
        return "schema_is_invalid"

def schema_is_invalid(**context):
    house_keeping(**context)
    column_feedback_list = context['ti'].xcom_pull(key='column_feedback_list')
    reject_list = [d for d in column_feedback_list if d['result'].lower() == 'reject']
    raise Exception("Computed schema is not valid. Reason: {0}".format(json.dumps(reject_list)))


def house_keeping(**context):    
    souce_ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']
    version = souce_ledger["version"]
    label = souce_ledger["label"]

    schemafile_local = os.path.join(os.sep, "tmp", "schema_{0}_{1}_{2}.json".format(project_code,version,label))
    if os.path.exists(schemafile_local): os.remove(schemafile_local)

    local_char_dq_file = os.path.join(os.sep, "tmp", "char_dq_{0}_{1}_{2}.json".format(project_code, version, label))
    if os.path.exists(local_char_dq_file): os.remove(local_char_dq_file)

    local_num_dq_file = os.path.join(os.sep, "tmp", "num_dq_{0}_{1}_{2}.json".format(project_code,version,label))
    if os.path.exists(local_num_dq_file): os.remove(local_num_dq_file)

    local_dt_dq_file = os.path.join(os.sep, "tmp", "dt_dq_{0}_{1}_{2}.json".format(project_code,version,label))
    if os.path.exists(local_dt_dq_file): os.remove(local_dt_dq_file)


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


with DAG(dag_id="nrda_gen_schema_only", schedule_interval=None, default_args=default_args, catchup=False, max_active_runs=3, user_defined_macros={
    'json': json
}) as dag:


    preliminary_schema_inference = SparkSubmitOperator(
        task_id="preliminary_schema_inference",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/nrda/gen_schema.py",
        executor_memory="30g",
        executor_cores=9,
        application_args=["tmp", "{{ dag_run.conf['project_code'] }}", "{{ json.dumps(dag_run.conf['ledger']) }}"],
        verbose=False
    )

    preliminary_char_metrics = SparkSubmitOperator(
        task_id="preliminary_char_metrics",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/nrda/run_char_metrics.py",
        executor_memory="30g",
        executor_cores=9,
        application_args=["tmp", "{{ dag_run.conf['project_code'] }}", "{{ json.dumps(dag_run.conf['ledger']) }}", ""],
        verbose=True
    )

    preliminary_dt_metrics = SparkSubmitOperator(
        task_id="preliminary_dt_metrics",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/nrda/run_dt_metrics.py",
        executor_memory="30g",
        executor_cores=9,
        application_args=["tmp", "{{ dag_run.conf['project_code'] }}", "{{ json.dumps(dag_run.conf['ledger']) }}", ""],
        verbose=False
    )

    preliminary_num_metrics = SparkSubmitOperator(
        task_id="preliminary_num_metrics",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/nrda/run_num_metrics.py",
        executor_memory="30g",
        executor_cores=9,
        application_args=["tmp", "{{ dag_run.conf['project_code'] }}", "{{ json.dumps(dag_run.conf['ledger']) }}", ""],
        verbose=False
    )

    submit_schema_metrics = PythonOperator(
        task_id="submit_schema_metrics",
        python_callable=submit_schema_metrics,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=1)
    )

    check_is_schema_valid = BranchPythonOperator(
        task_id='check_is_schema_valid',
        python_callable=is_schema_valid,
        provide_context=True)

    schema_is_invalid = PythonOperator(
        task_id='schema_is_invalid',
        python_callable=schema_is_invalid,
        provide_context=True
    )

    house_keeping = PythonOperator(
        task_id='house_keeping', 
        python_callable=house_keeping, 
        provide_context=True)

    send_message = PythonOperator(
        task_id="send_message",
        python_callable=send_success_message,
        provide_context=True,
        on_success_callback=None
    )

    # hello_world_printer >> preliminary_schema_inference >> [preliminary_char_metrics, preliminary_dt_metrics, preliminary_num_metrics] >> end_job >> send_message
    preliminary_schema_inference >> [preliminary_char_metrics, preliminary_dt_metrics, preliminary_num_metrics] >> submit_schema_metrics
    submit_schema_metrics >> check_is_schema_valid
    check_is_schema_valid >> house_keeping >> send_message
    check_is_schema_valid >> schema_is_invalid 
    #metrics_job_end >> send_message
