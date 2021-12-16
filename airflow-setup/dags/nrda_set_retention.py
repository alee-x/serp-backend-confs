import sys
import os
import json
import airflow
import requests
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from subprocess import PIPE, Popen

# Would be cleaner to add the path to the PYTHONPATH variable
currentdir = os.path.dirname(os.path.realpath(__file__))
subdir = currentdir+'/scripts/nrda'

sys.path.append(subdir)

from rabbitmq_notifier import send_progress_message, send_failure_message, send_success_message

def submit_retention_to_apply(**context):

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
    print("ledger: " + json.dumps(ledger))

    set_retention_api_url = "{0}/api/Datasets/SetRetention/{1}".format(nrda_backend_api_conn.host, ledger['id'])
    print("nrda backend api host: " + set_retention_api_url)
    request_payload = {'retention_value':ledger["attributes"]["retentionvalue"], 'retention_unit':ledger["attributes"]["retentionunit"]}
    print("Request payload: " + json.dumps(request_payload))
    api_call_headers = {'Authorization': 'Bearer ' + tokens['access_token']}
    api_call_response = requests.post(set_retention_api_url, json=request_payload, headers=api_call_headers, verify=False)
    print("Response status:" + str(api_call_response.status_code))

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


with DAG(dag_id="nrda_set_retention", schedule_interval=None, default_args=default_args, catchup=False, user_defined_macros={
    'json': json
}) as dag:

    submit_retention_to_apply = PythonOperator(
        task_id="submit_retention_to_apply",
        python_callable=submit_retention_to_apply,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=1)
    )


    send_message = PythonOperator(
        task_id="send_message",
        python_callable=send_success_message,
        provide_context=True,
        on_success_callback=None
    )

    submit_retention_to_apply >> send_message