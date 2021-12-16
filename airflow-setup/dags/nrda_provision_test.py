import sys
import os
import json
import airflow
import glob
import requests
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from subprocess import PIPE, Popen
from json import JSONEncoder
from airflow.hooks.base_hook import BaseHook

# Would be cleaner to add the path to the PYTHONPATH variable
currentdir = os.path.dirname(os.path.realpath(__file__))
subdir = currentdir+'/scripts/nrda'

sys.path.append(subdir)

def get_sql_template_list(**context):
    # - call backend api to retrieve sql template list
    airflow_oauth2_conn = BaseHook.get_connection('airflow_oauth2')
    nrda_backend_api_conn = BaseHook.get_connection('nrdav2_backend_api')

    token_url = airflow_oauth2_conn.host
    # - client (application) credentials on keycloak
    client_id = airflow_oauth2_conn.login
    client_secret = airflow_oauth2_conn.password
    # - single call with client credentials as the basic auth header - will return access_token
    data = {'grant_type': 'client_credentials'}
    access_token_response = requests.post(token_url, data=data, verify=False, allow_redirects=False, auth=(client_id, client_secret))
    
    print(access_token_response)
    tokens = json.loads(access_token_response.text)
    print("access token: " + tokens['access_token'])

    dataset_id = 0
    if len(context['dag_run'].conf['ledgers'])>0:
            dataset_id = context['dag_run'].conf['ledgers'][0]['dataset_id']
    print("dataset id: " + str(dataset_id))
    schema_get_api_url = "{0}/api/Datasets/{1}/FlattenedQueries".format(nrda_backend_api_conn.host,dataset_id)

    api_call_headers = {'Authorization': 'Bearer ' + tokens['access_token']}
    api_call_response = requests.get(schema_get_api_url, headers=api_call_headers, verify=True)
    print(api_call_response)
    sql_template_list = api_call_response.json()

    print("template list:" + json.dumps(sql_template_list))
    #mock: template list from datasets query api
    ##mock_sql_template_list = [{"source_classification":"F2-DB","source_label":"df_patients","provision_sql_query":"select * from {project_db}.base_{label}_{version} limit 10","provision_platform":"staging","is_view":"false"},
    ##                          {"source_classification":"F2-DB","source_label":"df_covid_test","provision_sql_query":"select * from {project_db}.base_{label}_{version} limit 10","provision_platform":"staging","is_view":"true"}]
    
    
    #todo: call template api to get template list
    # mock_sql_template_list = [{"source_classification":"F2-DB","source_label":"df_patients","provision_sql_query":"select `[queryTable1]`.`id`, `[queryTable1]`.`is_pregnant`, `[queryTable1]`.`race_is_us_asian`, `[queryTable2]`.`invited_to_test`, `[queryTable2]`.`trained_worker`, `[queryTable2]`.`location` from `[queryTable1]` inner join `[queryTable2]` on `[queryTable1]`.`id` = `[queryTable2]`.`patient_id` limit 10","provisioning_tabel_names":{"userTable1":"df_patients","userTable2":"df_covid_test"},"provision_platform":"staging"}]
    ledgers = context['dag_run'].conf['ledgers']
    project_code = context['dag_run'].conf['project_code']
    platform = ledgers[0]["attributes"]["provision_platform"].lower()
    print("Provision platform: " + platform)

    filtered_template_list = []
    for souce_ledger in ledgers:
         for template in sql_template_list:
             if template["source_classification"].lower() == souce_ledger["classification"].lower() and template["source_label"].lower() == souce_ledger["label"].lower() and template["provision_platform"].lower() == souce_ledger["attributes"]["provision_platform"].lower():
                 filtered_template_list.append(template)
                 print("template to apply: " + json.dumps(template) )
    
    ledger_json_file_name = os.path.join(os.sep, "tmp", "ledgers_{0}_for_provision.json".format(context['dag_run'].run_id))
    with open(ledger_json_file_name, 'w') as fp:
        json.dump(ledgers, fp)
    print("ledger json file name: {0}".format(ledger_json_file_name))
    context['ti'].xcom_push(key='ledger_json_file_name', value=ledger_json_file_name)
    # for souce_ledger in ledgers:
    #      for template in sql_template_list:
    #          if template["source_classification"] == souce_ledger["classification"] and template["source_label"] == souce_ledger["label"] and template["provision_platform"] == souce_ledger["attributes"]["provision_platform"]:
    #             #  filtered_template_list_test.append(template)
    #              print("template_test to apply: " + json.dumps(template) )

    context['ti'].xcom_push(key='filtered_template_list', value=filtered_template_list)

    if platform == "staging":
        return "run_for_provision_on_staging"
    elif platform == "db2":
        return "run_provision_on_db2"
    elif platform == "mssql":
        return "run_provision_on_mssql"
    elif platform == "postgres":
        return "run_provision_on_postgres"
    else:
        return "platform_is_invalid"

def platform_is_invalid(**context):
    raise Exception("Platform is not valid")    

from rabbitmq_notifier import send_progress_message, send_failure_message, send_success_message

def prep_result_legders(**context):
    ledgers = context['dag_run'].conf['ledgers']
    project_code = context['dag_run'].conf['project_code']   
    filtered_template_list = context['ti'].xcom_pull(key='filtered_template_list')
    result_legders = []

    for template in filtered_template_list:
        source_ledger = next((l for l in ledgers if l['label'].lower() == template["source_label"].lower() and l['classification'].lower() == template["source_classification"].lower()), None)
        file_location = source_ledger["location_details"]
        version = source_ledger["version"]
        classification = source_ledger["classification"]
        label = source_ledger["label"]
        target_label = source_ledger["attributes"]["target_label_name"].format(label = label, classification = classification, version = version)
        provisioned_file_hdfs_path = os.path.join(os.sep, "stage", project_code,"provisioned","{0}.parquet".format(target_label))
        if source_ledger["attributes"]["is_provisioned_view"].lower() == "true":
            provisioned_file_hdfs_path = "nrda_{0}_{1}".format(project_code.lower(),target_label)
        result_ledger = {
            "classification": source_ledger["attributes"]["target_classification"],
            "label": target_label,
            "version": version,
            "group_count": source_ledger["group_count"],
            "location": "HDFS-DB",
            "location_details": provisioned_file_hdfs_path,
            "attributes":{
                "parquet_file_path": os.path.join(os.sep, "stage", project_code,"provisioned","{0}.parquet".format(target_label))
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
with DAG(dag_id="nrda_provision_test", schedule_interval=None, default_args=default_args, catchup=False, max_active_runs=3, user_defined_macros={
    'json': json
}) as dag:

    get_sql_template_list = BranchPythonOperator(
        task_id="get_sql_template_list",
        python_callable=get_sql_template_list,
        provide_context=True
    )
    platform_is_invalid = PythonOperator(
        task_id='platform_is_invalid',
        python_callable=platform_is_invalid,
        provide_context=True
    )
    run_for_provision_on_staging = SparkSubmitOperator(
        task_id="run_for_provision_on_staging",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/nrda/provisioning/run_for_provision_on_staging_new.py",
        executor_memory="30g",
        executor_cores=4,
        application_args=[
                          "{{ dag_run.conf['project_code'] }}",
                          "{{ ti.xcom_pull(key='ledger_json_file_name') }}",
                          "{{ json.dumps(ti.xcom_pull(key='filtered_template_list') )}}"]
    )
   
    run_provision_on_db2 = DummyOperator(task_id='run_provision_on_db2')    
    run_provision_on_mssql = DummyOperator(task_id='run_provision_on_mssql')    
    run_provision_on_postgres = DummyOperator(task_id='run_provision_on_postgres')
    
    prep_result_legders = PythonOperator(
        task_id="prep_ledger",
        python_callable=prep_result_legders,
        provide_context=True,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    send_message = PythonOperator(
        task_id="send_message",
        python_callable=send_success_message,
        provide_context=True,
        on_success_callback=None
    )
    get_sql_template_list >> [platform_is_invalid,run_for_provision_on_staging,run_provision_on_db2,run_provision_on_mssql,run_provision_on_postgres] >> prep_result_legders >> send_message
    #get_sql_template_list >> [platform_is_invalid,run_for_provision_on_staging]