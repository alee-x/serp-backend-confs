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


def submit_schema_metrics_diff(**context):

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
    print("nrda backend api host: " + schema_check_api_url)
    schema_path = os.path.join(os.sep, "tmp", "schema_{0}_{1}_{2}.json".format(project_code,ledger["version"],ledger["label"]))
    print("schema path: {0}".format(schema_path))
    schema = json.loads(open(schema_path, "r").read())
    char_dq_list = json.loads(open(os.path.join(os.sep, "tmp", "char_dq_{0}_{1}_{2}.json".format(project_code, ledger["version"], ledger["label"])), "r").read())
    num_dq_list = json.loads(open(os.path.join(os.sep, "tmp", "num_dq_{0}_{1}_{2}.json".format(project_code, ledger["version"], ledger["label"])), "r").read())
    dt_dq_list = json.loads(open(os.path.join(os.sep, "tmp", "dt_dq_{0}_{1}_{2}.json".format(project_code, ledger["version"], ledger["label"])), "r").read())

    request_payload = {'version': ledger["version"], 'asset_name': asset_name, 'schema_file': schema,
                       'metrics_char_results': char_dq_list, 'metrics_dt_results': dt_dq_list, 'metrics_num_results': num_dq_list,
                       'has_header':ledger["attributes"]["has_header"], 'delimiter':ledger["attributes"]["delimiter"],'charset':ledger["attributes"]["charset"],
                       'date_format':ledger["attributes"]["date_format"], 'timestamp_format':ledger["attributes"]["timestamp_format"], 'ledger_id':ledger['id']}
    print("Request payload: " + json.dumps(request_payload))
    api_call_headers = {'Authorization': 'Bearer ' + tokens['access_token']}
    api_call_response = requests.post(schema_check_api_url, json=request_payload, headers=api_call_headers, verify=False)
    metadata = api_call_response.json()
    print(metadata)
    
    if 'schema_valid' not in metadata:
        raise ValueError("Unexpected error response when running schema compare.")
    #metadata['schema_valid'] = True  #debug code - need to be removed!

    context['ti'].xcom_push(
        key='schema_valid', value=metadata['schema_valid'])
    if metadata['return_info']['schema_cols_to_rename']:
        context['ti'].xcom_push(key='schema_cols_to_rename', value=metadata['return_info']['schema_cols_to_rename'])
    if metadata['return_info']['schema_cols_to_exclude']:
        context['ti'].xcom_push(key='schema_cols_to_exclude', value=metadata['return_info']['schema_cols_to_exclude'])
    if metadata['return_info']['schema_cols_to_enc']:
        context['ti'].xcom_push(key='schema_cols_to_enc', value=metadata['return_info']['schema_cols_to_enc'])
    if metadata['return_info']['schema_to_apply']:
        # context['ti'].xcom_push(key='schema_to_apply', value=metadata['return_info']['schema_to_apply'])
        cols_type_changed = []
        # rename for compare
        if metadata['return_info']['schema_cols_to_rename']:
            for i in schema["fields"]:
                if i["name"] in metadata['return_info']['schema_cols_to_rename']:
                    print(metadata['return_info']['schema_cols_to_rename'][i["name"]])
                    i["name"] = metadata['return_info']['schema_cols_to_rename'][i["name"]]

        for i in metadata['return_info']['schema_to_apply']["fields"]:
            i['metadata'] = {}
            if i not in schema["fields"]:
                cols_type_changed.append(i)
                print(i)
        if cols_type_changed:
            context['ti'].xcom_push(key='cols_type_changed', value=cols_type_changed)

    if metadata['schema_definition']:
        context['ti'].xcom_push(key='schema_definition', value=metadata['schema_definition'])
     
    if not metadata['schema_valid'] and metadata['column_feedback_list']:
        context['ti'].xcom_push(key='column_feedback_list', value=metadata['column_feedback_list'])
        
        
    # generate target label name for final schema - this is important as the schema will be used by other dags using label as naming convention
    label = ledger["label"]
    version = ledger["version"]
    classification = ledger["classification"]    
    target_label = ledger["attributes"]["target_label_name"].format(label = label, classification = classification, version = version)
    # overwrite the targe label name if destinationTablename is defined in schema def
    if metadata['schema_definition']['destinationTablename']:
        target_label = metadata['schema_definition']['destinationTablename']

    print("target label is {0}".format(target_label))
    
    hdfs_path = os.path.join(os.sep, "stage", project_code,ledger["version"],"final_schema_{0}.json".format(target_label))
    file_name = os.path.join(os.sep, "tmp", "final_schema_{0}_{1}_{2}.json".format(project_code, ledger["version"], target_label))
    with open(file_name, 'w') as fp:
        json.dump(metadata, fp)
    # put csv into hdfs
    put = Popen(["hdfs", "dfs", "-mkdir", "-p", os.path.join(os.sep,"stage", project_code, ledger["version"])], stdin=PIPE, bufsize=-1)
    put.communicate()
    put = Popen(["hdfs", "dfs", "-put", "-f", file_name, hdfs_path], stdin=PIPE, bufsize=-1)
    put.communicate()
    #os.remove(file_name)

    context['ti'].xcom_push(key='target_label', value=target_label)

def is_schema_valid(**context):

    is_schema_valid = context['ti'].xcom_pull(key='schema_valid')
    if(is_schema_valid):
        return "has_transformation_to_apply"
    else:
        return "schema_is_invalid"

def schema_is_invalid(**context):
    house_keeping(**context)
    column_feedback_list = context['ti'].xcom_pull(key='column_feedback_list')
    reject_list = [d for d in column_feedback_list if d['result'].lower() == 'reject']
    raise Exception("Computed schema is not valid. Reason: {0}".format(json.dumps(reject_list)))

def has_transformation_to_apply(**context):

    schema_cols_to_rename = context['ti'].xcom_pull(
        key='schema_cols_to_rename')
    schema_cols_to_exclude = context['ti'].xcom_pull(key='schema_cols_to_exclude')
    schema_cols_to_enc = context['ti'].xcom_pull(key='schema_cols_to_enc')
    cols_type_changed = context['ti'].xcom_pull(key='cols_type_changed')
    if(schema_cols_to_rename or schema_cols_to_exclude or schema_cols_to_enc or cols_type_changed):
        context['ti'].xcom_push(key='transformation_applied', value=True)
        return "apply_transformation"
    else:
        context['ti'].xcom_push(key='transformation_applied', value=False)
        return "no_transformation_applied"

def house_keeping(**context):    
    souce_ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']
    version = souce_ledger["version"]
    label = souce_ledger["label"]
    target_label = context['ti'].xcom_pull(key='target_label')

    schemafile_local = os.path.join(os.sep, "tmp", "schema_{0}_{1}_{2}.json".format(project_code,version,label))
    if os.path.exists(schemafile_local): os.remove(schemafile_local)

    local_char_dq_file = os.path.join(os.sep, "tmp", "char_dq_{0}_{1}_{2}.json".format(project_code, version, label))
    if os.path.exists(local_char_dq_file): os.remove(local_char_dq_file)
    local_char_dq_file = os.path.join(os.sep, "tmp", "char_dq_{0}_{1}_{2}.json".format(project_code,version,target_label))
    if os.path.exists(local_char_dq_file): os.remove(local_char_dq_file)

    local_num_dq_file = os.path.join(os.sep, "tmp", "num_dq_{0}_{1}_{2}.json".format(project_code,version,label))
    if os.path.exists(local_num_dq_file): os.remove(local_num_dq_file)
    local_num_dq_file = os.path.join(os.sep, "tmp", "num_dq_{0}_{1}_{2}.json".format(project_code,version,target_label))
    if os.path.exists(local_num_dq_file): os.remove(local_num_dq_file)

    local_dt_dq_file = os.path.join(os.sep, "tmp", "dt_dq_{0}_{1}_{2}.json".format(project_code,version,label))
    if os.path.exists(local_dt_dq_file): os.remove(local_dt_dq_file)
    local_dt_dq_file = os.path.join(os.sep, "tmp", "dt_dq_{0}_{1}_{2}.json".format(project_code,version,target_label))
    if os.path.exists(local_dt_dq_file): os.remove(local_dt_dq_file)

    local_schema_file = os.path.join(os.sep, "tmp", "final_schema_{0}_{1}_{2}.json".format(project_code,version,target_label))
    if os.path.exists(local_schema_file): os.remove(local_schema_file)

    rowcount_local = os.path.join(os.sep, "tmp", "rowcount_{0}_{1}_{2}.txt".format(project_code,version,target_label))
    if os.path.exists(rowcount_local): os.remove(rowcount_local)

def prep_result_legders(**context):
    souce_ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']
    # generate target label name - this is important as the schema will be used by other dags using label as naming convention
    #label = souce_ledger["label"]
    version = souce_ledger["version"]
    #classification = souce_ledger["classification"]    
    target_label = context['ti'].xcom_pull(key='target_label')
    parquet_stage_hdfs_path = os.path.join(os.sep, "stage", project_code, version,"{0}.parquet".format(target_label))

    hdfs_char_dq_file = os.path.join(os.sep, "stage", project_code, version,"char_dq_{0}.json".format(target_label))
    local_char_dq_file = os.path.join(os.sep, "tmp", "char_dq_{0}_{1}_{2}.json".format(project_code,version,target_label))
    put = Popen(["hdfs", "dfs", "-get", hdfs_char_dq_file, local_char_dq_file], stdin=PIPE, bufsize=-1)
    put.communicate()
    char_dq_str = open(local_char_dq_file, "r").read()

    hdfs_num_dq_file = os.path.join(os.sep, "stage", project_code, version,"num_dq_{0}.json".format(target_label))
    local_num_dq_file = os.path.join(os.sep, "tmp", "num_dq_{0}_{1}_{2}.json".format(project_code,version,target_label))
    put = Popen(["hdfs", "dfs", "-get", hdfs_num_dq_file, local_num_dq_file], stdin=PIPE, bufsize=-1)
    put.communicate()
    num_dq_str = open(local_num_dq_file, "r").read()

    hdfs_dt_dq_file = os.path.join(os.sep, "stage", project_code, version,"dt_dq_{0}.json".format(target_label))
    local_dt_dq_file = os.path.join(os.sep, "tmp", "dt_dq_{0}_{1}_{2}.json".format(project_code,version,target_label))
    put = Popen(["hdfs", "dfs", "-get", hdfs_dt_dq_file, local_dt_dq_file], stdin=PIPE, bufsize=-1)
    put.communicate()
    dt_dq_str = open(local_dt_dq_file, "r").read()
    
    metrics = {"char_metrics": json.loads(char_dq_str),"num_metrics": json.loads(num_dq_str),"dt_metrics":json.loads(dt_dq_str)}

    # get row count file
    rowcount_stage_hdfs_path = os.path.join(os.sep, "stage", project_code, version, "rowcount_{0}.txt".format(target_label))
    rowcount_local = os.path.join(os.sep, "tmp", "rowcount_{0}_{1}_{2}.txt".format(project_code,version,target_label))
    get = Popen(["hdfs", "dfs", "-get", rowcount_stage_hdfs_path, rowcount_local], stdin=PIPE, bufsize=-1)
    get.communicate()
    rowcount_str = open(rowcount_local, "r").read()

    # get final schema file
    hdfs_schema_file = os.path.join(os.sep, "stage", project_code,version,"final_schema_{0}.json".format(target_label))
    local_schema_file = os.path.join(os.sep, "tmp", "final_schema_{0}_{1}_{2}.json".format(project_code,version,target_label))
    put = Popen(["hdfs", "dfs", "-get", hdfs_schema_file, local_schema_file], stdin=PIPE, bufsize=-1)
    put.communicate()
    schema_str = open(local_schema_file, "r").read()

    result_legders = [{
        "classification": souce_ledger["attributes"]["target_classification"],
        "label": target_label,
        "version": version,
        "group_count": souce_ledger["group_count"],
        "location": "HDFS-DB",
        "location_details": parquet_stage_hdfs_path,
        "row_count": int(rowcount_str),
        "attributes":{
            "metrics": json.dumps(metrics),
            "source_ledger_id":  souce_ledger["id"],
            "schema": json.dumps(json.loads(schema_str))
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


with DAG(dag_id="nrda_staging_pipeline", schedule_interval=None, default_args=default_args, catchup=False, max_active_runs=1, user_defined_macros={
    'json': json
}) as dag:

    # hello_world_printer = PythonOperator(
    #     task_id="hello_world_printer",
    #     python_callable=print_hello,
    #     provide_context=True
    # )

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

    submit_schema_metrics_diff = PythonOperator(
        task_id="submit_schema_metrics_diff",
        python_callable=submit_schema_metrics_diff,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=1)
    )

    check_is_schema_valid = BranchPythonOperator(
        task_id='check_is_schema_valid',
        python_callable=is_schema_valid,
        provide_context=True)

    has_transformation_to_apply = BranchPythonOperator(
        task_id='has_transformation_to_apply',
        python_callable=has_transformation_to_apply,
        provide_context=True)

    apply_transformation = SparkSubmitOperator(
        task_id="apply_transformation",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/nrda/apply_transformation.py",
        executor_memory="10g",
        application_args=["{{ json.dumps(ti.xcom_pull(key='schema_cols_to_rename')) }}",
                          "{{ json.dumps(ti.xcom_pull(key='schema_cols_to_exclude')) }}",
                          "{{ json.dumps(ti.xcom_pull(key='schema_cols_to_enc')) }}",
                          "{{ json.dumps(ti.xcom_pull(key='cols_type_changed')) }}",
                          "{{ dag_run.conf['project_code'] }}",
                          "{{ json.dumps(dag_run.conf['ledger']) }}"],
        verbose=False
    )
     
    no_transformation_applied = DummyOperator(
        task_id='no_transformation_applied'
    )

    
    apply_schema = SparkSubmitOperator(
        task_id="apply_schema",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/nrda/apply_stage_schema.py",
        executor_memory="30g",
        executor_cores=9,
        application_args=["{{ json.dumps(ti.xcom_pull(key='transformation_applied'))  }}",
                          "{{ dag_run.conf['project_code'] }}",
                          "{{ json.dumps(dag_run.conf['ledger']) }}",
                          "{{ ti.xcom_pull(key='target_label') }}"],
                          
        trigger_rule=TriggerRule.ONE_SUCCESS,
        verbose=False
    )

    staging_char_metrics = SparkSubmitOperator(
        task_id="staging_char_metrics",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/nrda/run_char_metrics.py",
        executor_memory="30g",
        executor_cores=9,
        application_args=["stage", "{{ dag_run.conf['project_code'] }}", "{{ json.dumps(dag_run.conf['ledger']) }}", "{{ ti.xcom_pull(key='target_label') }}"],
        verbose=True
    )

    staging_dt_metrics = SparkSubmitOperator(
        task_id="staging_dt_metrics",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/nrda/run_dt_metrics.py",
       executor_memory="30g",
        executor_cores=9,
        application_args=["stage", "{{ dag_run.conf['project_code'] }}", "{{ json.dumps(dag_run.conf['ledger']) }}", "{{ ti.xcom_pull(key='target_label') }}"],
        verbose=False
    )

    staging_num_metrics = SparkSubmitOperator(
        task_id="staging_num_metrics",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/nrda/run_num_metrics.py",
        executor_memory="30g",
        executor_cores=9,
        application_args=["stage", "{{ dag_run.conf['project_code'] }}", "{{ json.dumps(dag_run.conf['ledger']) }}", "{{ ti.xcom_pull(key='target_label') }}"],
        verbose=False
    )

    metrics_job_end = PythonOperator(
        task_id="metrics_job_end",
        python_callable=prep_result_legders,
        provide_context=True
    )

    schema_is_invalid = PythonOperator(
        task_id='schema_is_invalid',
        python_callable=schema_is_invalid,
        provide_context=True
    )
    house_keeping = PythonOperator(
        task_id='house_keeping', 
        python_callable=house_keeping, 
        provide_context=True,
        trigger_rule=TriggerRule.ONE_SUCCESS)

    send_message = PythonOperator(
        task_id="send_message",
        python_callable=send_success_message,
        provide_context=True,
        on_success_callback=None
    )

    # hello_world_printer >> preliminary_schema_inference >> [preliminary_char_metrics, preliminary_dt_metrics, preliminary_num_metrics] >> end_job >> send_message
    preliminary_schema_inference >> [preliminary_char_metrics, preliminary_dt_metrics, preliminary_num_metrics] >> submit_schema_metrics_diff
    submit_schema_metrics_diff >> check_is_schema_valid
    check_is_schema_valid >> has_transformation_to_apply
    check_is_schema_valid >> schema_is_invalid >> house_keeping
    has_transformation_to_apply >> [no_transformation_applied, apply_transformation]  >> apply_schema
    apply_schema >> [staging_char_metrics, staging_dt_metrics, staging_num_metrics] >> metrics_job_end >> house_keeping >> send_message

    #metrics_job_end >> send_message
