import os
import sys
import json
import requests
import glob
import airflow
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook

#Would be cleaner to add the path to the PYTHONPATH variable
currentdir = os.path.dirname(os.path.realpath(__file__))
subdir = currentdir+'/scripts/nrda'

sys.path.append(subdir)

from rabbitmq_notifier import send_progress_message, send_failure_message, send_success_message


def get_expectations(**context):
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
    print(tokens)
    print("access token: " + tokens['access_token'])

    project_code = context['dag_run'].conf['project_code']
    ledger = context['dag_run'].conf['ledger']

    print("LEDGER")
    print(ledger['attributes'])

    get_expectation_url = "{0}/api/Expectation/Suite/{1}/{2}/{3}".format(nrda_backend_api_conn.host,ledger['dataset_id'],ledger['label'],ledger['classification'])
    print("get expectations backend url: " + get_expectation_url)

    api_call_headers = {'Authorization': 'Bearer ' + tokens['access_token']}
    api_call_response = requests.get(get_expectation_url, headers=api_call_headers, verify=False)
    print("response status code: {0}".format(api_call_response.status_code))
    if api_call_response.status_code == 204:
        raise ValueError("No expectation suite found for that asset name / classification")
    res = api_call_response.json()
    print(res)

    context['ti'].xcom_push(key='expectations',value=res['expectations'])


def add_expectations_to_suite(**context):
    suite_path = "/tmp/great_expectations/{0}/expectations/{0}.json".format(context['dag_run'].run_id)
    with open(suite_path) as infile:
        suite = json.load(infile)

    expectations = context['ti'].xcom_pull(key='expectations')
    suite['expectations'] = expectations

    with open(suite_path,'w') as outfile: 
        json.dump(suite,outfile)


def get_result_files(**context):
    run_id = context['dag_run'].run_id
    dir_path = "/tmp/great_expectations/{0}/".format(run_id)
    
    # json result file
    list_of_files = glob.glob("{0}/uncommitted/validations/{1}/{1}/*".format(dir_path,run_id))
    validation_path = max(list_of_files,key=os.path.getctime)
    json_result_path = glob.glob("{0}/*".format(validation_path))[0]

    with open(json_result_path) as infile:
        json_result = infile.read()
    print(json_result)
    context['ti'].xcom_push(key='json_result',value=json_result)
    
    # html result file
    list_of_files = glob.glob("{0}/uncommitted/data_docs/local_site/validations/{1}/{1}/*".format(dir_path,run_id))
    validation_path = max(list_of_files,key=os.path.getctime)
    html_result_path = glob.glob("{0}/*".format(validation_path))[0]

    with open(html_result_path) as infile:
        html_result = infile.read()
    print(html_result)
    context['ti'].xcom_push(key='html_result',value=html_result)


def prep_result_ledgers(**context):
    source_ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']

    json_result = context['ti'].xcom_pull(key='json_result')
    html_result = context['ti'].xcom_pull(key='html_result')

    result_legders = [{
        "classification": source_ledger["classification"],
        "label": source_ledger["label"],
        "version": source_ledger["version"],
        "group_count": source_ledger["group_count"],
        "location": source_ledger["location"],
        "location_details": source_ledger["location_details"],
        "attributes":{
            "project": project_code.lower(),
            "expectation_json_result": json_result,
            "expectation_html_result": html_result,
            "FailExpectationAttr": source_ledger['attributes']["failexpectation"]
        }
    }]

    if 'schema' in source_ledger['attributes']:
        result_legders[0]['attributes']['schema'] = source_ledger['attributes']['schema']

    if 'metrics' in source_ledger['attributes']:
        result_legders[0]['attributes']['metrics'] = source_ledger['attributes']['metrics']

    print(json.dumps(result_legders))
    context['ti'].xcom_push(key='result_legders',value=result_legders)
    


default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "jeffrey@chi.swan.ac.uk",
    "on_success_callback": send_progress_message,
    "on_failure_callback": send_failure_message
}

spark_confs = {
    "hive.metastore.uris":"thrift://hive-metastore:9083",
    "hive.server2.thrift.http.path":"cliservice",
    "datanucleus.autoCreateSchema":"false",
    "javax.jdo.option.ConnectionURL":"jdbc:postgresql://hive-metastore-postgresql/metastore",
    "javax.jdo.option.ConnectionDriverName":"org.postgresql.Driver",
    "javax.jdo.option.ConnectionPassword":"hive",
    "hive.server2.transport.mode":"http",
    "hive.server2.thrift.max.worker.threads":"5000",
    "javax.jdo.option.ConnectionUserName":"hive",
    "hive.server2.thrift.http.port":"10000",
    "hive.server2.enable.doAs":"false",
    "hive.metastore.warehouse.dir":"hdfs://namenode:9000/user/hive/warehouse",
    "spark.sql.warehouse.dir":"hdfs://namenode:9000/user/hive/warehouse"
}

with DAG(dag_id="nrda_expectation_pipeline", schedule_interval=None, default_args=default_args, catchup=False, max_active_runs=3, user_defined_macros={
    'json': json
}, tags=['working']) as dag:

    get_expectations = PythonOperator(
        task_id='get_expectations',
        python_callable=get_expectations,
        provide_context=True
    )
    # the 'conf' bit is what we need to get this to work in Docker, remove for push to SERP
    gen_expectation_suite = SparkSubmitOperator(
        task_id="gen_expectation_suite",
        conn_id="spark_conn",
        conf=spark_confs,
        application="/opt/airflow/dags/scripts/nrda/gen_expectation_suite.py",
        executor_memory="3g",
        executor_cores=3,
        application_args=["expectations", "{{ dag_run.conf['project_code'] }}", "{{ dag_run.run_id }}"],
        verbose=False
    )

    add_expectations_to_suite = PythonOperator(
        task_id='add_expectations_to_suite',
        python_callable=add_expectations_to_suite,
        provide_context=True
    )
    # the 'conf' bit is what we need to get this to work in Docker, remove for push to SERP
    run_expectation_suite = SparkSubmitOperator(
        task_id="run_expectation_suite",
        conn_id="spark_conn",
        conf=spark_confs,
        application="/opt/airflow/dags/scripts/nrda/run_expectation_suite.py",
        executor_memory="3g",
        executor_cores=3,
        application_args=["expectations", "{{ dag_run.conf['project_code'] }}", "{{ json.dumps(dag_run.conf['ledger']) }}", "{{ dag_run.run_id }}"],
        verbose=False
    )

    get_result_files = PythonOperator(
        task_id='get_result_files',
        python_callable=get_result_files,
        provide_context=True
    )

    prep_result_ledgers = PythonOperator(
        task_id='prep_result_ledgers',
        python_callable=prep_result_ledgers,
        provide_context=True
    )

    send_message = PythonOperator(
        task_id="send_message",
        python_callable=send_success_message,
        provide_context=True,
        on_success_callback=None
    )

    get_expectations >> gen_expectation_suite >> add_expectations_to_suite >> run_expectation_suite >> get_result_files >> prep_result_ledgers >> send_message