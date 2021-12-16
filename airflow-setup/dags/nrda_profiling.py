import sys
import os
import json
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule

#Would be cleaner to add the path to the PYTHONPATH variable
currentdir = os.path.dirname(os.path.realpath(__file__))
subdir = currentdir+'/scripts/nrda'

sys.path.append(subdir)

from rabbitmq_notifier import send_progress_message, send_failure_message, send_success_message

def store_ledger_to_file(**context):
    source_ledger = context['dag_run'].conf['ledger']
    ledger_json_file_name = os.path.join(os.sep, "tmp", "profiling_ledger_{0}.json".format(context['dag_run'].run_id))
    with open(ledger_json_file_name, 'w') as fp:
        json.dump(source_ledger, fp)
    print("ledger json file name: {0}".format(ledger_json_file_name))
    context['ti'].xcom_push(key='ledger_json_file_name', value=ledger_json_file_name)


def get_result_files(**context):
    html_result_path = os.path.join(os.sep, "tmp", "profiling_report_{0}.html".format(context['dag_run'].run_id))
    with open(html_result_path) as infile:
        html_result= infile.read()
    print(html_result)
    context['ti'].xcom_push(key='html_result_path',value=html_result_path)
    context['ti'].xcom_push(key='html_result',value=html_result)


def housekeeping(**context):
    ledger_json_file_name = context['ti'].xcom_pull(key='ledger_json_file_name')
    os.remove(ledger_json_file_name)
    print("ledger json file {0} has been deleted".format(ledger_json_file_name))
    html_result_path = context['ti'].xcom_pull(key='html_result_path')
    
    if os.path.exists(html_result_path): os.remove(html_result_path)
    print("report html file {0} has been deleted".format(html_result_path))


def prep_result_ledgers(**context):
    source_ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']

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
            "profiling_html_result": html_result
        }
    }]

    if 'schema' in source_ledger['attributes']:
        result_legders[0]['attributes']['schema'] = source_ledger['attributes']['schema']

    if 'metrics' in source_ledger['attributes']:
        result_legders[0]['attributes']['metrics'] = source_ledger['attributes']['metrics']

    print(json.dumps(result_legders))
    context['ti'].xcom_push(key='result_legders', value=result_legders)


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

with DAG(dag_id="nrda_profiling_pipeline", schedule_interval=None, default_args=default_args, catchup=False, max_active_runs=3, user_defined_macros={
    'json': json
}) as dag:

    store_ledger_to_file = PythonOperator(
        task_id='store_ledger_to_file', 
        python_callable=store_ledger_to_file, 
        provide_context=True
    )

    profiling = SparkSubmitOperator(
        task_id="profiling",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/nrda/profiling.py",
        executor_memory="30g",
        executor_cores=9,
        application_args=["tmp", "{{ dag_run.conf['project_code'] }}", "{{ ti.xcom_pull(key='ledger_json_file_name') }}", "{{ dag_run.run_id }}"],
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
        provide_context=True,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    housekeeping = PythonOperator(
        task_id='housekeeping', 
        python_callable=housekeeping, 
        provide_context=True
    )

    send_message = PythonOperator(
        task_id="send_message",
        python_callable=send_success_message,
        provide_context=True,
        on_success_callback=None
    )

    store_ledger_to_file >> profiling >> get_result_files >> prep_result_ledgers >> housekeeping >> send_message