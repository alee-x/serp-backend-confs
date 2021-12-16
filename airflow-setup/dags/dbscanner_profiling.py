import sys
import os
import json
from sqlalchemy import create_engine
import pandas as pd
from pandas_profiling import ProfileReport
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

currentdir = os.path.dirname(os.path.realpath(__file__))
subdir = currentdir+'/scripts/dbscanner'

sys.path.append(subdir)

from connection_setup import connection_setup
from custom_exceptions import NoSuchTableException, NoSuchSchemaException
from rabbitmq_service import send_result, send_failure


def profiling(**context):
    db_platform = context['dag_run'].conf['db_platform']
    db_name = context['dag_run'].conf['database_name']
    schema_name = context['dag_run'].conf['schema_name']
    table_name = context['dag_run'].conf['table_name']
    minimal = context['dag_run'].conf['minimal']
    explorative = context['dag_run'].conf['explorative']

    print("starting profiling for {0}.{1}.{2}.{3}".format(db_platform,db_name,schema_name,table_name))
    print("minimal: {0}".format(minimal))
    print("explorative: {0}".format(explorative))

    connection_str = context['ti'].xcom_pull(key='connection_str')
    engine = create_engine(connection_str)

    df = pd.read_sql('SELECT * FROM "{0}"."{1}"'.format(schema_name,table_name), engine)
    df.infer_objects()

    profile = ProfileReport(df, title='Pandas Profiling Report', explorative=explorative, minimal=minimal)
    
    html_report_file_path = os.path.join(os.sep, "tmp", "{0}_{1}_{2}_{3}.html".format(db_platform,db_name,schema_name,table_name))
    profile.to_file(html_report_file_path)

    print("html report saved at: {0}".format(html_report_file_path))
    context['ti'].xcom_push(key='html_report_file_path',value=html_report_file_path)

    json_report_file_path = os.path.join(os.sep, "tmp", "{0}_{1}_{2}_{3}.json".format(db_platform,db_name,schema_name,table_name))
    json_data = profile.to_json()
    profile.to_file(json_report_file_path)
    
    print("json report saved at: {0}".format(json_report_file_path))
    context['ti'].xcom_push(key='json_report_file_path',value=json_report_file_path)


def get_result(**context):
    html_report_file_path = context['ti'].xcom_pull(key='html_report_file_path')
    with open(html_report_file_path) as infile:
        html_result = infile.read()
    print(html_result)
    os.remove(html_report_file_path)
    print("html report removed from drive")
    context['ti'].xcom_push(key='html_result',value=html_result)

    json_report_file_path = context['ti'].xcom_pull(key='json_report_file_path')
    with open(json_report_file_path) as infile:
        json_result = infile.read()
    print(json_result)
    os.remove(json_report_file_path)
    print("json report removed from drive")
    context['ti'].xcom_push(key='json_result',value=json_result)


def prep_result(**context):
    html_result = context['ti'].xcom_pull(key='html_result')
    json_result = context['ti'].xcom_pull(key='json_result')

    result = {
        'db_path_object_id': context['dag_run'].conf['db_path_object_id'],
        'profiling_report_html' : html_result,
        'profiling_report_json': json_result
    }

    context['ti'].xcom_push(key='result',value=result)


default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "jeffrey@chi.swan.ac.uk",
    "on_failure_callback": send_failure
}

with DAG(dag_id="dbscanner_profiling_pipeline", schedule_interval=None, default_args=default_args, catchup=False, max_active_runs=1, user_defined_macros={
    'json': json
}, tags=['working']) as dag:

    connection_setup = PythonOperator(
        task_id='connection_setup',
        python_callable=connection_setup,
        provide_context=True
    )

    profiling = PythonOperator(
        task_id='profiling',
        python_callable=profiling,
        provide_context=True
    )

    get_result = PythonOperator(
        task_id='get_result',
        python_callable=get_result,
        provide_context=True
    )

    prep_result = PythonOperator(
        task_id='prep_result',
        python_callable=prep_result,
        provide_context=True
    )

    send_result = PythonOperator(
        task_id='send_result',
        python_callable=send_result,
        provide_context=True
    )

    connection_setup >> profiling >> get_result >> prep_result >> send_result