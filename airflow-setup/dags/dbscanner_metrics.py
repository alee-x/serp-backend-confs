import pyodbc
import ibm_db_sa
import psycopg2
import os
import sys
import json
from sqlalchemy import create_engine, inspect
import pandas as pd
import numpy as np
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

currentdir = os.path.dirname(os.path.realpath(__file__))
subdir = currentdir+'/scripts/dbscanner'

sys.path.append(subdir)

from connection_setup import connection_setup
from metrics import get_metrics
from rabbitmq_service import send_result, send_failure


def prep_result(**context):
    metrics = context['ti'].xcom_pull(key='metrics_result')

    result = metrics

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

with DAG(dag_id="dbscanner_metrics_pipeline", schedule_interval=None, default_args=default_args, catchup=False, max_active_runs=3, user_defined_macros={
    'json': json
}, tags=['working']) as dag:

    connection_setup = PythonOperator(
        task_id='connection_setup',
        python_callable=connection_setup,
        provide_context=True
    )

    get_metrics = PythonOperator(
        task_id='get_metrics',
        python_callable=get_metrics,
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

    connection_setup >> get_metrics >> prep_result >> send_result
