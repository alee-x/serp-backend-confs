import sys
import os
import json
import airflow
import glob
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from subprocess import PIPE, Popen
from airflow.hooks.base_hook import BaseHook

# Would be cleaner to add the path to the PYTHONPATH variable
currentdir = os.path.dirname(os.path.realpath(__file__))
subdir = currentdir+'/scripts/nrda'

sys.path.append(subdir)

from run_sql_postgres import run_sql_postgres

from rabbitmq_notifier import send_progress_message, send_failure_message, send_success_message

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

pg_conn = BaseHook.get_connection('postgres_test')

with DAG(dag_id="nrda_run_sql_postgres", schedule_interval=None, default_args=default_args, catchup=False, max_active_runs=1, user_defined_macros={'json': json}) as dag:

    run_sql_postgres = PythonOperator(
        task_id="run_sql_postgres",
        python_callable=run_sql_postgres,
        provide_context=True,
        pool='postgres_run_pool',
        op_kwargs={"error_msg_dir": "/tmp",
                   "output_msg_dir": "/tmp",
                   "pg_login":pg_conn.login,
                   "pg_password":pg_conn.password,
                   "pg_server": pg_conn.host,
                   "pg_port": pg_conn.port,
                   "pg_database": "data_load_test_rob"}
    )

    send_message = PythonOperator(
        task_id="send_message",
        python_callable=send_success_message,
        provide_context=True,
        on_success_callback=None
    )

    run_sql_postgres >> send_message 
