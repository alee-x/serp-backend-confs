import sys
import os
import json
import airflow
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

currentdir = os.path.dirname(os.path.realpath(__file__))
subdir = currentdir+'/scripts/nrda'

sys.path.append(subdir)
import test_delta

default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "jeffrey@chi.swan.ac.uk"
}


with DAG(dag_id="delta_lake_test", schedule_interval=None, default_args=default_args, catchup=False, is_paused_upon_creation=True, max_active_runs=3, user_defined_macros={
    'json': json
}, tags=['skipped']) as dag:


    preliminary_schema_inference = SparkSubmitOperator(
        task_id="test_delta",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/nrda/test_delta.py",
        executor_memory="30g",
        executor_cores=9,
        verbose=False
    )

    
