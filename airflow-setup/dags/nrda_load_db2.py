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

from data_loading.store_ledger_to_file import store_ledger_to_file
from data_loading.housekeeping import housekeeping
from data_loading.download_csv_to_load import download_csv_to_load
from data_loading.is_post_processing_required import is_post_processing_required
from data_loading.prep_result_legders import prep_result_legders
# from data_loading.sed_content import sed_content

from data_loading.db2.gen_load_table import gen_load_table
from data_loading.db2.run_load_load import run_load_load
from data_loading.db2.run_xref_load import run_xref_load
from data_loading.db2.gen_base_table import gen_base_table
from data_loading.db2.run_base_load import run_base_load
from data_loading.db2.run_base_load_without_postprocess import run_base_load_without_postprocess

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

with DAG(dag_id="nrda_load_db2_pipeline", schedule_interval=None, default_args=default_args, catchup=False, max_active_runs=1, user_defined_macros={
    'json': json
}) as dag:

    store_ledger_to_file = PythonOperator(
        task_id="store_ledger_to_file",
        python_callable=store_ledger_to_file,
        provide_context=True
    )

    extract_csv_to_load = SparkSubmitOperator(
        task_id="extract_csv_to_load",
        conn_id="spark_conn",
        application="/usr/local/airflow/dags/scripts/nrda/extract_csv_to_load.py",
        executor_memory="30g",
        executor_cores=9,
        application_args=["stage", "{{ dag_run.conf['project_code'] }}", "{{ ti.xcom_pull(key='ledger_json_file_name') }}", ",", "db2"],
        verbose=False
    )
    
    download_csv_to_load = PythonOperator(
        task_id="download_csv_to_load",
        python_callable=download_csv_to_load,
        provide_context=True
    )

    # sed_content = PythonOperator(
    #     task_id="sed_content",
    #     python_callable=sed_content,
    #     provide_context=True
    # )

    apply_sed = BashOperator(
        task_id="apply_sed",
        bash_command="cat $source_file | sed 's/\\\\\"//g' > $target_file",
        env={'source_file': '{{ ti.xcom_pull(key="csv_toclean_filename") }}', 'target_file': '{{ ti.xcom_pull(key="csv_toload_filename") }}'},
    )

    is_post_processing_required = BranchPythonOperator(
        task_id='is_post_processing_required',
        python_callable=is_post_processing_required,
        provide_context=True)

    gen_load_table = PythonOperator(
        task_id="gen_load_table",
        python_callable=gen_load_table,
        provide_context=True,
        pool='db2_run_pool'
    )

    run_load_load = PythonOperator(
        task_id="run_load_load",
        python_callable=run_load_load,
        provide_context=True,
        pool='db2_run_pool'
    )
    
    run_xref_load = PythonOperator(
        task_id="run_xref_load",
        python_callable=run_xref_load,
        provide_context=True,
        pool='db2_run_pool'
    )
    
    gen_base_table = PythonOperator(
        task_id="gen_base_table",
        python_callable=gen_base_table,
        provide_context=True,
        pool='db2_run_pool'
    )

    run_base_load_without_postprocess = PythonOperator(
        task_id="run_base_load_without_postprocess",
        python_callable=run_base_load_without_postprocess,
        provide_context=True,
        pool='db2_run_pool'
    )

    run_base_load = PythonOperator(
        task_id="run_base_load",
        python_callable=run_base_load,
        provide_context=True,
        pool='db2_run_pool'
    )

    prep_result_legders = PythonOperator(
        task_id="prep_ledger",
        python_callable=prep_result_legders,
        provide_context=True,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    housekeeping = PythonOperator(
        task_id="housekeeping",
        python_callable=housekeeping,
        provide_context=True
    )

    send_message = PythonOperator(
        task_id="send_message",
        python_callable=send_success_message,
        provide_context=True,
        on_success_callback=None
    )

    store_ledger_to_file >> extract_csv_to_load >> download_csv_to_load >> apply_sed >> gen_load_table >> gen_base_table >> run_load_load >> is_post_processing_required
    is_post_processing_required >> run_xref_load
    is_post_processing_required >> run_base_load_without_postprocess >> prep_result_legders
    run_xref_load >> run_base_load >> prep_result_legders >> housekeeping >> send_message