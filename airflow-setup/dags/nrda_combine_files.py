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

from rabbitmq_notifier import send_progress_message, send_failure_message, send_success_message

# store payload to file to avoid issue of oversized arg for spark-submit
def store_ledgers_to_file(**context):
    print("job task id: {0}".format(context['dag_run'].conf['jobtask_id']))
    ledgers = context['dag_run'].conf['ledgers']
    project_code = context['dag_run'].conf['project_code']   
    print(project_code)
    print("total ledgers: {0}".format(len(ledgers)))    
    souce_ledger = ledgers[0]
    print("first ledger: {0}".format(souce_ledger))
    version = souce_ledger["version"]
    classification = souce_ledger["classification"]
    target_label = souce_ledger["attributes"]["targettablename"].format(classification = classification, version = version)
    mkdir = Popen(["hdfs", "dfs", "-mkdir", "-p", os.path.join(os.sep,"stage", project_code, version,"combined")], stdin=PIPE, bufsize=-1)
    mkdir.communicate()
    ledgers_json_hdfs_path = os.path.join(os.sep, "stage", project_code, version,"combined","ledgers_{0}.json".format(target_label))
    ledgers_json_file_name = os.path.join(os.sep, "tmp", "ledgers_{0}_{1}_{2}.json".format(project_code, version, target_label))
    with open(ledgers_json_file_name, 'w') as fp:
        json.dump(ledgers, fp)
    put = Popen(["hdfs", "dfs", "-put", "-f", ledgers_json_file_name, ledgers_json_hdfs_path], stdin=PIPE, bufsize=-1)
    put.communicate()
    os.remove(ledgers_json_file_name)
    context['ti'].xcom_push(key='ledgers_json_hdfs_path', value=ledgers_json_hdfs_path)

def housekeeping(**context):    
    ledgers_json_hdfs_path = context['ti'].xcom_pull(key='ledgers_json_hdfs_path') 
    put = Popen(["hdfs", "dfs", "-rm", "-f", ledgers_json_hdfs_path], stdin=PIPE, bufsize=-1)
    put.communicate()
    print("ledger json file {0} has been deleted".format(ledgers_json_hdfs_path))
    

# return 1 result ledger
def prep_result_legders(**context):
    souce_ledger = context['dag_run'].conf['ledgers'][0]
    project_code = context['dag_run'].conf['project_code']   
    version = souce_ledger["version"]
    classification = souce_ledger["classification"]
    label = souce_ledger["attributes"]["targettablename"].format(classification = classification, version = version)
    combined_stage_hdfs_path = os.path.join(os.sep, "stage", project_code, version,"combined","{0}.parquet".format(label))
    combined_stage_hdfs_location = "HDFS-DB"
    if souce_ledger["location"].upper() == "HDFS":
        combined_stage_hdfs_path = os.path.join(os.sep, "stage", project_code, version,"combined","{0}.csv".format(label))
        combined_stage_hdfs_location = "HDFS"
     # get row count file
    rowcount_stage_hdfs_path = os.path.join(os.sep, "stage", project_code, version,"combined", "rowcount_{0}.txt".format(label))
    rowcount_local = os.path.join(os.sep, "tmp", "rowcount_{0}_{1}_{2}_{3}.txt".format(project_code,version,"combined",label))
    get = Popen(["hdfs", "dfs", "-get", rowcount_stage_hdfs_path, rowcount_local], stdin=PIPE, bufsize=-1)
    get.communicate()
    rowcount_str = open(rowcount_local, "r").read()

    result_legders = [{
        "classification": souce_ledger["attributes"]["targetclassification"],
        "label": label,
        "version": version,
        "group_count": souce_ledger["group_count"],
        "location": combined_stage_hdfs_location,
        "location_details": combined_stage_hdfs_path,
        "row_count": int(rowcount_str),
        "attributes":{
            "project": project_code.lower(),
            "total_source_ledgers": len(context['dag_run'].conf['ledgers'])
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


with DAG(dag_id="nrda_combine_files", schedule_interval=None, default_args=default_args, catchup=False, max_active_runs=1, user_defined_macros={
    'json': json
}, tags=['working']) as dag:

    store_ledgers_to_file = PythonOperator(
        task_id="store_ledgers_to_file",
        python_callable=store_ledgers_to_file,
        provide_context=True
    )

    combine_files = SparkSubmitOperator(
        task_id="combine_files",
        conn_id="spark_conn",
        conf=spark_confs,
        # application="/usr/local/airflow/dags/scripts/nrda/combine_files.py",
        application="/opt/airflow/dags/scripts/nrda/combine_files.py",
        # executor_memory="30g",
        # executor_cores=9,
        executor_memory="2g",
        executor_cores=3,
        application_args=["{{ dag_run.conf['project_code'] }}", "{{ ti.xcom_pull(key='ledgers_json_hdfs_path') }}"],
        verbose=False
    )

    prep_result_legders = PythonOperator(
        task_id="prep_ledger",
        python_callable=prep_result_legders,
        provide_context=True
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
    store_ledgers_to_file  >> combine_files >> prep_result_legders >> housekeeping >> send_message 