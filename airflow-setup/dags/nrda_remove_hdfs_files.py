import sys
import os
import json
import airflow
import glob
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from subprocess import PIPE, Popen

# Would be cleaner to add the path to the PYTHONPATH variable
currentdir = os.path.dirname(os.path.realpath(__file__))
subdir = currentdir+'/scripts/nrda'

sys.path.append(subdir)

from rabbitmq_notifier import send_progress_message, send_failure_message, send_success_message

def remove_hdfs_files(**context):
    ledgers = context['dag_run'].conf['ledgers']
    for ledger in ledgers:
        location = ledger["location_details"]
        filexistchk="hdfs dfs -test -e "+location+";echo $?"
        #echo $? will print the exit code of previously execited command
        filexistchk_output=Popen(filexistchk,shell=True,stdout=PIPE).communicate()
        filechk="hdfs dfs -test -d "+location+";echo $?"
        filechk_output=Popen(filechk,shell=True,stdout=PIPE).communicate()
        #Check if location exists
        if '1' not in str(filexistchk_output[0]):
            # House keeping
            housekeeping = Popen(["hdfs", "dfs", "-rm", "-r", location], stdin=PIPE, bufsize=-1)
            housekeeping.communicate()
            print("file {0} has been deleted.".format(location))
            #check if its a directory
            if '1' not in str(filechk_output[0]):
                print('The given URI is a directory: '+location)
            else:
                print('The given URI is a file: '+location)
        else:
            print(location+ " does not exist. Please check the URI")
       

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

with DAG(dag_id="nrda_remove_hdfs_files", schedule_interval=None, default_args=default_args, catchup=False, max_active_runs=3, user_defined_macros={
    'json': json
}) as dag:

    remove_hdfs_files = PythonOperator(
        task_id="remove_hdfs_files",
        python_callable=remove_hdfs_files,
        provide_context=True
    )

    # send_message = PythonOperator(
    #     task_id="send_message",
    #     python_callable=send_success_message,
    #     provide_context=True,
    #     on_success_callback=None
    # )
    # remove_hdfs_files  >> send_message 
    remove_hdfs_files