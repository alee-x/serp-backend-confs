import sys
import os
import json
import airflow
import pika
import requests
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from subprocess import PIPE, Popen
from time import process_time
import datetime 
import errno
import time
import paramiko
from stat import S_ISDIR

def send_progress_message(sftp_path, hdfs_path, vhost):
    rabbitmq_conn = BaseHook.get_connection('rabbitmq_conn')
    print("rabbitmq host: " + rabbitmq_conn.host)

    credentials = pika.PlainCredentials(rabbitmq_conn.login, rabbitmq_conn.password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_conn.host,
                                                                   rabbitmq_conn.port,
                                                                   vhost,
                                                                   credentials))
    channel = connection.channel()
    channel.queue_declare(queue='SFTP_To_HDFS', durable=True)
    print('rabbit_vhost: {}'.format(vhost))
    message = json.dumps(
        {'sftp_path': sftp_path, "hdfs_path": hdfs_path}
    )
    channel.basic_publish(exchange='', routing_key='SFTP_To_HDFS',
                          body=message)
    print(" [x] Sent {}".format(message))
    connection.close()

def sftp_get_recursive(path, sftp,vhost):
    item_list = sftp.listdir(path)
    for item in item_list:
        item = str(item)
        if S_ISDIR(sftp.stat(path + "/" + item).st_mode):
            sftp_get_recursive(path + "/" + item,sftp, vhost)
        else:
            #sftp.get(path + "/" + item, dest + "/" + item)
            #if("covid_test_export" in item.lower()):            
            if(not item.lower().endswith(".filepart")):
               print(path + "/" + item)
               sftp_to_hdfs(path + "/" + item, sftp, vhost)



def sftp_to_hdfs(sftp_path, sftp, vhost):
    localfilePath = '/tmp/sftp_to_hdfs{0}'.format(sftp_path)
    if not os.path.exists(os.path.dirname(localfilePath)):
        try:
            os.makedirs(os.path.dirname(localfilePath))
            print("Local directory {0} has been created.".format(os.path.dirname(localfilePath)))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    print("Downloading SFTP file from {0} ...".format(sftp_path))
    #Download sftp file to local
    start = process_time()   
    sftp.get(sftp_path,localfilePath)
    elapsed = process_time() 
    elapsed = elapsed - start
    print ("SFTP file {0} has been successfully downloaded to local {1}. (Time spent: {2})".format(sftp_path, localfilePath, str(datetime.timedelta(seconds = elapsed)) ))
    #Upload to downloaded file to HDFS
    hdfsfilePath = '/frontdoor/sftp_to_hdfs{0}'.format(sftp_path)
    timeStarted = time.time()        
    put = Popen(["hdfs", "dfs", "-mkdir", "-p", os.path.dirname(hdfsfilePath)], stdin=PIPE, bufsize=-1)
    put.communicate()
    put = Popen(["hdfs", "dfs", "-put", "-f", localfilePath, hdfsfilePath], stdin=PIPE, bufsize=-1)
    put.communicate()   
    timeDelta = time.time() - timeStarted                     # Get execution time.
    print("Finished process in "+str(timeDelta)+" seconds.")  # Output result.
    print ("Local file {0} has been successfully uploaded to HDFS {1}. ".format(localfilePath, hdfsfilePath))
    #Send rabbit msg
    send_progress_message(sftp_path, hdfsfilePath, vhost['vhost'])
    #Remove local file
    os.remove(localfilePath)
    #Remove sftp file
    try:
        sftp.remove(sftp_path)
    except Exception:
        print("connection error - will try again")
        ftp_conn = BaseHook.get_connection('ftp_default')
        transport = paramiko.Transport((ftp_conn.host, ftp_conn.port))
        transport.connect(username = ftp_conn.login, password = ftp_conn.password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.remove(sftp_path)

    
    print ("SFTP file {0} has been successfully deleted. ".format(sftp_path))

def nrda_sftp_to_hdfs(**context):
    ftp_conn = BaseHook.get_connection('ftp_default')
    print("vhost: " + ftp_conn.extra)
    vhost = json.loads(ftp_conn.extra)

    transport = paramiko.Transport((ftp_conn.host, ftp_conn.port))
    transport.connect(username = ftp_conn.login, password = ftp_conn.password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp_get_recursive("",sftp,vhost)

default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "jeffrey@chi.swan.ac.uk"
}

with DAG(dag_id="nrda_sftp_to_hdfs", schedule_interval="*/20 * * * *", default_args=default_args, catchup=False, max_active_runs=1, user_defined_macros={
    'json': json
}) as dag:

    run_sftp_to_hdfs = PythonOperator(
        task_id="nrda_sftp_to_hdfs",
        python_callable=nrda_sftp_to_hdfs,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=1)
    )