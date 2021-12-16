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
import boto3
from botocore.client import Config
from time import process_time
import datetime 
import errno
import time

def send_progress_message(s3_path, hdfs_path, vhost):
    rabbitmq_conn = BaseHook.get_connection('rabbitmq_conn')
    print("rabbitmq host: " + rabbitmq_conn.host)

    credentials = pika.PlainCredentials(rabbitmq_conn.login, rabbitmq_conn.password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_conn.host,
                                                                   rabbitmq_conn.port,
                                                                   vhost,
                                                                   credentials))
    channel = connection.channel()
    channel.queue_declare(queue='S3_To_HDFS', durable=True)
    print('rabbit_vhost: {}'.format(vhost))
    message = json.dumps(
        {'s3_path': s3_path, "hdfs_path": hdfs_path}
    )
    channel.basic_publish(exchange='', routing_key='S3_To_HDFS',
                          body=message)
    print(" [x] Sent {}".format(message))
    connection.close()

def run_s3_to_hdfs(**context):

    s3_conn = BaseHook.get_connection('s3_conn')
    print("s3 host: " + s3_conn.host)
    print("s3 vhost: " + s3_conn.extra)
    vhost = json.loads(s3_conn.extra)
    # Configure S3 Connection
    s3 = boto3.resource('s3',
    aws_access_key_id = s3_conn.login,
    aws_secret_access_key = s3_conn.password,
    endpoint_url = s3_conn.host,
    config=Config(signature_version='s3v4'))

    client = s3.meta.client

    # Print out bucket names
    for bucket in s3.buckets.all():
        print(bucket.name)
        # Get the client from the resource
    bucket_name = "frontdoor"
    paginator = client.get_paginator('list_objects')
    page_iterator = paginator.paginate(Bucket=bucket_name)
    for page in page_iterator: 
        if "Contents" in page:
            for obj in page['Contents']:
                s3filePath = f"/{bucket_name}/{obj['Key']}"
                localfilePath = '/tmp/s3_to_hdfs/{0}/{1}'.format(bucket_name,obj['Key'])
                if not os.path.exists(os.path.dirname(localfilePath)):
                    try:
                        os.makedirs(os.path.dirname(localfilePath))
                        print("Local directory {0} has been created.".format(localfilePath))
                    except OSError as exc: # Guard against race condition
                        if exc.errno != errno.EEXIST:
                            raise
                        
                print("Downloading S3 file {0} ...".format(s3filePath))
                #Download s3 file to local
                start = process_time()   
                client.download_file(bucket_name, obj['Key'], localfilePath)
                elapsed = process_time() 
                elapsed = elapsed - start
                print ("S3 file {0} has been successfully downloaded to local {1}. (Time spent: {2})".format(s3filePath, localfilePath, str(datetime.timedelta(seconds = elapsed)) ))
                #Upload to downloaded file to HDFS
                hdfsfilePath = '/frontdoor/s3_to_hdfs/{0}'.format(obj['Key'])   
                timeStarted = time.time()        
                put = Popen(["hdfs", "dfs", "-mkdir", "-p", os.path.dirname(hdfsfilePath)], stdin=PIPE, bufsize=-1)
                put.communicate()
                put = Popen(["hdfs", "dfs", "-put", "-f", localfilePath, hdfsfilePath], stdin=PIPE, bufsize=-1)
                put.communicate()   
                timeDelta = time.time() - timeStarted                     # Get execution time.
                print("Finished process in "+str(timeDelta)+" seconds.")  # Output result.
                print ("Local file {0} has been successfully uploaded to HDFS {1}. ".format(localfilePath, hdfsfilePath))
                #Send rabbit msg
                send_progress_message(s3filePath, hdfsfilePath, vhost['vhost'])
                #Remove local file
                os.remove(localfilePath)
                #Remove S3 file
                client.delete_object(Bucket=bucket_name, Key=obj['Key'])
                print ("S3 file {0} has been successfully deleted. ".format(s3filePath))
        else:
            print ("No files to move")

default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "jeffrey@chi.swan.ac.uk"
}

with DAG(dag_id="nrda_s3_to_hdfs", schedule_interval="*/15 * * * *", default_args=default_args, catchup=False, max_active_runs=1, user_defined_macros={
    'json': json
}) as dag:

    run_s3_to_hdfs = PythonOperator(
        task_id="run_s3_to_hdfs",
        python_callable=run_s3_to_hdfs,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(minutes=1)
    )