import sys
import os
import json
from datetime import datetime
import pika
from airflow.hooks.base_hook import BaseHook
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

#Would be cleaner to add the path to the PYTHONPATH variable
currentdir = os.path.dirname(os.path.realpath(__file__))
subdir = currentdir+'/scripts/nrda'

sys.path.append(subdir)
from rabbitmq_notifier import send_progress_message, send_failure_message, send_success_message

def send_to_queue(**context):
    ledgers = context['dag_run'].conf['ledgers']
    ledger = ledgers[0]

    print("LEDGER")
    print(json.dumps(ledger))

    action_code = ledger['attributes']['action_code']
    print("action code: ", action_code)

    action_parameter = ledger['attributes']['action_parameter']
    print("action parameter: ", action_parameter)

    rabbitmq_conn = BaseHook.get_connection('rabbitmq_conn')
    print("rabbitmq host: " + rabbitmq_conn.host)

    credentials = pika.PlainCredentials(rabbitmq_conn.login, rabbitmq_conn.password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_conn.host,
                                                                   rabbitmq_conn.port,
                                                                   context['dag_run'].conf['rabbit_vhost'],
                                                                   credentials))
    channel = connection.channel()
    channel.queue_declare(queue='SendDataDag', durable=True)
    print('rabbit_vhost: {}'.format(context['dag_run'].conf['rabbit_vhost']))
    print('Producing messages at {}'.format(datetime.utcnow()))

    message = json.dumps({
        'action_code': action_code,
        'action_parameter': action_parameter,
        'ledgers': ledgers
    })
    channel.basic_publish(exchange='', routing_key='FromSendDataDAG',
                          body=message)
    print(" [x] Sent {}".format(message))
    connection.close()


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

with DAG(dag_id="nrda_send_data_pipeline", schedule_interval=None, default_args=default_args, catchup=False, max_active_runs=3, user_defined_macros={
    'json': json
}) as dag:

    send_to_queue = PythonOperator(
        task_id='send_to_queue', 
        python_callable=send_to_queue, 
        provide_context=True
    )

    send_message = PythonOperator(
        task_id="send_message",
        python_callable=send_success_message,
        provide_context=True,
        on_success_callback=None
    )

    send_to_queue >> send_message