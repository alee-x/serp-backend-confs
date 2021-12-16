import json
from datetime import datetime
import pika
import traceback
from airflow.hooks.base_hook import BaseHook

def send_progress_message(context):
    print(context)
    rabbitmq_conn = BaseHook.get_connection('rabbitmq_conn')
    print("rabbitmq host: " + rabbitmq_conn.host)
    rabbit_vhost = context['dag_run'].conf['rabbit_vhost']
    # rabbit_vhost = "dev" # temporary fix as keeps trying to use vhost 'damian'
    print('rabbit_vhost: {}'.format(rabbit_vhost))

    credentials = pika.PlainCredentials(rabbitmq_conn.login, rabbitmq_conn.password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_conn.host,
                                                                   rabbitmq_conn.port,
                                                                   rabbit_vhost,
                                                                   credentials))
    channel = connection.channel()
    channel.queue_declare(queue='AirflowNotification', durable=True)
    print('Producing messages at {}'.format(datetime.utcnow()))
    message = json.dumps(
        {'jobtask_id': context['dag_run'].conf['jobtask_id'], "run_id": context['run_id'], "status": "Running", "result_string": context['task'].task_id}
    )
    channel.basic_publish(exchange='', routing_key='AirflowNotification',
                          body=message)
    print(" [x] Sent {}".format(message))
    connection.close()

def send_failure_message(context):
    exception = context.get('exception')
    formatted_exception = ''.join(
    traceback.format_exception(etype=type(exception), 
        value=exception, tb=exception.__traceback__
    )
    ).strip()

    rabbitmq_conn = BaseHook.get_connection('rabbitmq_conn')
    print("rabbitmq host: " + rabbitmq_conn.host)

    rabbit_vhost = context['dag_run'].conf['rabbit_vhost']
    # rabbit_vhost = "dev" # temporary fix as keeps trying to use vhost 'damian'
    print('rabbit_vhost: {}'.format(rabbit_vhost))

    credentials = pika.PlainCredentials(rabbitmq_conn.login, rabbitmq_conn.password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_conn.host,
                                                                   rabbitmq_conn.port,
                                                                   rabbit_vhost,
                                                                   credentials))
    channel = connection.channel()
    channel.queue_declare(queue='AirflowNotification', durable=True)
    print('Producing messages at {}'.format(datetime.utcnow()))
    message = json.dumps(
        {'jobtask_id': context['dag_run'].conf['jobtask_id'], "run_id": context['run_id'], "status": "Failed", "result_string":formatted_exception}
    )
    channel.basic_publish(exchange='', routing_key='AirflowNotification',
                          body=message)
    print(" [x] Sent {}".format(message))
    connection.close()

def send_success_message(**context):
    rabbitmq_conn = BaseHook.get_connection('rabbitmq_conn')
    print("rabbitmq host: " + rabbitmq_conn.host)

    rabbit_vhost = context['dag_run'].conf['rabbit_vhost']
    #rabbit_vhost = "dev" # temporary fix as keeps trying to use vhost 'damian'
    print('rabbit_vhost: {}'.format(rabbit_vhost))

    credentials = pika.PlainCredentials(rabbitmq_conn.login, rabbitmq_conn.password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_conn.host,
                                                                   rabbitmq_conn.port,
                                                                   rabbit_vhost,
                                                                   credentials))
    channel = connection.channel()
    channel.queue_declare(queue='AirflowNotification', durable=True)
    print('rabbit_vhost: {}'.format(rabbit_vhost))
    print('Producing messages at {}'.format(datetime.utcnow()))
    result_ledgers = context['ti'].xcom_pull(key='result_legders')
    if(result_ledgers):
        message = json.dumps(
            {'jobtask_id': context['dag_run'].conf['jobtask_id'],
             'result_ledger_list': result_ledgers,
                "run_id": context['run_id'], "status": "Completed"}
        )
    else:
        message = json.dumps(
            {'jobtask_id': context['dag_run'].conf['jobtask_id'],
                "run_id": context['run_id'], "status": "Completed"}
        )
    channel.basic_publish(exchange='', routing_key='AirflowNotification',
                          body=message)
    print(" [x] Sent {}".format(message))
    connection.close()
