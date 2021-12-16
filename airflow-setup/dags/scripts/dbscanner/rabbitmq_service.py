import json
import pika
import traceback
from airflow.hooks.base_hook import BaseHook


def send_result(**context):
    rabbitmq_conn = BaseHook.get_connection('rabbitmq_conn')
    rabbit_queue = context['dag_run'].conf['rabbit_queue']
    rabbit_vhost = context['dag_run'].conf['rabbit_vhost']

    print("rabbitmq host: " + rabbitmq_conn.host)
    print("rabbitmq queue: " + rabbit_queue)
    print("rabbitmq vhost: " + rabbit_vhost)

    credentials = pika.PlainCredentials(rabbitmq_conn.login, rabbitmq_conn.password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_conn.host,
                                                                   rabbitmq_conn.port,
                                                                   rabbit_vhost,
                                                                   credentials))
    channel = connection.channel()
    channel.queue_declare(queue=rabbit_queue, durable=True)

    result = context['ti'].xcom_pull(key='result')
    message = json.dumps(result)

    channel.basic_publish(exchange='', routing_key=rabbit_queue,
                          body=message)
    print(" [x] Sent {}".format(message))
    connection.close()


def send_failure(**context):
    rabbitmq_conn = BaseHook.get_connection('rabbitmq_conn')
    rabbit_queue = context['dag_run'].conf['rabbit_queue']
    rabbit_vhost = context['dag_run'].conf['rabbit_vhost']

    print("rabbitmq host: " + rabbitmq_conn.host)
    print("rabbitmq queue: " + rabbit_queue)
    print("rabbitmq vhost: " + rabbit_vhost)

    credentials = pika.PlainCredentials(rabbitmq_conn.login, rabbitmq_conn.password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_conn.host,
                                                                   rabbitmq_conn.port,
                                                                   rabbit_vhost,
                                                                   credentials))
    channel = connection.channel()
    channel.queue_declare(queue=rabbit_queue, durable=True)

    exception = context.get('exception')
    formatted_exception = ''.join(
    traceback.format_exception(etype=type(exception), 
        value=exception, tb=exception.__traceback__
    )
    ).strip()

    message = json.dumps({
        "run_id": context['dag_run'].run_id,
        "status": "Failed",
        "result_string": formatted_exception
    })

    channel.basic_publish(exchange='', routing_key=rabbit_queue,
                          body=message)
    print(" [x] Sent {}".format(message))
    connection.close()
