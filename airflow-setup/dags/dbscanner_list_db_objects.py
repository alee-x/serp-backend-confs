import os
import sys
import json
from sqlalchemy import create_engine, inspect
import re
import pika
from airflow.hooks.base_hook import BaseHook
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.hooks.base_hook import BaseHook

currentdir = os.path.dirname(os.path.realpath(__file__))
subdir = currentdir+'/scripts/dbscanner'

sys.path.append(subdir)

import system_schemas_to_ignore
from connection_setup import connection_setup
from custom_exceptions import NoSuchTableException, NoSuchSchemaException
from rabbitmq_service import send_failure


def list_items(**context):
    connection_str = context['ti'].xcom_pull(key='connection_str')
    engine = create_engine(connection_str)
    inspector = inspect(engine)

    engine_name = engine.name
    db_name = engine.url.database
    # Override for DB2 as this includes the full connection string. Just get the name before the first ;
    # if engine_name == "ibm_db_sa":
    #     db_name = engine.url.database.split(';', -1)[0]
        # db2_schemas_to_check = context['dag_run'].conf['db2_schemas_to_check']

    schemas_to_ignore = context['dag_run'].conf['schemas_to_ignore']
    schemas_to_check = context['dag_run'].conf['schemas_to_check']

    print(type(schemas_to_ignore))
    print(schemas_to_ignore)

    # set up connection to rabbit queue
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


    # for each schema in the database
    schemas = []
    for db_schema in inspector.get_schema_names():
        db_schema = db_schema.strip()
        db_schema_l = db_schema.lower()

        if ((engine_name == "mssql" and db_schema_l not in system_schemas_to_ignore.mssql) 
            or (engine_name == "postgresql" and db_schema_l not in system_schemas_to_ignore.pgsql)
            or (engine_name == "ibm_db_sa" and (db_schema_l not in system_schemas_to_ignore.db2 )) #and db_schema.lower() in db2_schemas_to_check))
            ):

            if schemas_to_ignore:
                if db_schema_l in schemas_to_ignore:
                    continue
            
            if schemas_to_check:
                if db_schema_l not in schemas_to_check:
                    continue

            # prepare a list of all objects in the schema
            objects = []
            for object_name in inspector.get_table_names(db_schema):
                objects.append(object_name.strip())

            for object_name in inspector.get_view_names(db_schema):
                objects.append(object_name.strip())

            # keep track of the number of objects, as the index will be used to delay future calls
            # index = len(objects)
            index = 1

            # for each object (table/view) in the schemas
            for db_table in objects:                
                is_table = db_table.lower() in map(lambda x: str.lower(str.strip(x)), inspector.get_table_names(db_schema))
                is_view = db_table.lower() in map(lambda x: str.lower(str.strip(x)), inspector.get_view_names(db_schema))

                if not is_table and not is_view:
                    raise NoSuchTableException("Table/View {0} can not be found in db.".format(db_table))

                if is_table:   
                    is_view_result = False

                if is_view:
                    is_view_result = True

                table = {
                    "table_name" : db_table, 
                    "is_view" : is_view_result
                }

                schema = {
                    "schema_name" : db_schema, 
                    "table" : table
                }

                message = json.dumps({
                    "run_id": context['dag_run'].run_id,
                    "db_platform": context['dag_run'].conf['db_platform'],
                    "db_conn_id": context['dag_run'].conf['database_conn_id'],
                    "db_name": context['dag_run'].conf['database_name'],
                    "engine" : engine_name,
                    "schema": schema,
                    "index": index
                })

                channel.basic_publish(exchange='', routing_key=rabbit_queue,body=message)
                print("sent message: {0}".format(message))

                # index -= 1
                index += 1
            
    connection.close()


default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "jeffrey@chi.swan.ac.uk",
    "on_failure_callback": send_failure
}

with DAG(dag_id="dbscanner_list_db_objects_pipeline", schedule_interval=None, default_args=default_args, catchup=False, max_active_runs=3, user_defined_macros={
    'json': json
}, tags=['working']) as dag:

    connection_setup = PythonOperator(
        task_id='connection_setup',
        python_callable=connection_setup,
        provide_context=True
    )

    list_items = PythonOperator(
        task_id='list_items',
        python_callable=list_items,
        provide_context=True
    )

    # prep_result = PythonOperator(
    #     task_id='prep_result',
    #     python_callable=prep_result,
    #     provide_context=True
    # )

    # send_result = PythonOperator(
    #     task_id='send_result',
    #     python_callable=send_result,
    #     provide_context=True
    # )
    
    connection_setup >> list_items #>> prep_result >> send_result