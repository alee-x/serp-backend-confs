import pyodbc
import ibm_db_sa
import psycopg2
import os
import sys
import json
import pika
from sqlalchemy import create_engine, inspect, types, exc
import warnings
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook

currentdir = os.path.dirname(os.path.realpath(__file__))
subdir = currentdir+'/scripts/dbscanner'

sys.path.append(subdir)

from connection_setup import connection_setup
from custom_exceptions import NoSuchTableException, NoSuchSchemaException
from metrics import get_metrics
from rabbitmq_service import send_result, send_failure


def get_structure(**context):
    db_platform = context['dag_run'].conf['db_platform']
    db_name = context['dag_run'].conf['database_name']
    schema_name = context['dag_run'].conf['schema_name']
    table_name = context['dag_run'].conf['table_name']

    connection_str = context['ti'].xcom_pull(key='connection_str')
    engine = create_engine(connection_str)
    inspector = inspect(engine)

    if schema_name.lower() not in map(lambda x: str.lower(str.strip(x)), inspector.get_schema_names()):
        raise NoSuchSchemaException("Schema {0} can not be found in db {1}".format(schema_name,db_name))

    is_table = table_name.lower() in map(lambda x: str.lower(str.strip(x)), inspector.get_table_names(schema_name))
    is_view = table_name.lower() in map(lambda x: str.lower(str.strip(x)), inspector.get_view_names(schema_name))

    if not is_table and not is_view:
        raise NoSuchTableException("Table/View {0} can not be found in schema {1}".format(table_name,schema_name))

    table={}
    view={}
    columns=[]    
    
    sourceColumns = []
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore")
        try:
            sourceColumns = inspector.get_columns(table_name,schema_name)
        except exc.SAWarning as w:
            msg = "Warning encountered: " + schema_name + "    " + table_name + "    " + str(w)
            pass
        except:
            print(schema_name + "    " + table_name + "    " + sys.exc_info())                    
        warnings.filterwarnings("error")

    for column in sourceColumns:
        if db_platform == "db2" and isinstance(column['type'], ibm_db_sa.base.DOUBLE):       # Handle visit_DOUBLE error, can handle others with elif
            coltype = "DOUBLE"
        elif isinstance(column['type'], types.NullType):
            coltype = "NOT RECOGNISED"
        else: 
            coltype = str(column['type'])  

        c = { "name": column['name'], "type": coltype, "nullable": column['nullable'], "autoincrement": column['autoincrement']}
        columns.append(c)

    if is_table:   
        schema_result = {"fields" : columns, "is_view" : False,\
                    "pk_constraint" : inspector.get_pk_constraint(table_name,schema_name), \
                    "fk_keys" : inspector.get_foreign_keys(table_name,schema_name)}
    if is_view:
        schema_result = {"fields" : columns, "is_view" : True, "definition" : inspector.get_view_definition(table_name,schema_name)}

    context['ti'].xcom_push(key='engine_name',value=engine.name)
    context['ti'].xcom_push(key='schema_result',value=schema_result)


def prep_result(**context):
    result = {
        'db_path_object_id': context['dag_run'].conf['db_path_object_id'],
        'db_name': context['dag_run'].conf['database_name'],
        'schema_name': context['dag_run'].conf['schema_name'],
        'table_name': context['dag_run'].conf['table_name'],
        'schema': context['ti'].xcom_pull(key='schema_result'),
        'metrics': context['ti'].xcom_pull(key='metrics_result')
    }

    context['ti'].xcom_push(key='result',value=result)


default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "jeffrey@chi.swan.ac.uk",
    "on_failure_callback": send_failure
}

with DAG(dag_id="dbscanner_extract_schema_pipeline", schedule_interval=None, default_args=default_args, catchup=False, max_active_runs=3, user_defined_macros={
    'json': json
}, tags=["working"]) as dag:

    connection_setup = PythonOperator(
        task_id='connection_setup',
        python_callable=connection_setup,
        provide_context=True
    )

    get_structure = PythonOperator(
        task_id='get_structure',
        python_callable=get_structure,
        provide_context=True
    )

    get_metrics = PythonOperator(
        task_id='get_metrics',
        python_callable=get_metrics,
        provide_context=True
    )

    prep_result = PythonOperator(
        task_id='prep_result',
        python_callable=prep_result,
        provide_context=True
    )

    send_result = PythonOperator(
        task_id='send_result',
        python_callable=send_result,
        provide_context=True
    )
    
    connection_setup >> get_structure >> get_metrics >> prep_result >> send_result