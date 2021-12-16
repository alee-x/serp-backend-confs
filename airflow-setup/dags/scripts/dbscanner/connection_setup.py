from airflow.hooks.base_hook import BaseHook

def connection_setup(**context):
    db_platform = context['dag_run'].conf['db_platform']
    db_name = context['dag_run'].conf['database_name']
    db_conn_id = context['dag_run'].conf['database_conn_id']

    conn = BaseHook.get_connection(db_conn_id)
    connection_str = "://{0}:{1}@{2}:{3}/{4}".format(conn.login,conn.password,conn.host,conn.port,db_name)

    if db_platform == "db2":
        connection_str = "db2+ibm_db" + connection_str

    if db_platform == "mssql":
        connection_str = "://{0}:{1}@{2}:{3}/{4}?driver=ODBC+Driver+17+for+SQL+Server".format(conn.login,conn.password,conn.host,conn.port,db_name)
        connection_str = "mssql+pyodbc" + connection_str 

    if db_platform == "postgres":
        connection_str = "postgres" + connection_str

    context['ti'].xcom_push(key='connection_str',value=connection_str)