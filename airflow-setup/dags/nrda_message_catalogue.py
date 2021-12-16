import ast
import sys
import json
import airflow
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.hooks.base_hook import BaseHook

from collections import namedtuple
from typing import List
from json import JSONEncoder
from ast import literal_eval
import os
currentdir = os.path.dirname(os.path.realpath(__file__))
subdir = currentdir+'/scripts/nrda'

sys.path.append(subdir)
from rabbitmq_notifier import send_progress_message, send_failure_message, send_success_message

currentdir = os.path.dirname(os.path.realpath(__file__))
msubdir = currentdir+'/models'

sys.path.append(msubdir)

from catalogue_models import DtColumn, NumColumn, CharColumn, Metrics, TableObject, Folder, Nodes, Colume, Structure, CatalogueAsset
from catalogue_attachment_models import CatalogueAttachmentAsset, SourceDataSet

default_args = {
    "owner": "airflow",
    "start_date": airflow.utils.dates.days_ago(1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "llyr@chi.swan.ac.uk",
    # "retries": 1,
    # "retry_delay": timedelta(minutes=20),
    "on_success_callback": send_progress_message,
    "on_failure_callback": send_failure_message
}

with DAG(dag_id="nrda_message_catalogue", schedule_interval=None, default_args=default_args, catchup=False, max_active_runs=2, user_defined_macros={'json': json}) as dag:

    ### Check if the provided ledgers have a schema object as currently the schema info is provided by ledger in attributes
    ### Metrics need to be sent to the catalogue before the ledger attachments - otherwise it will break
    def is_ledger_valid(**context):
        ledgers = []

        if 'ledger' in context['dag_run'].conf:
            print('ledger found')
            ledgers.append(context['dag_run'].conf['ledger'])
        elif 'ledgers' in context['dag_run'].conf:
            if len(context['dag_run'].conf['ledgers'])>0:
                print('ledgers found')
                ledgers = context['dag_run'].conf['ledgers']
        else:
            print('no ledgers or ledger found')
            raise ValueError("No ledgers or ledger")

        valid = False

        valid_ledgers = []

        for ledger in ledgers:
            print("ledger ID:", ledger['id'])
            print("attributes:", ledger['attributes'])

            # check if the ledger has a metrics object before trying to get the metrics
            if 'metrics' in ledger['attributes']:
                valid = True
                print("Ledger ", ledger['id'], " OK")
                valid_ledgers.append(ledger)
            else:
                print("Ledger ", ledger['id'], " INVALID - no metrics object")

        if not valid:
            print("No metrics object found in any ledgers")
            raise ValueError("No metrics object found in {0} ledger(s)".format(len(ledgers)))
        else:
            context['ti'].xcom_push(key='ledgers',value=valid_ledgers)


    ### Get Schema information by doing API call to get Schema
    def get_schema_metrics(**context):

        ######## Below to call API ########
        ######## This is being worked on but currently the schema info is provided by ledger in attributes.
        # airflow_oauth2_conn = BaseHook.get_connection('airflow_oauth2')
        # token_url = airflow_oauth2_conn.host
        # #client (application) credentials on keycloak
        # client_id = airflow_oauth2_conn.login
        # client_secret = airflow_oauth2_conn.password
        # #step A, B - single call with client credentials as the basic auth header - will return access_token
        # data = {'grant_type': 'client_credentials'}
        # access_token_response = requests.post(token_url, data=data, verify=False, allow_redirects=False, auth=(client_id, client_secret))
        # # print(access_token_response)
        # tokens = json.loads(access_token_response.text)
        # # print("access token: " + tokens['access_token'])

        # #check if there is one ledger or many ledgers
        # dataset_id = 0
        # if 'ledger' in context['dag_run'].conf:
        #     dataset_id = context['dag_run'].conf['ledger']['dataset_id']
        # elif 'ledgers' in context['dag_run'].conf:
        #     if len(context['dag_run'].conf['ledgers'])>0:
        #         dataset_id = context['dag_run'].conf['ledgers'][0]['dataset_id']

        # schema_get_api_url = "http://192.168.69.64:5005/api/Schemas/{0}".format(dataset_id)

        # print(schema_get_api_url)
        # api_call_headers = {'Authorization': 'Bearer ' + tokens['access_token']}
        # api_call_response = requests.get(schema_get_api_url, headers=api_call_headers, verify=False)
        
        # schemas = api_call_response.json()

        ######## End ########


        ####### Below is retrieved by attributes ########
        ledgers = context['ti'].xcom_pull(key='ledgers')

        for ledger in ledgers:
            print(ledger)

            # check if the ledger has a schema object before trying to get the metrics
            if 'schema' not in ledger['attributes']:
                continue

            schemas =  json.loads(ledger['attributes']['schema'])
            print(schemas)

            schema = schemas['schema_definition']

            #for schema in schemas:
            #check if asset name is not null
            if ledger['id'] != 0:
                columes = []
                for filedDef in schema['fieldDef']:
                    print(filedDef)
                    columnObj = Colume(**filedDef)
                    columes.append(columnObj)

                print(len(columes))

                structure = Structure(columes)
                context['ti'].xcom_push(key='dataset_structure_' + str(ledger['id']) , value=structure)
            else:
                print("ledger id invalid")

    ### Get metrics from context (could be multiple ledgers passed in depending no how many are in a dataset) 
    def get_metrics(**context):
        ledgers = context['ti'].xcom_pull(key='ledgers')
        
        tableObjList = []

        print(ledgers)

        for ledger in ledgers:

            print(ledger['attributes'])
            print(ledger['attributes']['metrics'])
            metadata =  json.loads(ledger['attributes']['metrics'])
            #print(metadata['char_metrics'][0])
            #print(metadata.char_metrics[0].name, metadata.char_metrics[0].count)

            # charObj = CharColumn(**metadata['char_metrics'][0])
            # print(charObj.column_name)

            charList = []
            for charMetric in metadata['char_metrics']:
                charObj = CharColumn(**charMetric)
                charList.append(charObj)

            numList = []
            for numMetric in metadata['num_metrics']:
                numObj = NumColumn(**numMetric)
                numList.append(numObj)

            dtList = []
            for dtMetric in metadata['dt_metrics']:
                dtObj = DtColumn(**dtMetric)
                dtList.append(dtObj)
            
            metricsObj = Metrics(charList, numList, dtList) 
            structure = context['ti'].xcom_pull(key='dataset_structure_' + str(ledger['id']))

            print('### Structure1')
            print(structure)
            StructureJsonData = json.dumps(structure, default=lambda o: o.__dict__, indent=4)
            print('### Structure2')
            print(StructureJsonData)


            tableObj = TableObject(metricsObj, structure, ledger['label'])

            tableObjList.append(tableObj)
        
        print('### TableObject')
        tableObjJsonData = json.dumps(tableObjList, default=lambda o: o.__dict__, indent=4)
        print(tableObjJsonData)

        context['ti'].xcom_push(key='dataset_metrics', value=tableObjList)


    ### Get path in Catalogue 
    def get_catalogue_path(**context):
        
        if 'data_catalogue_dest' in context['dag_run'].conf:
            catalogueDest = context['dag_run'].conf['data_catalogue_dest']
            print('data_catalogue_dest found:', catalogueDest)

            if catalogueDest:
                print('catalogueDest available')
                allParts = catalogueDest.split("/")
                
                mainFolder = Folder(allParts[0],"")
                print(allParts[0])

                subParts = allParts[1:]
                print(subParts)
                listSubFolders = []
                parentFolder = allParts[0]
                for part in subParts:
                    if part == '':
                        raise ValueError("Node cannot be null or empty, check the file path is in the format '/file/path/here'")
                    subFolder = Folder(part, parentFolder)
                    listSubFolders.append(subFolder)
                    parentFolder = part

                nodes = Nodes(mainFolder,listSubFolders)

                context['ti'].xcom_push(key='dataset_nodes', value=nodes)

                print('### Node')
                nodesJsonData = json.dumps(nodes, default=lambda o: o.__dict__, indent=4)
                print(nodesJsonData)
                

        else:
            print('no data_catalogue_dest found')


    ### Construct FQS dataset model to send to Metadata Gateway via API method
    def construct_metrics_catalogue_asset(**context):
        projectName = context['dag_run'].conf['project_name']

        metrics = context['ti'].xcom_pull(key='dataset_metrics')
        nodes = context['ti'].xcom_pull(key='dataset_nodes')
        catalogueAsset = CatalogueAsset(metrics,nodes, projectName)
        context['ti'].xcom_push(key='dataset_catalogue_asset', value=catalogueAsset)

        print('### Asset')
        assetJsonData = json.dumps(catalogueAsset, default=lambda o: o.__dict__, indent=4)
        print(assetJsonData)


    ### Send Message to Metadata Gateway
    def send_metrics_to_metadata_gateway(**context):
        airflow_oauth2_conn = BaseHook.get_connection('airflow_oauth2')
        token_url = airflow_oauth2_conn.host
        #client (application) credentials on keycloak
        client_id = airflow_oauth2_conn.login
        client_secret = airflow_oauth2_conn.password
        #step A, B - single call with client credentials as the basic auth header - will return access_token
        data = {'grant_type': 'client_credentials'}
        access_token_response = requests.post(token_url, data=data, verify=False, allow_redirects=False, auth=(client_id, client_secret))
        tokens = json.loads(access_token_response.text)
        
        metadata_gateway_conn =  BaseHook.get_connection('metadata_gateway')
        schema_get_api_url = "{0}/Receiver/FQS".format(metadata_gateway_conn.host)

        catalogueAsset = context['ti'].xcom_pull(key='dataset_catalogue_asset')

        request_payload = json.dumps(catalogueAsset, default=lambda o: o.__dict__)

        # print(request_payload)
        # api_call_headers = {'Authorization': 'Bearer ' + tokens['access_token']}
        # print(api_call_headers)
        #api_call_response = requests.get(schema_get_api_url, headers=api_call_headers, verify=False)
        api_call_headers = {'Content-type': 'application/json', 'Accept': 'text/plain', 'Authorization': 'Bearer ' + tokens['access_token']}
        api_call_response = requests.post(schema_get_api_url, data=request_payload, headers=api_call_headers, verify=False)

        print("status code: {0}".format(api_call_response.status_code))
        print("content: {0}".format(api_call_response.content))     

        if api_call_response.status_code != 200:
            print("error connecting to metadata gateway")
            raise ValueError("Error connecting to metadata gateway")

        response = api_call_response.json()
        print("RESPONSE")
        print(type(response))

        if not response:
            print("Catalogue asset rejected")
            raise ValueError("Catalogue asset rejected")
        
        print(response)

    
    ### Check if we want to send attachments
    def is_send_attachments(**context):
        ledgers = context['ti'].xcom_pull(key='ledgers')

        for ledger in ledgers:
            send_attachments = ledger['attributes']['sendattachments']
            print("ledger ", ledger['id'], " send_attachments: ", send_attachments)
            if send_attachments:
                return "get_ledger_attachments"

        return "send_message"


    ### Get the attachments for the given ledgers and construct models to be sent to the metadata gateway
    def get_ledger_attachments(**context):
        ledgers = context['ti'].xcom_pull(key='ledgers')
        
        print(ledgers)

        airflow_oauth2_conn = BaseHook.get_connection('airflow_oauth2')
        nrda_backend_api_conn = BaseHook.get_connection('nrdav2_backend_api')

        token_url = airflow_oauth2_conn.host
        #client (application) credentials on keycloak
        client_id = airflow_oauth2_conn.login
        client_secret = airflow_oauth2_conn.password
        #step A, B - single call with client credentials as the basic auth header - will return access_token
        data = {'grant_type': 'client_credentials'}

        project_name = context['dag_run'].conf['project_name']
        nodes = context['ti'].xcom_pull(key='dataset_nodes')
        source_dataset = SourceDataSet(False, project_name, nodes)

        attachment_messages = []
        for ledger in ledgers:
            attrs = ledger['attributes']
            if attrs['sendattachments']:
                # get access token for nrda backend
                access_token_response = requests.post(token_url, data=data, verify=False, allow_redirects=False, auth=(client_id, client_secret))
                tokens = json.loads(access_token_response.text)
                print("access token: " + tokens['access_token'])

                # get ledger attachments for ledger id
                get_ledger_attachment_url = "{0}/api/LedgerAttachment/{1}".format(nrda_backend_api_conn.host, ledger['id'])
                print("nrda backend api host: " + get_ledger_attachment_url)
                api_call_headers = {'Authorization': 'Bearer ' + tokens['access_token']}
                api_call_response = requests.get(get_ledger_attachment_url, headers=api_call_headers)

                # no ledger attachments
                if api_call_response.status_code == 204:
                    print("no ledger attachments")
                    continue

                # error connecting to backend api
                if api_call_response.status_code != 200:
                    print("error connecting to backend api")
                    continue

                response_json = api_call_response.json()
                print(json.dumps(response_json))

                # loop through each of the ledger attachments - key is the filename, value is the file contents - construct a model to send to metadata gateway
                for key, value in response_json['fileNameAndContents'].items():
                    catalogue_attachment_asset = CatalogueAttachmentAsset(key, value, nodes, source_dataset)
                    attachment_messages.append(catalogue_attachment_asset)

        context['ti'].xcom_push(key='attachment_messages', value=attachment_messages)


    ### Check if there are any attachments to send
    def any_attachments(**context):
        attachment_messages = context['ti'].xcom_pull(key='attachment_messages')
        print(attachment_messages)

        if attachment_messages == []:
            print("no attachments to send")
            return "send_message"

        return "send_ledger_attachments_to_metadata_gateway"


    ### Send ledger attachment messages to Metadata Gateway
    def send_ledger_attachments_to_metadata_gateway(**context):
        airflow_oauth2_conn = BaseHook.get_connection('airflow_oauth2')
        metadata_gateway_conn = BaseHook.get_connection('metadata_gateway')

        token_url = airflow_oauth2_conn.host
        #client (application) credentials on keycloak
        client_id = airflow_oauth2_conn.login
        client_secret = airflow_oauth2_conn.password
        #step A, B - single call with client credentials as the basic auth header - will return access_token
        data = {'grant_type': 'client_credentials'}

        attachment_messages = context['ti'].xcom_pull(key='attachment_messages')

        c = 1
        length = len(attachment_messages)
        for msg in attachment_messages:
            print("sending attachment {0} of {1}".format(c, length))

            access_token_response = requests.post(token_url, data=data, verify=False, allow_redirects=False, auth=(client_id, client_secret))
            tokens = json.loads(access_token_response.text)
            print("access token: " + tokens['access_token'])
        
            # get ledger attachments for ledger id
            post_file_attachment_url = "{0}/api/FileAttachment/Json".format(metadata_gateway_conn.host)
            print("metadata gateway api host: " + post_file_attachment_url)
            api_call_headers = {'Content-type': 'application/json', 'Accept': 'text/plain', 'Authorization': 'Bearer ' + tokens['access_token']}

            request_payload = json.dumps(msg, default=lambda o: o.__dict__, indent=4)
            print("request payload: {0}".format(request_payload))

            api_call_response = requests.post(post_file_attachment_url, data=request_payload, headers=api_call_headers, verify=False)
            print("status code: {0}".format(api_call_response.status_code))
            print("content: {0}".format(api_call_response.content))            

            response = api_call_response.json()
            print(response)        


####----####

    is_ledger_valid = PythonOperator(
        task_id='is_ledger_valid',
        python_callable=is_ledger_valid,
        provide_context=True)

    get_schema_metrics = PythonOperator(
        task_id='get_schema_metrics',
        python_callable=get_schema_metrics,
        provide_context=True)

    get_metrics = PythonOperator(
        task_id='get_metrics',
        python_callable=get_metrics,
        provide_context=True)

    get_catalogue_path = PythonOperator(
        task_id='get_catalogue_path',
        python_callable=get_catalogue_path,
        provide_context=True)

    construct_metrics_catalogue_asset = PythonOperator(
       task_id='construct_metrics_catalogue_asset',
       python_callable=construct_metrics_catalogue_asset,
       provide_context=True)

    send_metrics_to_metadata_gateway = PythonOperator(
        task_id='send_metrics_to_metadata_gateway',
        python_callable=send_metrics_to_metadata_gateway,
        provide_context=True)

    is_send_attachments = BranchPythonOperator(
        task_id='is_send_attachments',
        python_callable=is_send_attachments,
        provide_context=True)

    get_ledger_attachments = PythonOperator(
        task_id='get_ledger_attachments',
        python_callable=get_ledger_attachments,
        provide_context=True)

    any_attachments = BranchPythonOperator(
        task_id='any_attachments',
        python_callable=any_attachments,
        provide_context=True)

    send_ledger_attachments_to_metadata_gateway = PythonOperator(
        task_id='send_ledger_attachments_to_metadata_gateway',
        python_callable=send_ledger_attachments_to_metadata_gateway,
        provide_context=True)

    send_message = PythonOperator(
        task_id="send_message",
        python_callable=send_success_message,
        provide_context=True,
        on_success_callback=None
    )

####----####

    is_ledger_valid >> get_schema_metrics
    get_schema_metrics >> get_metrics >> get_catalogue_path 
    get_catalogue_path >> construct_metrics_catalogue_asset >> send_metrics_to_metadata_gateway
    send_metrics_to_metadata_gateway >> is_send_attachments >> [get_ledger_attachments, send_message]
    get_ledger_attachments >> any_attachments >> [send_ledger_attachments_to_metadata_gateway, send_message]
    send_ledger_attachments_to_metadata_gateway >> send_message
    any_attachments >> send_message