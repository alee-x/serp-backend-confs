import sys
import os
import json
# store payload to file to avoid issue of oversized arg for spark-submit
def store_ledger_to_file(**context):
    print("Received {} for key=message".format(context["dag_run"].conf))
    print("job task id: {0} for project {1}".format(context['dag_run'].conf['jobtask_id'], context['dag_run'].conf['project_code'] ))
    souce_ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']
    print("source ledger: {0}".format(json.dumps(souce_ledger)))
    version = souce_ledger["version"]

    # create a directory for this dag run
    local_dir = os.path.join(os.sep, "tmp", context['dag_run'].run_id)
    os.makedirs(local_dir,exist_ok = True)
    print("local dir: ", local_dir)
    context['ti'].xcom_push(key='local_dir', value=local_dir)


    print(context['dag_run'])
    print(context['execution_date'])

    ledger_json_file_name = os.path.join(local_dir, "ledger_{0}_{1}_{2}_{3}.json".format(project_code, version, souce_ledger["label"], souce_ledger["id"]))
    with open(ledger_json_file_name, 'w') as fp:
        json.dump(souce_ledger, fp)
    print("ledger json file name: {0}".format(ledger_json_file_name))
    context['ti'].xcom_push(key='ledger_json_file_name', value=ledger_json_file_name)