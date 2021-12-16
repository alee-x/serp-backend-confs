import os
import glob
from subprocess import PIPE, Popen
def sed_content(**context):
    ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']
    print(ledger)
    
    csv_file = context['ti'].xcom_pull(key='csv_toload_filename')    
    print("csv file name: {0}".format(csv_file))
    # sed to remove quotes (\"")
    tmp_file = os.path.join(os.sep, "tmp", "{0}_{1}_{2}.csv".format(project_code,ledger["label"],ledger["version"]))
    print("tmp file name: {0}".format(tmp_file))
    sed = Popen(["cat", csv_file, "|","sed", "s/\\\\\"//g", ">", tmp_file], stdin=PIPE, bufsize=-1)
    sed.communicate()
    # replace original file
    os.remove(csv_file)
    mv = Popen(["mv", tmp_file, csv_file], stdin=PIPE, bufsize=-1)
    mv.communicate()
    context['ti'].xcom_push(key='csv_toload_filename', value=csv_file)