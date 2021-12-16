import os
import glob
from subprocess import PIPE, Popen
def download_csv_to_load(**context):
    ledger = context['dag_run'].conf['ledger']
    project_code = context['dag_run'].conf['project_code']
    print(ledger)
    
    local_dir = context['ti'].xcom_pull(key='local_dir')

    csv_hdfs_dir = os.path.join(os.sep, "stage", project_code,ledger["version"],"{0}.csv".format(ledger["label"]))
    
    # download the directory from hdfs
    put = Popen(["hdfs", "dfs", "-copyToLocal","-f", csv_hdfs_dir, local_dir], stdin=PIPE, bufsize=-1)
    put.communicate()
    # get csv file name
    csv_file = glob.glob("{0}/{1}.csv/*.csv".format(local_dir, ledger["label"]))[0]
    cleaned_csv_file = "{0}/data.csv".format(local_dir)
    # csv_file = "/tmp/df_assessments.csv"
    print("csv file name: {0}".format(csv_file))
    print("cleaned csv file name: {0}".format(cleaned_csv_file))
    context['ti'].xcom_push(key='csv_toclean_filename', value=csv_file)
    context['ti'].xcom_push(key='csv_toload_filename', value=cleaned_csv_file)
    # raise Exception("fake exception to stop the flow")