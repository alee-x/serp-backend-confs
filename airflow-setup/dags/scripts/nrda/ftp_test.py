import os
import glob
import paramiko
from airflow.hooks.base_hook import BaseHook
from stat import S_ISDIR
from time import process_time
import datetime
import errno
import time
from subprocess import PIPE, Popen


def sftp_get_recursive(path, sftp):
    item_list = sftp.listdir(path)
    for item in item_list:
        item = str(item)
        if S_ISDIR(sftp.stat(path + "/" + item).st_mode):
            sftp_get_recursive(path + "/" + item,sftp)
        else:
            #sftp.get(path + "/" + item, dest + "/" + item)
            if(not item.lower().endswith(".filepart")):
               print(path + "/" + item)
               sftp_to_delete(path + "/" + item, sftp)

def sftp_to_delete(sftp_path, sftp):
    #Remove sftp file
    sftp.remove(sftp_path)

def sftp_to_hdfs(sftp_path, sftp):
    localfilePath = '/tmp/sftp_to_hdfs{0}'.format(sftp_path)
    if not os.path.exists(os.path.dirname(localfilePath)):
        try:
            os.makedirs(os.path.dirname(localfilePath))
            print("Local directory {0} has been created.".format(os.path.dirname(localfilePath)))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    print("Downloading SFTP file from {0} ...".format(sftp_path))
    #Download sftp file to local
    start = process_time()   
    sftp.get(sftp_path,localfilePath)
    elapsed = process_time() 
    elapsed = elapsed - start
    print ("SFTP file {0} has been successfully downloaded to local {1}. (Time spent: {2})".format(sftp_path, localfilePath, str(datetime.timedelta(seconds = elapsed)) ))
    #Upload to downloaded file to HDFS
    hdfsfilePath = '/frontdoor/sftp_to_hdfs{0}'.format(sftp_path)
    timeStarted = time.time()        
    put = Popen(["hdfs", "dfs", "-mkdir", "-p", os.path.dirname(hdfsfilePath)], stdin=PIPE, bufsize=-1)
    put.communicate()
    put = Popen(["hdfs", "dfs", "-put", "-f", localfilePath, hdfsfilePath], stdin=PIPE, bufsize=-1)
    put.communicate()   
    timeDelta = time.time() - timeStarted                     # Get execution time.
    print("Finished process in "+str(timeDelta)+" seconds.")  # Output result.
    print ("Local file {0} has been successfully uploaded to HDFS {1}. ".format(localfilePath, hdfsfilePath))
    #Send rabbit msg
    # send_progress_message(sftp_path, hdfsfilePath, vhost['vhost'])
    #Remove local file
    os.remove(localfilePath)
    #Remove sftp file
    # sftp.remove(sftp_path)
    print ("SFTP file {0} has been successfully deleted. ".format(sftp_path))

def ftp_test(**context):
    ftp_conn = BaseHook.get_connection('ftp_default')
    transport = paramiko.Transport((ftp_conn.host, ftp_conn.port))
    transport.connect(username = ftp_conn.login, password = ftp_conn.password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp_get_recursive("",sftp)
