import sys
import os
from subprocess import PIPE, Popen

def check_if_file_is_delta(location_details):
    location = os.path.join(location_details, "_delta_log")
    print("checking on path {0}".format(location))
    filexistchk="hdfs dfs -test -e "+location+";echo $?"
    #echo $? will print the exit code of previously execited command
    filexistchk_output=Popen(filexistchk,shell=True,stdout=PIPE).communicate()
    filechk="hdfs dfs -test -d "+location+";echo $?"
    filechk_output=Popen(filechk,shell=True,stdout=PIPE).communicate()
    #Check if location exists
    if '1' not in str(filexistchk_output[0]):
        #check if its a directory
        if '1' not in str(filechk_output[0]):
            print('The given ledger is delta lake file')
            return True
    
    print('The given ledger is not delta lake file')
    return False