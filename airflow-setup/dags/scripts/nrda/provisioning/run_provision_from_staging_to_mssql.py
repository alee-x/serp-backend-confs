from os.path import expanduser, join, abspath
import sys
import os
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, date_format
from pyspark.sql.types import *
import json
import os
import getpass
from subprocess import PIPE, Popen

if __name__ == "__main__":

    for arg in sys.argv[1:]:
        print("var: " + arg)

    project_code = sys.argv[1]
    ledgers = json.loads(sys.argv[2])
    filtered_template_list = json.loads(sys.argv[3])
    workdir = "staging"
    warehouse_location = abspath('spark-warehouse')

    # Initialize Spark Session
    spark = SparkSession \
        .builder \
        .appName("Create staging database and tables for provisioning") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .getOrCreate()
    
    database = "nrda_{0}".format(project_code.lower())
    # create project database first if it doesn't exist - TODO: secruity should be applied in future
    spark.sql("CREATE DATABASE IF NOT EXISTS {0} LOCATION '/project_dbs/{1}'".format(database,project_code))
    # create tables for all given ledgers in this version for provisioning query later
    for ledger in ledgers:
        ledger_table_name = "base_{0}_{1}".format(ledger["label"].lower(),ledger["version"]) #has to be some format
        spark.sql("DROP TABLE IF EXISTS {0}.{1}".format(database,ledger_table_name))
        spark.sql("CREATE TABLE IF NOT EXISTS {0}.{1} USING PARQUEt LOCATION '{2}'".format(database,ledger_table_name,ledger["location_details"]))

    # loop through each chosen template and apply the sql on staging to create view or table
    for template in filtered_template_list:
        print(template)
        source_ledger = next((l for l in ledgers if l['label'].lower() == template["source_label"].lower() and l['classification'].lower() == template["source_classification"].lower()), None)
        df = spark.sql(template["provision_sql_query"].format(project_db = database, label = source_ledger['label'], classification = source_ledger['classification'], version = source_ledger['version']))
        df.printSchema()

        # convert timestamp and date type format to a db2 friendly one
        for item in df.schema:
            if isinstance(item.dataType, TimestampType):
               df = df.withColumn(item.name, date_format(item.name, 'yyyy-MM-dd HH:mm:ss'))
               print("Convert format of col {0} of timestamp type to yyyy-MM-dd HH:mm:ss for db loading".format(item.name))
            if isinstance(item.dataType, DateType):
               df = df.withColumn(item.name, date_format(item.name, 'yyyy-MM-dd'))
               print("Convert format of col {0} of date type to yyyy-MM-dd for db loading".format(item.name))
        # extract df to csv for db loading
        csv_hdfs_path = os.path.join(os.sep, workdir, project_code, source_ledger["version"], "{0}_to_provision.csv".format(source_ledger["label"]))
        df.coalesce(1).write.csv(path=csv_hdfs_path,mode="overwrite",emptyValue='')
        # store schema of df to generate table on db
        schemafile_hdfs = os.path.join(os.sep, workdir, project_code, source_ledger["version"], "schema_{0}_to_provision.json".format(source_ledger["label"]))
        schemafile_local = os.path.join(os.sep, "tmp", "schema_{0}_{1}_{2}_to_provision.json".format(project_code,source_ledger["version"],source_ledger["label"]))
        with open(schemafile_local, "w") as text_file:
            text_file.write(df.schema.json())

        # Store schema.json to hdfs    
        put = Popen(["hdfs", "dfs", "-mkdir", "-p", os.path.join(os.sep, workdir, project_code, source_ledger["version"])], stdin=PIPE, bufsize=-1)
        put.communicate()
        put = Popen(["hdfs", "dfs", "-put", "-f", schemafile_local, schemafile_hdfs], stdin=PIPE, bufsize=-1)
        put.communicate()
        #os.remove(schemafile_local)