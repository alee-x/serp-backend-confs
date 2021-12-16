from os.path import expanduser, join, abspath
import sys
import os
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import *
import json
import os
import getpass
from subprocess import PIPE, Popen

if __name__ == "__main__":

    for arg in sys.argv[1:]:
        print("var: " + arg)

    project_code = sys.argv[1]
    ledger_json_file_path = sys.argv[2]
    filtered_template_list = json.loads(sys.argv[3])
    ledger_json_str = open(ledger_json_file_path, "r").read()
    ledgers = json.loads(ledger_json_str)
    warehouse_location = abspath('spark-warehouse')

    # Initialize Spark Session
    spark = SparkSession \
        .builder \
        .appName("Provisioning from staging to staging") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .getOrCreate()
    
    for ledger in ledgers:
        ledger_table_name = "base_{0}_{1}_{2}".format(project_code,ledger["label"].lower(),ledger["version"]) #has to be some format
        if ledger["location_details"].endswith(".parquet"): 
            df = spark.read.format("parquet").load(ledger["location_details"])
            spark.catalog.dropTempView(ledger_table_name)
            df.createOrReplaceTempView(ledger_table_name)
        else:
            df = spark.read.format("parquet").table(ledger["location_details"])
            spark.catalog.dropTempView(ledger_table_name)
            df.createOrReplaceTempView(ledger_table_name)
        # loop through each chosen template and apply the sql on staging to create view or table
    
    for template in filtered_template_list:
        print(template)
        source_ledger = next((l for l in ledgers if l['label'].lower() == template["source_label"].lower() and l['classification'].lower() == template["source_classification"].lower()), None)
        file_location = source_ledger["location_details"]
        version = source_ledger["version"]
        classification = source_ledger["classification"]
        label = source_ledger["label"]
        target_label = source_ledger["attributes"]["target_label_name"].format(label = label, classification = classification, version = version)
        provisioned_file_hdfs_path = os.path.join(os.sep, "stage", project_code,"provisioned","{0}.parquet".format(target_label))
        sql_stm = template["provision_sql_query"].replace("[queryTable1]","base_{project_code}_{userTable1}_{version}").replace("[queryTable2]","base_{project_code}_{userTable2}_{version}").format(project_code = project_code, userTable1 = template["provisioning_tabel_names"]["userTable1"], userTable2 = template["provisioning_tabel_names"]["userTable2"], label = label, classification = classification, version = version)
        print(sql_stm)        
        if source_ledger["attributes"]["is_provisioned_view"].lower() == "false":
            provisioned_df = spark.sql(sql_stm)
            provisioned_df.printSchema()
            provisioned_df.write.format("parquet").mode('overwrite').parquet(provisioned_file_hdfs_path)
        else:
            target_table = source_ledger["attributes"]["target_label_name"].format(label = label, classification = classification, version = version)
            provisioned_df = spark.sql(sql_stm)
            provisioned_df.printSchema()
            provisioned_df.write.format("parquet").mode('overwrite').option("path", provisioned_file_hdfs_path).saveAsTable("nrda_{0}_{1}".format(project_code.lower(),target_table))
    #housekeeping
    for ledger in ledgers:
        ledger_table_name = "base_{0}_{1}_{2}".format(project_code,ledger["label"].lower(),ledger["version"]) #has to be some format
        spark.catalog.dropTempView(ledger_table_name)

    os.remove(ledger_json_file_path)
    print("ledger json file {0} has been deleted".format(ledger_json_file_path))