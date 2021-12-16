from os.path import expanduser, join, abspath
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
import json
import os
import getpass
from subprocess import PIPE, Popen
from utils import check_if_file_is_delta

# my added
from pyspark import SparkConf

if __name__ == "__main__":

    for arg in sys.argv[1:]:
        print("var: " + arg)

    project_code = sys.argv[1]
    ledgers_json_hdfs_path = sys.argv[2]    
    file_name = os.path.join(os.sep, "tmp", os.path.basename(ledgers_json_hdfs_path))      
    # Get schema metadata file from hdfs
    get = Popen(["hdfs", "dfs", "-get", ledgers_json_hdfs_path, file_name], stdin=PIPE, bufsize=-1)
    get.communicate()
    ledgers_json_str = open(file_name, "r").read()
    ledgers = json.loads(ledgers_json_str)
    os.remove(file_name)

    # warehouse_location = abspath('spark-warehouse')
    warehouse_location = "hdfs://namenode:9000/user/hive/warehouse"
    print(warehouse_location)

    settings = [
            ("hive.metastore.uris", "thrift://hive-metastore:9083"),
            ("hive.server2.thrift.http.path", "cliservice"),
            ("datanucleus.autoCreateSchema", "false"),
            ("javax.jdo.option.ConnectionURL", "jdbc:postgresql://hive-metastore-postgresql/metastore"),
            ("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver"),
            ("javax.jdo.option.ConnectionPassword", "hive"),
            ("hive.server2.transport.mode", "http"),
            ("hive.server2.thrift.max.worker.threads", "5000"),
            ("javax.jdo.option.ConnectionUserName", "hive"),
            ("hive.server2.thrift.http.port", "10000"),
            ("hive.server2.enable.doAs", "false"),
            ("hive.metastore.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse"),
            ("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0"),
            ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
            ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
            ("spark.driver.extraJavaOptions", "-Divy.cache.dir=/tmp -Divy.home=/tmp")
        ]

    spark_conf = SparkConf().setAppName("Combine files to one").setAll(settings)
    
    # Initialize Spark Session
    spark = SparkSession \
        .builder \
        .master("spark://spark-master:7077") \
        .config(conf = spark_conf) \
        .enableHiveSupport() \
        .getOrCreate()

    # Initialize Spark Session
    # spark = SparkSession \
    #     .builder \
    #     .appName("Combine files to one") \
    #     .config("spark.jars.packages", "io.delta:delta-core_2.11:0.6.1") \
    #     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    #     .config("spark.sql.warehouse.dir", warehouse_location) \
    #     .enableHiveSupport() \
    #     .getOrCreate()
        
    # spark.sparkContext.addPyFile("/usr/local/spark/jars/io.delta_delta-core_2.11-0.6.1.jar")
    # Apache Spark 2.4.x has a known issue (SPARK-25003) that requires explicit activation
    # of the extension and cloning of the session. This will unnecessary in Apache Spark 3.x.
    if spark.sparkContext.version < "3.":
        spark.sparkContext._jvm.io.delta.sql.DeltaSparkSessionExtension() \
            .apply(spark._jsparkSession.extensions())
        spark = SparkSession(spark.sparkContext, spark._jsparkSession.cloneSession())

    print(project_code)
    print("total ledgers: {0}".format(len(ledgers)))    
    souce_ledger = ledgers[0]
    print("first ledger: {0}".format(souce_ledger))
    version = souce_ledger["version"]
    classification = souce_ledger["classification"]
    label = souce_ledger["attributes"]["targettablename"].format(classification = classification, version = version)
    combined_stage_hdfs_path = os.path.join(os.sep, "stage", project_code, version,"combined","{0}.parquet".format(label))
    
    
    ledger_path_list = []
    for ledger in ledgers:
        ledger_path_list.append(ledger["location_details"])
    
    location_type = souce_ledger["location"]
    file_location = souce_ledger["location_details"]
    row_count = 0
    if location_type.upper() == "HDFS-DB":
        
        if check_if_file_is_delta(file_location):
            file_type = "delta" 
            print("Combine databrick delta lake files")
            # house keeping to make sure combined file is for this version
            housekeeping = Popen(["hdfs", "dfs", "-rm", "-r", combined_stage_hdfs_path], stdin=PIPE, bufsize=-1)
            housekeeping.communicate()           
            for ledger in ledgers:
                df = spark.read.format(file_type).load(ledger["location_details"])
                df.write.mode('append').option("mergeSchema", "true").format("delta").save(combined_stage_hdfs_path)     
            df_com = spark.read.format(file_type).load(combined_stage_hdfs_path)
            row_count = df_com.count()
        else:
            file_type = "parquet"
            print("Combine spark parquet files")
            df = spark.read.format(file_type).load(ledger_path_list)
            df.write.mode('overwrite').option("overwriteSchema", "true").format("delta").save(combined_stage_hdfs_path)
            row_count = df.count()
    else:
        # CSV options
        print("Combine txt/csv files")
        combined_stage_hdfs_path = os.path.join(os.sep, "stage", project_code, version,"combined","{0}.csv".format(label))
        mkdir = Popen(["hdfs", "dfs", "-mkdir", "-p", combined_stage_hdfs_path], stdin=PIPE, bufsize=-1)
        mkdir.communicate()
        for ledger in ledgers:
            cp = Popen(["hdfs", "dfs", "-cp", ledger["location_details"], combined_stage_hdfs_path], stdin=PIPE, bufsize=-1)
            cp.communicate()
        df_com = spark.read.format("csv").load(combined_stage_hdfs_path)
        row_count = df_com.count()    
    
    print("row counts: {0}".format(row_count))
    # Store staging row count file to hdfs    
    rowcount_stage_hdfs_path = os.path.join(os.sep, "stage", project_code, version,"combined", "rowcount_{0}.txt".format(label))
    rowcount_local = os.path.join(os.sep, "tmp", "rowcount_{0}_{1}_{2}_{3}.txt".format(project_code,version,"combined",label))
    with open(rowcount_local, "w") as text_file:
        text_file.write('{}'.format(row_count))

    put = Popen(["hdfs", "dfs", "-put", "-f", rowcount_local, rowcount_stage_hdfs_path], stdin=PIPE, bufsize=-1)
    put.communicate()
    os.remove(rowcount_local) 
