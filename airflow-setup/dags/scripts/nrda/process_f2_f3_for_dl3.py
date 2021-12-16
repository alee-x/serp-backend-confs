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

if __name__ == "__main__":

    for arg in sys.argv[1:]:
        print("var: " + arg)

    project_code = sys.argv[1]
    f2_ledger_json_hdfs_path = sys.argv[2]
    f3_ledger_json_hdfs_path = sys.argv[3]
    f3_map_ledger_json_hdfs_path = sys.argv[4]
    jobtask_id = sys.argv[5]
 
    # Get f2 ledger file from hdfs
    f2_ledger_json_local_path = os.path.join(os.sep, "tmp", os.path.basename(f2_ledger_json_hdfs_path))
    get = Popen(["hdfs", "dfs", "-get", f2_ledger_json_hdfs_path, f2_ledger_json_local_path], stdin=PIPE, bufsize=-1)
    get.communicate()
    f2_ledger_json_str = open(f2_ledger_json_local_path, "r").read()
    f2_ledger = json.loads(f2_ledger_json_str)
    os.remove(f2_ledger_json_local_path)
    # Get f3 ledger file from hdfs
    f3_ledger_json_local_path = os.path.join(os.sep, "tmp", os.path.basename(f3_ledger_json_hdfs_path))
    get = Popen(["hdfs", "dfs", "-get", f3_ledger_json_hdfs_path, f3_ledger_json_local_path], stdin=PIPE, bufsize=-1)
    get.communicate()
    f3_ledger_json_str = open(f3_ledger_json_local_path, "r").read()
    f3_ledger = json.loads(f3_ledger_json_str)
    os.remove(f3_ledger_json_local_path)
    # Get f3-map ledger file from hdfs
    f3_map_ledger_json_local_path = os.path.join(os.sep, "tmp", os.path.basename(f3_map_ledger_json_hdfs_path))
    get = Popen(["hdfs", "dfs", "-get", f3_map_ledger_json_hdfs_path, f3_map_ledger_json_local_path], stdin=PIPE, bufsize=-1)
    get.communicate()
    f3_map_ledger_json_str = open(f3_map_ledger_json_local_path, "r").read()
    f3_map_ledger = json.loads(f3_map_ledger_json_str)
    os.remove(f3_map_ledger_json_local_path)

    warehouse_location = abspath('spark-warehouse')

    # Initialize Spark Session
    spark = SparkSession \
        .builder \
        .appName("Process F2 and F3 for DL3") \
        .config("spark.jars.packages", "io.delta:delta-core_2.11:0.6.1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .enableHiveSupport() \
        .getOrCreate()
        
    spark.sparkContext.addPyFile("/usr/local/spark/jars/io.delta_delta-core_2.11-0.6.1.jar")
    # Apache Spark 2.4.x has a known issue (SPARK-25003) that requires explicit activation
    # of the extension and cloning of the session. This will unnecessary in Apache Spark 3.x.
    if spark.sparkContext.version < "3.":
        spark.sparkContext._jvm.io.delta.sql.DeltaSparkSessionExtension() \
            .apply(spark._jsparkSession.extensions())
        spark = SparkSession(spark.sparkContext, spark._jsparkSession.cloneSession())

    print(project_code)
    # version = f2_ledger["version"]
    # label = f2_ledger["attributes"]["targettablename"].format(version = version)
    # output_file_hdfs_path = os.path.join(os.sep, "stage", project_code, version,"dl3","{0}.parquet".format(label))
    f2_view = "f2_{0}".format(jobtask_id)
    f3_view = "f3_{0}".format(jobtask_id)
    f3_map_view = "f3_map_{0}".format(jobtask_id)

    if f2_ledger["location"].upper() == "HDFS-DB" and f3_ledger["location"].upper() == "HDFS-DB" and f3_map_ledger["location"].upper() == "HDFS-DB":        
        if f2_ledger["attributes"]["usenewid"].lower() == "false":
            # load f3 to temp view
            file_type = "parquet"
            if check_if_file_is_delta(f3_ledger["location_details"]):
                file_type = "delta"       
            f3_df = spark.read.format(file_type).load(f3_ledger["location_details"])
            f3_df.printSchema()
            f3_df.createOrReplaceTempView(f3_view)
            # load f3-map to temp view
            file_type = "parquet"
            if check_if_file_is_delta(f3_map_ledger["location_details"]):
                file_type = "delta"       
            f3_map_df = spark.read.format(file_type).load(f3_map_ledger["location_details"])
            f3_map_df.printSchema()
            f3_map_df.createOrReplaceTempView(f3_map_view)
            #File 3 with values as ORIGINAL IDs
            sql_stm = 'SELECT b.Id, Alf, Score	FROM {f3} a INNER JOIN {f3_map} b ON a.Id = b.DSxID ORDER BY a.Id'.format(f3 = f3_view, f3_map=f3_map_view)
            transformed_f3 = spark.sql(sql_stm)
            transformed_f3.printSchema()
            transformed_f3_hdfs_path = os.path.join(os.sep, "stage", project_code, f3_ledger["version"], "dl3", "transformed_f3.parquet")
            transformed_f3.write.mode('overwrite').option("overwriteSchema", "true").format("delta").save(transformed_f3_hdfs_path)
        else:
            # load f2 to temp view
            file_type = "parquet"
            if check_if_file_is_delta(f2_ledger["location_details"]):
                file_type = "delta"       
            f2_df = spark.read.format(file_type).load(f2_ledger["location_details"])
            f2_df.printSchema()
            f2_df.createOrReplaceTempView(f2_view)
            # load f3-map to temp view
            file_type = "parquet"
            if check_if_file_is_delta(f3_map_ledger["location_details"]):
                file_type = "delta"       
            f3_map_df = spark.read.format(file_type).load(f3_map_ledger["location_details"])
            f3_map_df.printSchema()
            f3_map_df.createOrReplaceTempView(f3_map_view)
            #File 3 with values as ORIGINAL IDs
            sql_stm = 'SELECT b.DSxID, a.* FROM {f2} a INNER JOIN {f3_map} b ON a.Id = b.Id ORDER BY b.DSxID'.format(f2 = f2_view, f3_map=f3_map_view)
            transformed_f2 = spark.sql(sql_stm)
            transformed_f2.printSchema()
            transformed_f2 = transformed_f2.drop('Id')
            transformed_f2 = transformed_f2.withColumnRenamed('DSxID', 'Id')
            transformed_f2.printSchema()
            transformed_f2_hdfs_path = os.path.join(os.sep, "stage", project_code, f3_ledger["version"], "dl3", "transformed_f2.parquet")
            transformed_f2.write.mode('overwrite').option("overwriteSchema", "true").format("delta").save(transformed_f2_hdfs_path)       
        
    else:
        print("Process F2 and F3 for DL3 only takes parquet files")
        raise Exception("Process F2 and F3 for DL3 only takes parquet files")
