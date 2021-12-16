from os.path import expanduser, join, abspath
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, udf, expr, concat, col, to_date, to_timestamp
import json
import os
import getpass
from subprocess import PIPE, Popen
from utils import check_if_file_is_delta

if __name__ == "__main__":

    for arg in sys.argv[1:]:
        print("var: " + arg)

    schema_cols_to_rename = json.loads(sys.argv[1])
    schema_cols_to_exclude = json.loads(sys.argv[2])
    schema_cols_to_enc = json.loads(sys.argv[3])
    cols_type_changed = json.loads(sys.argv[4])
    project_code = sys.argv[5]
    ledger = json.loads(sys.argv[6])

    warehouse_location = abspath('spark-warehouse')

    # Initialize Spark Session
    spark = SparkSession \
        .builder \
        .appName("Apply rename, drop, and enc transformation") \
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
    
    file_location = ledger["location_details"]
    location_type = ledger["location"]
    is_delta = check_if_file_is_delta(file_location)

    file_type = "parquet"
    if location_type.upper() == "HDFS-DB" and is_delta:
       file_type = "delta"
    else:
        if location_type.upper() == "HDFS":
            file_location = os.path.join(os.sep, "tmp", project_code, ledger["version"], "{0}.parquet".format(ledger["label"]))

    print("file_type: " + file_type + " || file_location: " + file_location)
    df = spark.read.format(file_type).load(file_location)

    if(schema_cols_to_rename):
        if isinstance(schema_cols_to_rename, dict):
            for old_name, new_name in schema_cols_to_rename.items():
                df = df.withColumnRenamed(old_name, new_name)
        else:
            raise ValueError("'columns' should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}")

    if(schema_cols_to_exclude):
        for col in schema_cols_to_exclude:
            df = df.drop(col)
    
    print("cols_type_changed:")
    print(cols_type_changed)
    
    if(cols_type_changed):
        for col in cols_type_changed:
            if col["type"] == "date":
                # expStr = "TO_DATE(CAST(UNIX_TIMESTAMP({0}, {1}) AS TIMESTAMP))".format(col["name"],ledger["attributes"]["date_format"])
                # print(expStr)
                df = df.withColumn(col["name"], to_date(col["name"], ledger["attributes"]["date_format"]))
            elif col["type"] == "timestamp":
                # expStr = "CAST(UNIX_TIMESTAMP({0}, {1}) AS TIMESTAMP)".format(col["name"],ledger["attributes"]["timestamp_format"])
                # print(expStr)
                df = df.withColumn(col["name"], to_timestamp(col["name"], ledger["attributes"]["timestamp_format"]))
            else: 
                expStr = "CAST({0} AS {1})".format(col["name"],col["type"])
                print(expStr)
                df = df.withColumn(col["name"], expr(expStr))

    
    transformed_parquet_hdfs_path = os.path.join(os.sep, "tmp", project_code, ledger["version"], "{0}_transformed.parquet".format(ledger["label"]))

    print("transformed_parquet_hdfs_path")
    print(transformed_parquet_hdfs_path)

    df.write.mode('overwrite').parquet(transformed_parquet_hdfs_path)
    # House keeping - delete source tmp file
    if location_type.upper() == "HDFS":
       housekeeping = Popen(["hdfs", "dfs", "-rm", "-r", file_location], stdin=PIPE, bufsize=-1)
       housekeeping.communicate()
