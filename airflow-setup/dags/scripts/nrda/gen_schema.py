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

    workdir = sys.argv[1]
    project_code = sys.argv[2]
    ledger = json.loads(sys.argv[3])

    warehouse_location = abspath('spark-warehouse')

    # Initialize Spark Session
    spark = SparkSession \
        .builder \
        .appName("Load CSV to generate schema") \
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
    print(ledger)
    # File location and type
    file_location = ledger["location_details"]
    location_type = ledger["location"]

    if location_type.upper() == "HDFS-DB":        
        file_type = "parquet"
        if check_if_file_is_delta(file_location):
            file_type = "delta"
        df = spark.read.format(file_type).load(file_location)
        df.printSchema()
        
        # parquet_hdfs_path = os.path.join(os.sep, workdir, project_code, ledger["version"], "{0}.parquet".format(ledger["label"]))
        # df.write.mode('overwrite').parquet(parquet_hdfs_path)

        schemafile_hdfs = os.path.join(os.sep, workdir, project_code, ledger["version"], "schema_{0}.json".format(ledger["label"]))
        schemafile_local = os.path.join(os.sep, "tmp", "schema_{0}_{1}_{2}.json".format(project_code,ledger["version"],ledger["label"]))
        with open(schemafile_local, "w") as text_file:
            text_file.write(df.schema.json())

        # Store schema.json to hdfs    
        put = Popen(["hdfs", "dfs", "-mkdir", "-p", os.path.join(os.sep, workdir, project_code, ledger["version"])], stdin=PIPE, bufsize=-1)
        put.communicate()
        put = Popen(["hdfs", "dfs", "-put", "-f", schemafile_local, schemafile_hdfs], stdin=PIPE, bufsize=-1)
        put.communicate()
    else:
        file_type = "csv"
        # CSV options
        infer_schema = "true"
        first_row_is_header = ledger["attributes"]["has_header"]
        delimiter = ledger["attributes"]["delimiter"]
        charset = ledger["attributes"]["charset"] if 'charset' in ledger["attributes"] else "UTF-8"
        date_format = ledger["attributes"]["date_format"] if 'date_format' in ledger["attributes"] else "yyyy-MM-dd"
        timestamp_format = ledger["attributes"]["timestamp_format"] if 'timestamp_format' in ledger["attributes"] else "yyyy-MM-dd'T'HH:mm:ssZ"
        multi_line = ledger["attributes"]["multi_line"] if 'multi_line' in ledger["attributes"] else "false"

        df = spark.read.format(file_type) \
            .option("inferSchema", infer_schema) \
            .option("multiLine", multi_line.lower()) \
            .option("quote", "\"") \
            .option("escape", "\"") \
            .option("header", first_row_is_header) \
            .option("sep", delimiter) \
            .option("charset", charset) \
            .option("dateFormat", date_format) \
            .option("timestampFormat", timestamp_format) \
            .load(file_location)  # .persist(StorageLevel.MEMORY_ONLY)

        df.printSchema()

        parquet_hdfs_path = os.path.join(os.sep, workdir, project_code, ledger["version"], "{0}.parquet".format(ledger["label"]))
        df.write.mode('overwrite').parquet(parquet_hdfs_path)

        schemafile_hdfs = os.path.join(os.sep, workdir, project_code, ledger["version"], "schema_{0}.json".format(ledger["label"]))
        schemafile_local = os.path.join(os.sep, "tmp", "schema_{0}_{1}_{2}.json".format(project_code,ledger["version"],ledger["label"]))
        with open(schemafile_local, "w") as text_file:
            text_file.write(df.schema.json())

        # Store schema.json to hdfs    
        put = Popen(["hdfs", "dfs", "-mkdir", "-p", os.path.join(os.sep, workdir, project_code, ledger["version"])], stdin=PIPE, bufsize=-1)
        put.communicate()
        put = Popen(["hdfs", "dfs", "-put", "-f", schemafile_local, schemafile_hdfs], stdin=PIPE, bufsize=-1)
        put.communicate()
        #os.remove(schemafile_local)
