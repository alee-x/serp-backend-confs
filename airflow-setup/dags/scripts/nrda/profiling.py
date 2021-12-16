from os.path import expanduser, join, abspath
import sys
import os
import json
from pyspark.sql import SparkSession
import spark_df_profiling
from subprocess import PIPE, Popen
from utils import check_if_file_is_delta

if __name__ == "__main__":

    for arg in sys.argv[1:]:
        print("var: " + arg)

    workdir = sys.argv[1]
    project_code = sys.argv[2]
    ledger_json_file_path = sys.argv[3]
    run_id = sys.argv[4]

    ledger_json_str = open(ledger_json_file_path, "r").read()
    ledger = json.loads(ledger_json_str)

    warehouse_location = abspath('spark-warehouse')

    # Initialize Spark Session
    spark = SparkSession \
        .builder \
        .appName("Profling") \
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
    location_type = ledger["location"]
    file_location = ledger["location_details"]
    df = None

    if location_type.upper() == "HDFS-DB":
        file_type = "parquet"
        if check_if_file_is_delta(file_location):
            file_type = "delta"
        df = spark.read.format(file_type).load(file_location)
    else:
        file_type = "csv"

        # CSV options
        infer_schema = "true"
        first_row_is_header = ledger["attributes"]["has_header"]
        delimiter = ledger["attributes"]["delimiter"]
        charset = ledger["attributes"]["charset"] if 'charset' in ledger["attributes"] else "UTF-8"
        date_format = ledger["attributes"]["date_format"] if 'date_format' in ledger["attributes"] else "yyyy-MM-dd"
        timestamp_format = ledger["attributes"]["timestamp_format"] if 'timestamp_format' in ledger["attributes"] else "yyyy-MM-dd'T'HH:mm:ssZ"

        df = spark.read.format(file_type) \
            .option("inferSchema", infer_schema) \
            .option("header", first_row_is_header) \
            .option("sep", delimiter) \
            .option("charset", charset) \
            .option("dateFormat", date_format) \
            .option("timestampFormat", timestamp_format) \
            .load(file_location)


    result = spark_df_profiling.ProfileReport(df)

    result_local = os.path.join(os.sep, "tmp", "profiling_report_{0}.html".format(run_id))
    
    with open(result_local, "w") as text_file:
        text_file.write(result.rendered_html())

    print("local path: {0}".format(result_local))