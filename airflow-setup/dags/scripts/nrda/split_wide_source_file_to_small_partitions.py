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

def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

if __name__ == "__main__":

    for arg in sys.argv[1:]:
        print("var: " + arg)

    workdir = sys.argv[1]
    project_code = sys.argv[2]
    ledger_json_file_path = sys.argv[3]
    local_dir = sys.argv[4]
    ledger_json_str = open(ledger_json_file_path, "r").read()
    ledger = json.loads(ledger_json_str)


    warehouse_location = abspath('spark-warehouse')

    # Initialize Spark Session
    spark = SparkSession \
        .builder \
        .appName("split_wide_source_file_to_small_partitions") \
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
        first_row_is_header = ledger["attributes"]["has_header"] if 'has_header' in ledger["attributes"] else "true"
        delimiter = ledger["attributes"]["delimiter"] if 'delimiter' in ledger["attributes"] else ","
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
    # get key fields
    key_fields = []
    if 'keyfields' in ledger["attributes"] and ledger["attributes"]["keyfields"]:
       key_fields = [key.strip() for key in ledger["attributes"]["keyfields"].split(",")] 
    # get partitions size
    partition_size = int(ledger["attributes"]["splitpartitionsize"]) - len(key_fields)
    # get list fiedls without key fields
    de_key_fields = df.schema.names
    for y in key_fields:
       de_key_fields.remove(y)
    # generate partitions
    index = 1
    for partition in batch(de_key_fields, partition_size):
        result_cols = key_fields + partition
        print(result_cols)
        put = Popen(["hdfs", "dfs", "-mkdir", "-p", os.path.join(os.sep, workdir, project_code, ledger["version"])], stdin=PIPE, bufsize=-1)
        put.communicate()
        parquet_hdfs_path = os.path.join(os.sep, workdir, project_code, ledger["version"], "{0}_{1}.parquet".format(ledger["label"], index))
        df.select(result_cols).write.mode('overwrite').parquet(parquet_hdfs_path)
        index = index + 1

    # Store number of partitions 
    partition_num_local = os.path.join(local_dir, "partition_num_{0}_{1}_{2}.txt".format(project_code,ledger["version"],ledger["label"]))
    print(partition_num_local)
    with open(partition_num_local, "w") as text_file:
        text_file.write('{0}'.format(index-1))


