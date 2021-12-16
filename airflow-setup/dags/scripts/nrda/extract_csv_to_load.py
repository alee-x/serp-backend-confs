from os.path import expanduser, join, abspath
import time
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, date_format, monotonically_increasing_id
from pyspark.sql.types import TimestampType, DateType
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
    ledger_json_file_path = sys.argv[3]
    delimiter = sys.argv[4]
    db_type = sys.argv[5]

    ledger_json_str = open(ledger_json_file_path, "r").read()
    ledger = json.loads(ledger_json_str)

    warehouse_location = abspath('spark-warehouse')

    # Initialize Spark Session
    spark = SparkSession \
        .builder \
        .appName("Extract csv from parquet for db data loading") \
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
    file_type = "parquet"
    if check_if_file_is_delta(file_location):
       file_type = "delta"
    print("Starting Parquet to dataframe of {0}...".format(file_type))
    start = time.time()
  
    df = spark.read.format(file_type).load(file_location)

    print("TimeTaken: ", time.time() - start)

    # convert timestamp and date type format to a db2 friendly one
    for item in df.schema:
        if isinstance(item.dataType, TimestampType):
           df = df.withColumn(item.name, date_format(item.name, 'yyyy-MM-dd HH:mm:ss'))
           print("Convert format of col {0} of timestamp type to yyyy-MM-dd HH:mm:ss for db2 loading".format(item.name))
        if isinstance(item.dataType, DateType):
           df = df.withColumn(item.name, date_format(item.name, 'yyyy-MM-dd'))
           print("Convert format of col {0} of date type to yyyy-MM-dd for db2 loading".format(item.name))

    csv_hdfs_path = os.path.join(os.sep, workdir, project_code, ledger["version"], "{0}.csv".format(ledger["label"]))
    print(csv_hdfs_path)
    
    if db_type == 'db2': # or db_type == 'pg':
        df.coalesce(1).write.csv(path=csv_hdfs_path,mode="overwrite",sep=delimiter,emptyValue='')
    else:
        df1 = df.withColumn("idx", monotonically_increasing_id())
        df1.createOrReplaceTempView('df1_idx')
        new_df = spark.sql('select row_number() over (order by "idx") as nrdav2_loading_rowid, * from df1_idx')
        new_df = new_df.drop("idx")
        spark.catalog.dropTempView('df1_idx')
        new_df.coalesce(1).write.csv(path=csv_hdfs_path,mode="overwrite",sep=delimiter,emptyValue='')
