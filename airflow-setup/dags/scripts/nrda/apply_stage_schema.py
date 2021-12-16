from os.path import expanduser, join, abspath
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import *
import json
import os
import getpass
from subprocess import PIPE, Popen
from utils import check_if_file_is_delta

if __name__ == "__main__":

    for arg in sys.argv[1:]:
        print("var: " + arg)

    transformation_applied = sys.argv[1]
    project_code = sys.argv[2]
    ledger = json.loads(sys.argv[3])
    target_label = sys.argv[4]

    warehouse_location = abspath('spark-warehouse')

    # Initialize Spark Session
    spark = SparkSession \
        .builder \
        .appName("Apply the final schema and convert to parquet") \
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

    file_location = os.path.join(os.sep, "tmp", project_code, ledger["version"], "{0}_transformed.parquet".format(ledger["label"]))
    file_type = "parquet"
    if(transformation_applied.lower() != 'true'):
        if ledger["location"].upper() == "HDFS-DB" and check_if_file_is_delta(ledger["location_details"]):
            file_type = "delta"
            file_location = ledger["location_details"]
        elif ledger["location"].upper() == "HDFS":
            file_location = os.path.join(os.sep, "tmp", project_code, ledger["version"], "{0}.parquet".format(ledger["label"])) 
        else:
            file_location = ledger["location_details"]
            #file_location = os.path.join(os.sep, "tmp", project_code, ledger["version"], "{0}.parquet".format(ledger["label"]))

    print("file_location")
    print(file_location)

    df = spark.read.format(file_type).load(file_location)   
    df.printSchema()
    # Store staging row count file to hdfs    
    rowcount_stage_hdfs_path = os.path.join(os.sep, "stage", project_code, ledger["version"], "rowcount_{0}.txt".format(target_label))
    rowcount_local = os.path.join(os.sep, "tmp", "rowcount_{0}_{1}_{2}.txt".format(project_code,ledger["version"],target_label))
    with open(rowcount_local, "w") as text_file:
        text_file.write('{}'.format(df.count()))

    put = Popen(["hdfs", "dfs", "-mkdir", "-p", os.path.join(os.sep,"stage", project_code, ledger["version"])], stdin=PIPE, bufsize=-1)
    put.communicate()
    put = Popen(["hdfs", "dfs", "-put", "-f", rowcount_local, rowcount_stage_hdfs_path], stdin=PIPE, bufsize=-1)
    put.communicate()
    os.remove(rowcount_local) 
    
    # Store staging delta parquet to hdfs
    delta_stage_hdfs_path = os.path.join(os.sep, "stage", project_code,ledger["version"],"{0}.parquet".format(target_label))
    df.write.mode('overwrite').option("overwriteSchema", "true").format("delta").save(delta_stage_hdfs_path)
    # House keeping - delete intern files
    
    housekeeping = Popen(["hdfs", "dfs", "-rm", "-r", os.path.join(os.sep, "tmp", project_code, ledger["version"], "{0}_transformed.parquet".format(ledger["label"]))], stdin=PIPE, bufsize=-1)
    housekeeping.communicate()
    housekeeping = Popen(["hdfs", "dfs", "-rm", "-r", os.path.join(os.sep, "tmp", project_code, ledger["version"], "{0}.parquet".format(ledger["label"]))], stdin=PIPE, bufsize=-1)
    housekeeping.communicate()

