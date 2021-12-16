from os.path import expanduser, join, abspath
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json
import os
import getpass
from subprocess import PIPE, Popen
from utils import check_if_file_is_delta

if __name__ == "__main__":

    for arg in sys.argv[1:]:
        print("var: " + arg)

    project_code = sys.argv[1]
    souce_ledger = json.loads(sys.argv[2])

    warehouse_location = abspath('spark-warehouse')

    # Initialize Spark Session
    spark = SparkSession \
        .builder \
        .appName("run_sql_transformation") \
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
    print("source ledger: {0}".format(souce_ledger))

    location_type = souce_ledger["location"]
    file_location = souce_ledger["location_details"]

    version = souce_ledger["version"]
    classification = souce_ledger["classification"]
    label = souce_ledger["label"]
    target_label = souce_ledger["attributes"]["target_label_name"].format(label = label, classification = classification, version = version)
    transformed_parquet_stage_hdfs_path = os.path.join(os.sep, "stage", project_code, version,"transformed","{0}.parquet".format(target_label))

    sql_stm = souce_ledger["attributes"]["sqlcodeblock"].format(source_table = label, label = label, classification = classification, version = version)

    if location_type.upper() == "HDFS-DB":        
        file_type = "parquet"
        if check_if_file_is_delta(file_location):
            file_type = "delta"       
        df = spark.read.format(file_type).load(file_location)
        df.printSchema()
        df.createOrReplaceTempView(label)
        transformed_df = spark.sql(sql_stm)
        transformed_df.printSchema()
        transformed_df.write.mode('overwrite').option("overwriteSchema", "true").format("delta").save(transformed_parquet_stage_hdfs_path)
        # Store staging row count file to hdfs    
        rowcount_stage_hdfs_path = os.path.join(os.sep, "stage", project_code, version,"transformed", "rowcount_{0}.txt".format(label))
        rowcount_local = os.path.join(os.sep, "tmp", "rowcount_{0}_{1}_{2}_{3}.txt".format(project_code,version,"transformed",label))
        with open(rowcount_local, "w") as text_file:
            text_file.write('{}'.format(transformed_df.count()))

        put = Popen(["hdfs", "dfs", "-put", "-f", rowcount_local, rowcount_stage_hdfs_path], stdin=PIPE, bufsize=-1)
        put.communicate()
        os.remove(rowcount_local) 
    else:
        print("Transformation by sql only takes parquet files")
        raise Exception("Transformation by sql only takes parquet files")
