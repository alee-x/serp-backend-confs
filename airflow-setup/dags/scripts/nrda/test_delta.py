from os.path import expanduser, join, abspath
import sys
import os
from pyspark.sql import SparkSession

if __name__ == "__main__":

    warehouse_location = abspath('spark-warehouse')

    # Initialize Spark Session
    spark = SparkSession \
        .builder \
        .appName("Delta TEST") \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .config("spark.jars.packages", "io.delta:delta-core_2.11:0.6.1") \
        .enableHiveSupport() \
        .getOrCreate()
    
    spark.sparkContext.addPyFile("/usr/local/spark/jars/io.delta_delta-core_2.11-0.6.1.jar")

    from delta.tables import *
    data = spark.range(0, 5)
    data.write.mode('overwrite').format("delta").save("/tmp/delta-table-jeff-test001")
