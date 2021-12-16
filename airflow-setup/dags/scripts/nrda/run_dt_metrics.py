from os.path import expanduser, join, abspath
import json
import datetime
import os 
from subprocess import PIPE, Popen
import sys
import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import *
from utils import check_if_file_is_delta

def default(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()

if __name__ == "__main__":

  for arg in sys.argv[1:]:
        print("var: " + arg)

  workdir = sys.argv[1]
  project_code = sys.argv[2]
  ledger = json.loads(sys.argv[3])
  target_label = sys.argv[4]

  label = ledger["label"]
  if not workdir:
     workdir = "tmp"
  if workdir == "stage":
      label = target_label
  
  warehouse_location = abspath('spark-warehouse')

  # Initialize Spark Session
  spark = SparkSession \
      .builder \
      .appName("Run metrics on columns of date/datetime type") \
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

  file_location = os.path.join(os.sep, workdir, project_code, ledger["version"], "{0}.parquet".format(label))
  file_type = "parquet"

  if workdir == "stage":
     file_type = "delta"   
  else:
     if ledger["location"].upper() == "HDFS-DB":
        file_location = ledger["location_details"]
     if check_if_file_is_delta(ledger["location_details"]):
        file_type = "delta"  
  print("location path: {0} - file type: {1}".format(file_location, file_type))   
  #sc = spark.sparkContext 
  df = spark.read.format(file_type).load(file_location)

  dtColumnList = [item.name.lower() for item in df.schema if (isinstance(item.dataType, DateType) or 
                                                             isinstance(item.dataType, TimestampType))]
  df_dt_only = df.select(dtColumnList)
  df_dt_only.printSchema()
  names = df_dt_only.schema.names
  dt_dq_list = []
  for name in names:
      df_dt_only.select(name).createOrReplaceTempView(name)
      count = spark.sql("select count(*) as count from {name}".format(name = name)).collect()[0]['count']
      distinct_count = spark.sql("select count(distinct {name}) as distinct_count from {name}".format(name = name)).collect()[0].distinct_count
      null_count = spark.sql("select count(*) as null_count from {name} where {name} is null".format(name = name)).collect()[0].null_count
      max_min_df = spark.sql("select max({name}) as max,  min({name}) as min from {name}".format(name = name)).collect()
      if count == null_count:
        dt_dq_dict = {"name":name,"count":count,"distinct_count":distinct_count,"null_count":null_count,"min":None,"max":None}
      else:
        dt_dq_dict = {"name":name,"count":count,"distinct_count":distinct_count,"null_count":null_count,"min":max_min_df[0]['min'],"max":max_min_df[0]['max']}
      dt_dq_list.append(dt_dq_dict)      
      spark.catalog.dropTempView(name)
      
  # define path to saved file
  hdfs_path = os.path.join(os.sep, workdir, project_code,ledger["version"],"dt_dq_{0}.json".format(label))
  file_name = os.path.join(os.sep, "tmp", "dt_dq_{0}_{1}_{2}.json".format(project_code,ledger["version"],label))
  with open(file_name, 'w') as fp:
      json.dump(dt_dq_list, fp,sort_keys=True,indent=1,default=default)
  # put csv into hdfs
  put = Popen(["hdfs", "dfs", "-put", "-f", file_name, hdfs_path], stdin=PIPE, bufsize=-1)
  put.communicate()
  #os.remove(file_name)