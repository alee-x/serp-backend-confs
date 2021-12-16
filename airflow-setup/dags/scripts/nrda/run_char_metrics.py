from os.path import expanduser, join, abspath
import sys
import json
import os 
import getpass
from subprocess import PIPE, Popen
import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import *
from utils import check_if_file_is_delta

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
      .appName("Run metrics on columns of string type") \
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
  
  strColumnList = [item.name.lower() for item in df.schema if (isinstance(item.dataType, StringType) or 
                                                             isinstance(item.dataType, BooleanType))]
  df_str_only = df.select(strColumnList)
  df_str_only.printSchema()

  names = df_str_only.schema.names
  char_dq_list = []
  for name in names:
      #spark.catalog.dropTempView(name)
      df_str_only.select(name).createOrReplaceTempView(name)
      #spark.catalog.cacheTable(name)
      count = spark.sql("select count(*) as count from {name}".format(name = name)).collect()[0]['count']
      distinct_count = spark.sql("select count(distinct {name}) as distinct_count from {name}".format(name = name)).collect()[0].distinct_count
      null_count = spark.sql("select count(*) as null_count from {name} where {name} is null".format(name = name)).collect()[0].null_count
      empty_count = spark.sql("select count(*) as empty_count from {name} where {name} = ''".format(name = name)).collect()[0].empty_count
      #spark.sql("select count(*) from {name} where {name} is null".format(name = name)).show()
      #spark.sql("select max(temperature), min(temperature), avg(temperature) from temperature").show()
      max_min_len_DF = spark.sql("select max(length({name})) as max_len, min(length({name})) as min_len from {name}".format(name = name)).collect()
      freqDF = spark.sql("select {name} as name, count(1) as total from {name} where {name} is not null group by {name} order by total desc limit 1".format(name = name)).collect()
      if count == null_count + empty_count:
          char_dq_dict = {"name":name,"count":count,"distinct_count":distinct_count,"null_count":null_count,"empty_count":empty_count,"min_len":0,"max_len":30,"most_freq_val":None,"freq":0}
      else:
          char_dq_dict = {"name":name,"count":count,"distinct_count":distinct_count,"null_count":null_count,"empty_count":empty_count,"min_len":max_min_len_DF[0].min_len,"max_len":max_min_len_DF[0].max_len,"most_freq_val":freqDF[0].name,"freq":freqDF[0].total}
      char_dq_list.append(char_dq_dict)
      spark.catalog.dropTempView(name)
    
  # print("current dir: "+os.getcwd())
  # print("current user: "+getpass.getuser())
  # define path to saved file
  hdfs_path = os.path.join(os.sep, workdir, project_code,ledger["version"],"char_dq_{0}.json".format(label))
  file_name = os.path.join(os.sep, "tmp", "char_dq_{0}_{1}_{2}.json".format(project_code, ledger["version"], label))
  with open(file_name, 'w') as fp:
      json.dump(char_dq_list, fp)
  # create path to your username on hdfs
  #hdfs_path = os.path.join(os.sep, 'cvst')
  # put csv into hdfs
  put = Popen(["hdfs", "dfs", "-put", "-f", file_name, hdfs_path], stdin=PIPE, bufsize=-1)
  put.communicate()
  #os.remove(file_name)