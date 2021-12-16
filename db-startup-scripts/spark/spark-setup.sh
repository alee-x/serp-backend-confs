#!/bin/bash

MINIOIP=`getent hosts S3_db | awk '{print $1}' | head -1`;
DASHIP=`getent hosts dashbr | awk '{print $1}' | head -1`;
METASTOREIP=`getent hosts hive-metastore | awk '{print $1}' | head -1`; 
DASH_HTTP_ENDPOINT="http://$DASHIP:8086";

cd conf;
cp metrics.properties.template metrics.properties && \
echo -e "*.sink.graphite.class"="org.apache.spark.metrics.sink.GraphiteSink\n"\
"*.sink.graphite.host"=$DASHIP"\n"\
"*.sink.graphite.port"=2003"\n"\
"*.sink.graphite.period"=10"\n"\
"*.sink.graphite.unit"=seconds"\n"\
"*.sink.graphite.prefix"="luca\n"\
"*.source.jvm.class"="org.apache.spark.metrics.source.JvmSource\n"\
"spark.metrics.appStatusSource.enabled=true" >> metrics.properties;

cp spark-defaults.conf.template spark-defaults.conf && \
echo -e "spark.master=spark://spark-master:7077\n"\
"spark.hadoop.fs.s3a.endpoint=http://$MINIOIP:9000\n"\
"spark.hadoop.fs.s3a.access.key=$MINIO_UN\n"\
"spark.hadoop.fs.s3a.secret.key=$MINIO_PWD\n"\
"spark.hadoop.fs.s3a.path.style.access=true\n"\
"spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem\n"\
"spark.jars.packages ch.cern.sparkmeasure:spark-measure_2.12:0.17\n"\
"spark.sparkmeasure.influxdbURL=$DASH_HTTP_ENDPOINT\n"\
"spark.extraListeners=ch.cern.sparkmeasure.InfluxDBSink\n"\
"spark.metrics.appStatusSource.enabled=true\n"\
"spark.eventLog.enabled=true\n"\
"spark.eventLog.dir=/opt/spark/logs\n"\
"spark.sql.warehouse.dir=hdfs://localhost:9000/user/hive/warehouse" >> spark-defaults.conf; 

cp log4j.properties.template log4j.properties && \
echo -e "log4j.logger.org.apache.spark.sql.hive.HiveUtils=ALL\n" \
"log4j.logger.org.apache.spark.sql.internal.SharedState=ALL\n"\
"log4j.logger.org.apache.spark.sql.hive.client.HiveClientImpl=ALL" >> log4j.properties;