FROM bde2020/spark-master:3.1.1-hadoop3.2

EXPOSE 8080 7077 7000 4040 6066 18080 8001 

ENV INIT_DAEMON_STEP=setup_spark
ENV CORE_CONF_fs_defaultFS=hdfs://namenode:9000
ENV SPARK_LOCAL_IP=spark-master
ENV SPARK_WORKLOAD=master

COPY hive-site.xml /spark/conf/
COPY spark-defaults.conf /spark/conf/

CMD ["/bin/bash", "/master.sh"]