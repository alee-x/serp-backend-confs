FROM apache/airflow:2.1.4

USER root
ARG SPARK_VERSION="3.1.1"
ARG HADOOP_VERSION="3.2"
ARG HADOOP_VERSION_F="3.2.0"

RUN apt-get update \
    && sudo apt-get -y install build-essential gcc libsasl2-dev python3-dev libldap2-dev libssl-dev g++ vim curl wget
# the --no-install-recommends helps limit some of the install so that you can be more explicit about what gets installed

RUN curl --silent https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
  && curl --silent https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && sudo apt-get update \
    && sudo ACCEPT_EULA=y apt-get install -y msodbcsql17 \
    && sudo ACCEPT_EULA=y apt-get install -y mssql-tools unixodbc-dev 

###############################
## Begin DB2 client installation
###############################
# DB2 prereqs (also installing sharutils package as we use the utility uuencode to generate password - all others are required for the DB2 Client) 
RUN dpkg --add-architecture i386 \
    && sudo apt-get update \
    && sudo apt-get install -y sharutils binutils libstdc++6:i386 libpam0g:i386 \
    && ln -s /lib/i386-linux-gnu/libpam.so.0 /lib/libpam.so.0 \
    && mkdir -p /db2install


ADD dclient.tar.gz /db2install/
COPY db2rtcl.rsp /db2install/client/

RUN sudo /db2install/client/db2setup -u /db2install/client/db2rtcl.rsp -f sysreq

# Clean up install files
# RUN rm -rf /db2install/rtcl

###############################
## Begin JAVA installation
###############################
# Java is required in order to spark-submit work
# Install OpenJDK-8
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    apt-get install -y gnupg2 && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EB9B1D8886F44E2A && \
    add-apt-repository "deb http://security.debian.org/debian-security stretch/updates main" && \
    apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    pip freeze && \
    java -version $$ \
    javac -version

# Setup JAVA_HOME 
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
RUN export JAVA_HOME

###############################
## SPARK files and variables
###############################
ENV SPARK_HOME /usr/local/spark

# Spark submit binaries and jars (Spark binaries must be the same version of spark cluster)
RUN cd "/tmp" && \
        wget --no-verbose "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
        tar -xvzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
        mkdir -p "${SPARK_HOME}/bin" && \
        mkdir -p "${SPARK_HOME}/assembly/target/scala-2.12/jars" && \
        mkdir -p "${SPARK_HOME}/jars" && \
        cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/jars/." "${SPARK_HOME}/jars/" && \
        cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/bin/." "${SPARK_HOME}/bin/" && \
        cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/jars/." "${SPARK_HOME}/assembly/target/scala-2.12/jars/" && \
        rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
        wget --no-verbose "https://repo1.maven.org/maven2/io/delta/delta-core_2.12/1.0.0/delta-core_2.12-1.0.0.jar" && \
        cp "delta-core_2.12-1.0.0.jar" "${SPARK_HOME}/jars/" && \
        cp "delta-core_2.12-1.0.0.jar" "${SPARK_HOME}/assembly/target/scala-2.12/jars/" && \
        mv "delta-core_2.12-1.0.0.jar" "io.delta_delta-core_2.12-1.0.0.jar" && \
        cp "io.delta_delta-core_2.12-1.0.0.jar" "${SPARK_HOME}/jars/" && \
        cp "io.delta_delta-core_2.12-1.0.0.jar" "${SPARK_HOME}/assembly/target/scala-2.12/jars/"


# Create SPARK_HOME env var
RUN export SPARK_HOME
ENV PATH $PATH:/usr/local/spark/bin

###############################
## HADOOP files and variables
###############################
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/etc/hadoop

RUN HADOOP_URL="https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION_F/hadoop-$HADOOP_VERSION_F.tar.gz" \
    && curl 'https://dist.apache.org/repos/dist/release/hadoop/common/KEYS' | gpg --import - \
    && curl -fSL "$HADOOP_URL" -o /tmp/hadoop.tar.gz \
    && curl -fSL "$HADOOP_URL.asc" -o /tmp/hadoop.tar.gz.asc \
    && gpg --verify /tmp/hadoop.tar.gz.asc \
    && mkdir -p "${HADOOP_HOME}" \
    && tar -xvf /tmp/hadoop.tar.gz -C "${HADOOP_HOME}" --strip-components=1 \
    && rm /tmp/hadoop.tar.gz /tmp/hadoop.tar.gz.asc \
    && ln -s "${HADOOP_HOME}/etc/hadoop" /etc/hadoop \
    && mkdir "${HADOOP_HOME}/logs" \
    && mkdir /hadoop-data

COPY core-site.xml /etc/hadoop/core-site.xml
COPY hdfs-site.xml /etc/hadoop/hdfs-site.xml

ENV PATH="$HADOOP_HOME/bin/:$PATH"

USER airflow
RUN pip install --no-cache-dir --user \
    apache-airflow-providers-apache-spark==2.0.1 \
    pika==1.2.0 \
    rabbitmq==0.2.0 \
    pyspark==3.1.1 \
    pyodbc==4.0.32 \
    pyreadstat==1.1.3 \
    pandas-profiling==3.0.0 \
    great-expectations==0.13.36 \
    py4j==0.10.9 \
    ibm_db_sa==0.3.7 \
    apache-airflow-providers-jdbc==2.0.1 \
    apache-airflow-providers-odbc==2.0.1 \
    apache-airflow-providers-celery \
    pymssql \
    apache-airflow-providers-microsoft-mssql \
    airflow-provider-great-expectations \
    delta-spark \
    findspark \
    apache-airflow-providers-apache-hdfs \
    apache-airflow-providers-apache-hive


RUN rm /home/airflow/.local/lib/python3.6/site-packages/great_expectations/data_context/types/resource_identifiers.py
COPY resource_identifiers.py /home/airflow/.local/lib/python3.6/site-packages/great_expectations/data_context/types/

USER root
COPY entrypoint /

RUN chmod +x /entrypoint

USER airflow