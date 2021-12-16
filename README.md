To build and start all services, and have them auto-populate with data:

```zsh
docker-compose up --build -d
```
---

To stop containers:
```zsh
docker-compose down
```

---

This requires a minimum of 16GB of RAM. It builds the following services, with the following ports and login info:

## Databases / data stores

- Minio; 3 server instances, load-balanced by nginx. A separate container populates these with data on startup.
	- Container name: **minio1**, **minio2**, **minio3**
		- Root user: admin
		- Root password: MyP4ssW0rd
		- Minio access key: minio
		- Minio secret key: minio1234
	- Ports:
		- 9001 (web UI)
		- 9000 (backend)
- DB2, with default database *SAIL*, schema *SAIL0000V*, and several tables
	- Container name: **db2**
		- Default user: db2inst1
		- Default password: MyP4ssW0rd
	- Ports:
		- 50000 (to connect to DB)
		- 55000
- MSSQL, with default database *SAIL*, schema *SAIL0000V*, and several tables
	- Container name: **mssql**
		- Default user: root
		- Default password: MyP4ssW0rd
	- Ports:
		- 5434 (*external* to docker-compose)
		- 1433 (*internal* to docker-compose)
- PostgreSQL, with default database *SAIL*, schema *SAIL0000V*, and several tables
	- Container name: **postgresfstore**
		- Default user: sa
		- Default password: MyP4ssW0rd
	- Ports:
		- 5438 (*external* to docker-compose)
		- 5432 (*internal* to docker-compose)

## Hadoop cluster

- Hadoop namenode
	- Container name: **namenode**
	- Ports:
		- 9870 (*internal* and *external*) (Web UI)
		- 9900 (*external*) (access HDFS)
		- 9000 (*internal*) (access HDFS)
- Hadoop datanode
	- Container name: **datanode**
	- Ports:
		- 9864 (*internal* and *external*) (access HDFS)
- Hadoop resource manager
	- Container name: **resourcemanager**
- Hadoop nodemanager
	- Container name: **nodemanager**
- Hadoop history server
	- Container name: **historyserver**
- Hadoop submitter, for automatically populating the Hadoop cluster on first start-up.
	- Container name: **hadoop-submitter**

## Spark cluster
- Spark master
	- Container name: **spark-master**
	- Ports:
		- 8070 (*external*) (Web UI)
		- 8080 (*internal*) (Web UI)
		- 7077 (*internal* and *external*) (spark-submit)
- Spark worker 1
	- Container name: **spark-worker-1**
	- Ports:
		- 8081 (*internal* and *external*) (communicates with master)

## Hive cluster
- Hive server
	- Container name: **hive-server**
	- Ports:
		- 10000 (*internal* and *external*) (connection)
		- 10002 (*internal* and *external*) (Web UI)
- Hive metastore
	- Container name: **hive-metastore**
	- Ports:
		- 9083 (*internal* and *external*) (connection)
- Hive metastore postgres database backend
	- Container name: **hive-metastore-postgresql**
- Hive presto coordinator
	- Container name: **presto-coordinator**
	- Ports:
		- 8089 (*internal* and *external*) (connection)
- Hive submitter, for automatically populating the Hive cluster on first start-up.
	- Container name: **hive-submitter**

## Airflow services

- Airflow Postgres, for the airflow webserver. Default DB *airflow*.
	- Container name: **postgres**
		- Default user: airflow
		- Default password: airflow
	- Ports:
		- None, not required
- Airflow webserver, self explanatory. Mounts dags from airflow-setup/dags. Automatically populated with all connections to other containers and relevant services.
	- Container name: **airflow-webserver**
		- Default user: airflow
		- Default password: airflow
	- Ports:
		- 8080 (*internal* and *external*) (web UI)
- Airflow scheduler
	- Container name: **airflow-scheduler**
	- Ports:
		- None
- Airflow worker
	- Container name: **airflow-worker**
	- Ports:
		- None
- Airflow init, sets up and configures all of the airflow services
	- Container name: **airflow-init**
	- Ports:
		- None
- Flower, also airflow related
	- Container name: **flower**
	- Ports:
		- None

## Others

- NRDA api, a mock/toy version of the NRDA api, built only to allow DAGs to run (i.e. not fully-featured)
	- Container name: **nrdapi**
		- User: No auth needed
	- Ports:
		- 5000 (*internal* and *external*)

## Monitoring and messaging services

- RabbitMQ with default user, and sets up a whole load of possible virtual hosts.
	- Container name: **rabbitmq**
		- Default user: airflow_mq
		- Default password: airflow_mq
		- Default vhost: airflow_mq_host
	- Ports:
		- 15672 (web UI)
		- 5672 (AMQP protocal)
- Spark History server for monitoring ongoing jobs. Reads logs from a share-mounted volume. Would be nicer if it read from HDFS but eh.
	- Container name: **spark-history**
	- Ports:
		- 18080 (web UI)

---

### List of web UI's

- Minio
	- http://localhost:9001
- Hadoop
	- http://localhost:9870
- Spark-master
	- http://localhost:8070
- Hive
	- http://localhost:10002
- Airflow
	- http://localhost:8080
- RabbitMQ
	- http://localhost:15672
- Spark history server
	- http://localhost:18080