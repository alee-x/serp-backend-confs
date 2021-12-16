import sys
import json
import datetime
from os.path import expanduser, join, abspath
from pyspark.sql import SparkSession
from pyspark import SparkConf
from utils import check_if_file_is_delta
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context import BaseDataContext

if __name__ == "__main__":

    for arg in sys.argv[1:]:
        print("var: " + arg)

    workdir = sys.argv[1]
    project_code = sys.argv[2]
    ledger = json.loads(sys.argv[3])
    run_id = sys.argv[4]

    warehouse_location = "hdfs://namenode:9000/user/hive/warehouse"
    print(warehouse_location)

    config = DataContextConfig(
        config_version=2,
        plugins_directory=None,
        config_variables_file_path=None,

        datasources={
            "my_spark_datasource": {
                "data_asset_type": {
                    "class_name": "SparkDFDataset",
                    "module_name": "great_expectations.dataset",
                },
                "class_name": "SparkDFDatasource",
                "module_name": "great_expectations.datasource",
                "batch_kwargs_generators": {},
            }
        },
        stores={
        "expectations_store": {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": "/tmp/great_expectations/{0}/expectations/".format(run_id), 
            },
        },
        "validations_store": {
            "class_name": "ValidationsStore",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": "/tmp/great_expectations/{0}/uncommitted/validations/".format(run_id), 
            },
        },
        "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
        },
        expectations_store_name="expectations_store",
        validations_store_name="validations_store",
        evaluation_parameter_store_name="evaluation_parameter_store",
        data_docs_sites={
            "local_site": {
                "class_name": "SiteBuilder",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "/tmp/great_expectations/{0}/uncommitted/data_docs/local_site/".format(run_id), 
                },
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder",
                    "show_cta_footer": True,
                },
            }
        },
        validation_operators={
            "action_list_operator": {
                "class_name": "ActionListValidationOperator",
                "action_list": [
                    {
                        "name": "store_validation_result",
                        "action": {"class_name": "StoreValidationResultAction"},
                    },
                    {
                        "name": "store_evaluation_params",
                        "action": {"class_name": "StoreEvaluationParametersAction"},
                    },
                    {
                        "name": "update_data_docs",
                        "action": {"class_name": "UpdateDataDocsAction"},
                    },
                ],
            }
        },
        anonymous_usage_statistics={
            "enabled": True
        }
    )

    context = BaseDataContext(project_config=config)
    print("LOG: CREATED BASE DATA CONTEXT")

    suite = context.get_expectation_suite(run_id)

    settings = [
            ("hive.metastore.uris", "thrift://hive-metastore:9083"),
            ("hive.server2.thrift.http.path", "cliservice"),
            ("datanucleus.autoCreateSchema", "false"),
            ("javax.jdo.option.ConnectionURL", "jdbc:postgresql://hive-metastore-postgresql/metastore"),
            ("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver"),
            ("javax.jdo.option.ConnectionPassword", "hive"),
            ("hive.server2.transport.mode", "http"),
            ("hive.server2.thrift.max.worker.threads", "5000"),
            ("javax.jdo.option.ConnectionUserName", "hive"),
            ("hive.server2.thrift.http.port", "10000"),
            ("hive.server2.enable.doAs", "false"),
            ("hive.metastore.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse"),
            ("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0"),
            ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
            ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
            ("spark.driver.extraJavaOptions", "-Divy.cache.dir=/tmp -Divy.home=/tmp")
        ]

    spark_conf = SparkConf().setAppName("Profiling").setAll(settings)
    
    # Initialize Spark Session
    spark = SparkSession \
        .builder \
        .master("spark://spark-master:7077") \
        .config(conf = spark_conf) \
        .enableHiveSupport() \
        .getOrCreate()

        
    # spark.sparkContext.addPyFile("/usr/local/spark/jars/io.delta_delta-core_2.12-0.6.1.jar")
   
    # Apache Spark 2.4.x has a known issue (SPARK-25003) that requires explicit activation
    # of the extension and cloning of the session. This will unnecessary in Apache Spark 3.x.
    if spark.sparkContext.version < "3.":
        spark.sparkContext._jvm.io.delta.sql.DeltaSparkSessionExtension() \
            .apply(spark._jsparkSession.extensions())
        spark = SparkSession(spark.sparkContext, spark._jsparkSession.cloneSession())
        
    print("LOG: INITIALISED SPARK SESSION")

    location_type = ledger["location"]
    file_location = ledger["location_details"]
    df = None

    if location_type.upper() == "HDFS-DB":
        file_type = "parquet"
        if check_if_file_is_delta(file_location):
            file_type = "delta"
        df = spark.read.format(file_type).load(file_location)
    else:
        file_type = "csv"

        # CSV options
        infer_schema = "true"
        first_row_is_header = ledger["attributes"]["has_header"]
        delimiter = ledger["attributes"]["delimiter"]
        charset = ledger["attributes"]["charset"] if 'charset' in ledger["attributes"] else "UTF-8"
        date_format = ledger["attributes"]["date_format"] if 'date_format' in ledger["attributes"] else "yyyy-MM-dd"
        timestamp_format = ledger["attributes"]["timestamp_format"] if 'timestamp_format' in ledger["attributes"] else "yyyy-MM-dd'T'HH:mm:ssZ"

        df = spark.read.format(file_type) \
            .option("inferSchema", infer_schema) \
            .option("header", first_row_is_header) \
            .option("sep", delimiter) \
            .option("charset", charset) \
            .option("dateFormat", date_format) \
            .option("timestampFormat", timestamp_format) \
            .load(file_location)


    batch_kwargs={
        "dataset": df,
        "datasource": "my_spark_datasource",
    }

    batch = context.get_batch(
        batch_kwargs=batch_kwargs,
        expectation_suite_name=run_id
    )

    run_id_args = {
        "run_name": run_id, 
        "run_time": datetime.datetime.now(datetime.timezone.utc)
    }

    results = context.run_validation_operator(
        "action_list_operator",
        assets_to_validate=[batch],
        run_id=run_id_args)
