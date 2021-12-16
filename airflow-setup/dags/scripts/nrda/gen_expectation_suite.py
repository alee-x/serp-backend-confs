import sys
import json
from os.path import expanduser, join, abspath
import os
from pyspark.sql import SparkSession
from pyspark import SparkConf
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context import BaseDataContext
import logging

if __name__ == "__main__":
    workdir = sys.argv[1]
    project_code = sys.argv[2]
    run_id = sys.argv[3]
    print(run_id)

    warehouse_location = "hdfs://namenode:9000/user/hive/warehouse"
    print(warehouse_location)

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
            ("hive.metastore.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
        ]
    spark_conf = SparkConf().setAppName("Expectations").setAll(settings)
    
    # Initialize Spark Session
    spark = SparkSession \
        .builder \
        .master("spark://spark-master:7077") \
        .config(conf = spark_conf) \
        .enableHiveSupport() \
        .getOrCreate()


    print("LOG: INITIALISED SPARK SESSION")

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
    print("LOG: {0}".format(workdir))
    context = BaseDataContext(project_config=config)
    print("LOG: CREATED BASE DATA CONTEXT")
    context.create_expectation_suite(run_id, overwrite_existing=True)
    print("LOG: EXPECTATION SUITE CREATED AT '/tmp/great_expectations/{0}/expectations/{0}.json'".format(run_id))