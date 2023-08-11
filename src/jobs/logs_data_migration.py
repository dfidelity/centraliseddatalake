from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import boto3
import sys
import traceback
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.conf import SparkConf
import time
from datetime import datetime
from etl_job_modules.execution_logic import incremental_to_raw
from etl_job_modules.s3_functions import remove_special_from_value
from etl_job_modules.logs_module import get_last_run_end_date, get_batch_id, initial_log_setup, write_logs_to_table
from etl_job_modules.utility_functions import send_notification
from etl_job_modules.spark_session_builder import spark_session_builder
from etl_job_modules.hudi_operations import hudi_snapshot_read, hudi_write_s3


if __name__ == '__main__':
    # Reading arguments
    aws_account_id = sys.argv[1]
    sns_arn = sys.argv[2]
    config_db_name = sys.argv[3]
    config_table = sys.argv[4]
    group_id = sys.argv[7]
    # Spark session creation
    sparksession = spark_session_builder("Incremental-job-from-landing-to-raw-" + group_id, aws_account_id, sns_arn)
    sparksession.sparkContext.setLogLevel("ERROR")
    # Reading config table

    env_name = "prod" if aws_account_id == "767030707495" else "preprod"
    source_bucket = "s3://rx-gbs-datalake-logs-prod" if aws_account_id == "767030707495" else "s3://rx-gbs-datalake-logs/"
    target_bucket = "s3://rx-gbs-datalake-logs-" + env_name + "/"
    source_db = "globaldb_etl_config" if aws_account_id == "767030707495" else "gbs_config"
    target_db = "globaldb_etl_config_" + env_name
    table_name = "etl_execution_log_history"

    raw_path = source_bucket + 'etl_execution_log_history/'

    print("reading source data <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    data_df = sparksession.read.options(basePath=raw_path).format("parquet").load(raw_path + "*")
    print('SOURCE ROW COUNT->', data_df.count())

    print("writing data to staging <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    staging_path = target_bucket + 'etl_execution_log_history/'
    data_df.repartition(10).write.mode("overwrite").parquet(path=staging_path, compression="snappy")

    sparksession.stop()


