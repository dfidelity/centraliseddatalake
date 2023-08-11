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
    source_bucket = "s3://rx-gbs-datalake-rawzone-prod" if aws_account_id == "767030707495" else "s3://rx-gbs-datalake-rawzone/"
    target_bucket = "s3://rx-gbs-datalake-rawzone-" + env_name + "/"
    source_db = "efm" if aws_account_id == "767030707495" else "globaldb_efm_raw"
    target_db = "globaldb_efm_" + env_name
    source = "efm"
    table_name = "efm_surveyparticipant"
    partition_key = "partitiondate"
    primary_key = "projectid,participantid"


    raw_path = source_bucket + source + '/' + table_name + '/'

    print("reading source data <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    # data_df = hudi_snapshot_read(sparksession, raw_path)
    sparksession.catalog.setCurrentDatabase(source_db)
    data_df =sparksession.sql(f"""
        SELECT * FROM {table_name}
        """)
    print('SOURCE ROW COUNT->', data_df.count())
    drop_list = []
    for c in data_df.dtypes:
        if c[0][0] == '_':
            drop_list.append(c[0])
    data_df = data_df.drop(*drop_list)
    data_df.printSchema()

    data_df.na.drop()
    print('N\A DROP ROW COUNT->', data_df.count())

    print("writing data to staging <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    staging_path = target_bucket + "oneoff/" + source + '/' + table_name + '/'
    data_df.repartition(1).write.mode("overwrite").parquet(path=staging_path, compression="snappy")
    # hudi_write_s3(data_df, staging_path, table_info['partition_key'], target_db,
    #               "staging_" + table_info['table_name'], table_info['primary_key'], operation, "overwrite")

    print("readong staging data <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    data_df = sparksession.read.options(basePath=staging_path).format("parquet").load(staging_path + "*")
    # data_df = hudi_snapshot_read(sparksession, staging_path)

    print("writing data to target <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    target_path = target_bucket + source + '/' + table_name + '/'
    operation = "upsert"
    hudi_write_s3(data_df, target_path, partition_key, target_db,
                  table_name, primary_key, operation, "overwrite")

    sparksession.stop()


