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
    try:
        hive_context = HiveContext(sparksession.sparkContext)
        config_table = hive_context.table(config_db_name + '.' + config_table)
        config_table.registerTempTable("etl_job_config_temp")
        config_df = hive_context.sql("select * from etl_job_config_temp " +
                                     "where landing_to_raw = 1 and group_id = '" + group_id + "'").cache()
        config_df.printSchema()
    except Exception as err:
        print(traceback.print_tb(err.__traceback__))
        sys.exit("--ERROR in getting etl job config--")

    env_name = "prod" if aws_account_id == "767030707495" else "preprod"
    target_bucket = "s3://rx-gbs-datalake-rawzone-" + env_name + "/"
    target_db = "globaldb_raw_" + env_name

    i = 0
    while i < config_df.count():
        table_info = {
            'schema_path': str(config_df.select('schema_path').collect()[i][0]).strip(),
            'table_name': str(config_df.select('table_name').collect()[i][0]).strip(),
            'source': str(config_df.select('source').collect()[i][0]).strip(),
            'partition_key': remove_special_from_value(str(config_df.select('partition_key').collect()[i][0]).strip()),
            'raw_bucket': str(config_df.select('raw_path').collect()[i][0]).strip(),
            'raw_db': str(config_df.select('raw_db').collect()[i][0]).strip(),
            'primary_key': remove_special_from_value(str(config_df.select('unique_key').collect()[i][0]).strip()),
            'update_supported': True if str(config_df.select('update_supported').collect()[i][0]).strip().lower() == 'true' else False
        }

        if table_info['source'] == 'efm':
            target_db = "globaldb_efm_" + env_name
        elif table_info['source'] == 'gleanin':
            target_db = "globaldb_gleanin_" + env_name

        if table_info['partition_key'] == '':
            table_info['partition_key'] = "partitiondate"

        table_info['raw_path'] = table_info['raw_bucket'] + table_info['source'] + '/' + table_info['table_name'] + '/'

        print("reading source data <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        data_df = hudi_snapshot_read(sparksession, table_info['raw_path'])
        print('SOURCE ROW COUNT->', data_df.count())

        data_df.na.drop()
        print('N\A DROP ROW COUNT->', data_df.count())

        print("writing data to staging <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        staging_path = target_bucket + "oneoff/" + table_info['source'] + '/' + table_info['table_name'] + '/'
        data_df.repartition(1).write.mode("overwrite").parquet(path=staging_path, compression="snappy")
        # hudi_write_s3(data_df, staging_path, table_info['partition_key'], target_db,
        #               "staging_" + table_info['table_name'], table_info['primary_key'], operation, "overwrite")

        print("readong staging data <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        data_df = sparksession.read.options(basePath=staging_path).format("parquet").load(staging_path + "*")
        # data_df = hudi_snapshot_read(sparksession, staging_path)

        print("writing data to target <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        table_info['raw_path'] = target_bucket + table_info['source'] + '/' + table_info['table_name'] + '/'
        operation = "upsert" if table_info['update_supported'] else "insert"
        hudi_write_s3(data_df, table_info['raw_path'], table_info['partition_key'], target_db,
                      table_info['table_name'], table_info['primary_key'], operation, "overwrite")

        i += 1

    sparksession.stop()


