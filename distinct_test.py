from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import boto3
import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
import time
from datetime import datetime
from etl_job_modules.execution_logic import merge_test
from etl_job_modules.utility_functions import send_notification
from etl_job_modules.logs_module import get_batch_id, initial_log_setup, write_logs_to_table, get_last_run_end_date
from etl_job_modules.spark_session_builder import spark_session_builder


if __name__ == '__main__':
    # Reading arguments
    aws_account_id = sys.argv[1]
    config_db_name = sys.argv[2]
    config_table = sys.argv[3]
    # Spark session creation
    # sparksession = spark_session_builder("Incremental-job-from-landing-to-raw", "337962473469")
    sparksession = spark_session_builder("Incremental-job-from-raw-to-refined", aws_account_id)
    sparksession.sparkContext.setLogLevel("ERROR")
    # Reading config table
    try:
        hive_context = HiveContext(sparksession.sparkContext)
        # config_table = hive_context.table("ukconfig.etl_job_config")
        config_table = hive_context.table(config_db_name + '.' + config_table)
        config_table.registerTempTable("etl_job_config_temp")
        etl_config_df = hive_context.sql("select * from etl_job_config_temp where ignore_flag = 0").cache()
        etl_config_df.printSchema()
    except Exception as err:
        send_notification('initial', err, 'Getting etl job config')
        sys.exit("--ERROR in getting etl job config--")

    test_results = {}

    i = 0
    while i < etl_config_df.count():
        table_name = str(etl_config_df.select('table_name').collect()[i][0]).strip()
        source = str(etl_config_df.select('source').collect()[i][0]).strip()
        raw_path = str(etl_config_df.select('raw_path').collect()[i][0]).strip()
        update_date_column = str(etl_config_df.select('update_date_column').collect()[i][0]).lower().strip()
        partition_key_column = str(etl_config_df.select('partition_key').collect()[i][0]).lower().strip()
        refined_path = str(etl_config_df.select('refined_path').collect()[i][0]).lower().strip()
        primary_key = str(etl_config_df.select('unique_key').collect()[i][0]).lower().strip()
        raw_db = str(etl_config_df.select('raw_db').collect()[i][0]).strip()
        refined_db = str(etl_config_df.select('refined_db').collect()[i][0]).strip()
        update_supported = etl_config_df.select('update_supported').collect()[i][0]

        test_results[source + '_' + table_name] = merge_test(sparksession, source, table_name, refined_db, refined_path, primary_key)
        i += 1

    print('RESULTS!!! =>>>>>>>>>>>>>>>>>>>>')
    print(test_results)

    sparksession.stop()
