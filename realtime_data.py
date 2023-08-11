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
from etl_job_modules.execution_logic import realtime_execution
from etl_job_modules.s3_functions import remove_prefix
from etl_job_modules.logs_module import get_last_run_end_date, get_batch_id, initial_log_setup, write_logs_to_table
from etl_job_modules.utility_functions import send_notification
from etl_job_modules.spark_session_builder import spark_session_builder

if __name__ == '__main__':
    # Reading arguments
    aws_account_id = sys.argv[1]
    sns_arn = sys.argv[2]
    config_db_name = sys.argv[3]
    config_table = sys.argv[4]
    logs_table = sys.argv[5]
    log_history_s3_path = sys.argv[6]
    landing_bucket = sys.argv[7]
    file_path = sys.argv[8]
    inserted_date = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    # Spark session creation
    sparksession = spark_session_builder("Incremental-job-from-landing-to-raw", aws_account_id)
    sparksession.sparkContext.setLogLevel("ERROR")
    # Reading config table

    # Getting source_path from file_path
    file_loc = remove_prefix(file_path, landing_bucket)
    source_loc = '/'.join(file_loc.split('/')[:2])
    source_path = landing_bucket + source_loc + '/'
    print("SOURCE PATH -> ", source_path)

    try:
        hive_context = HiveContext(sparksession.sparkContext)
        config_table = hive_context.table(config_db_name + '.' + config_table)
        config_table.registerTempTable("etl_job_config_temp")
        config_df = hive_context.sql("select * from etl_job_config_temp where ignore_flag = 0 and landing_path = '" +
                                     source_path + "'").cache()
        config_df.printSchema()
    except Exception as err:
        send_notification(sns_arn, 'initial', err, 'Getting etl job config')
        sys.exit("--ERROR in getting etl job config--")

    # Getting batch id
    batch_id = get_batch_id(sparksession, hive_context, config_db_name, logs_table, sns_arn)
    # Creating initial logs
    write_logs_df = initial_log_setup(sparksession, hive_context, config_db_name, logs_table, sns_arn)

    i = 0
    while i < config_df.count():
        table_name = str(config_df.select('table_name').collect()[i][0]).strip()
        source = str(config_df.select('source').collect()[i][0]).strip()
        source_format = str(config_df.select('landing_format').collect()[i][0]).strip()
        partition_key = str(config_df.select('partition_key').collect()[i][0]).strip()
        raw_path = str(config_df.select('raw_path').collect()[i][0]).strip()
        row_tag = str(config_df.select('row_tag').collect()[i][0]).strip()
        csv_separator = str(config_df.select('csv_separator').collect()[i][0]).strip()
        csv_multi_line = config_df.select('csv_multi_line').collect()[i][0]
        header = config_df.select('header').collect()[i][0]
        incremental_supported = config_df.select('incremental_supported').collect()[i][0]
        raw_db = str(config_df.select('raw_db').collect()[i][0]).strip()
        refined_path = str(config_df.select('refined_path').collect()[i][0]).lower().strip()
        refined_db = str(config_df.select('refined_db').collect()[i][0]).strip()
        date_column = str(config_df.select('update_date_column').collect()[i][0]).strip()
        date_column_format = str(config_df.select('update_date_format').collect()[i][0]).strip()
        primary_key = str(config_df.select('unique_key').collect()[i][0]).strip()
        update_supported = config_df.select('update_supported').collect()[i][0]

        # setting extract end datetime fot this run
        extract_end_datetime = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        # setting extract start datetime that depends from the previous run extract end datetime
        extract_start_datetime = get_last_run_end_date(sparksession, hive_context, config_db_name, logs_table,
                                                       'REALTIME', table_name, source, sns_arn)

        write_logs_df = realtime_execution(sparksession, hive_context, batch_id, source, table_name, raw_db, refined_db,
                                           extract_start_datetime, extract_end_datetime, write_logs_df, source_path,
                                           source_format, file_path, inserted_date, raw_path, refined_path,
                                           partition_key, incremental_supported, update_supported, date_column,
                                           date_column_format, primary_key, row_tag, header, csv_separator,
                                           csv_multi_line)
        i += 1

    write_logs_to_table(sparksession, write_logs_df, log_history_s3_path, sns_arn)

    sparksession.stop()
