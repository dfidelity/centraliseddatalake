from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import boto3
import sys
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.conf import SparkConf
import time
from datetime import datetime
from etl_job_modules.execution_logic import data_procedures
from etl_job_modules.s3_functions import remove_special_from_value
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
    print("PARAMETERS CREATED!")
    # Spark session creation
    # sparksession = spark_session_builder("Incremental-job-from-landing-to-raw", "337962473469")
    sparksession = spark_session_builder("Incremental-job-from-landing-to-raw", aws_account_id, sns_arn)
    sparksession.sparkContext.setLogLevel("ERROR")
    print("SESSION CREATED!")
    # sparksession.sparkContext.setSystemProperty("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY")
    # Reading config table
    # try:
    #     hive_context = HiveContext(sparksession.sparkContext)
    #     # config_table = hive_context.table("ukconfig.etl_job_config")
    #     config_table = hive_context.table(config_db_name + '.' + config_table)
    #     config_table.registerTempTable("etl_job_config_temp")
    #     config_df = hive_context.sql("select * from etl_job_config_temp where procedure_run = 1").cache()
    #     config_df.printSchema()
    # except Exception as err:
    #     send_notification(sns_arn, 'initial', err, 'Getting etl job config')
    #     sys.exit("--ERROR in getting etl job config--")

    hive_context = HiveContext(sparksession.sparkContext)
    print("HIVE CONTEXT CREATED!")

    # Getting batch id
    # batch_id = get_batch_id(sparksession, hive_context, 'ukconfig', 'etl_execution_log_history')
    batch_id = get_batch_id(sparksession, hive_context, config_db_name, logs_table, sns_arn)
    print("BATCH ID IN PLACE!")
    # Creating initial logs
    write_logs_df = initial_log_setup(sparksession, hive_context, config_db_name, logs_table, sns_arn)
    print("LOGS CREATED!")

    refined_path = "s3://rx-uk-datalake-refinedzone/dbo/hello_world/"
    refined_db = "uk_refinedzone"
    table_name = "hello_world"
    source = "dbo"
    partition_key = "partitiondate"
    primary_key = "event"
    target_path = "s3://rx-uk-datalake-refinedzone/dbo/hello_world/"
    target_db = "uk_refinedzone"
    write_tech = "hudi"

    # setting extract end datetime fot this run
    extract_end_datetime = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    # setting extract start datetime that depends from the previous run extract end datetime
    extract_start_datetime = get_last_run_end_date(sparksession, hive_context, config_db_name, logs_table,
                                                   'PROCEDURES', table_name, source, sns_arn)
    print("START DATETIME IN PLACE!")

    write_logs_df = data_procedures(sparksession, hive_context, batch_id, source, table_name, refined_db, extract_start_datetime,
                    extract_end_datetime, write_logs_df, refined_path, target_path, target_db, partition_key, primary_key)

    # i = 0
    # while i < config_df.count():
    #     refined_path = str(config_df.select('refined_path').collect()[i][0]).lower().strip()
    #     refined_db = str(config_df.select('refined_db').collect()[i][0]).strip()
    #     table_name = str(config_df.select('table_name').collect()[i][0]).strip()
    #     source = str(config_df.select('source').collect()[i][0]).strip()
    #     partition_key = remove_special_from_value(str(config_df.select('partition_key').collect()[i][0]).strip())
    #     primary_key = remove_special_from_value(str(config_df.select('unique_key').collect()[i][0]).strip())
    #
    #
    #     # setting extract end datetime fot this run
    #     extract_end_datetime = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    #     # setting extract start datetime that depends from the previous run extract end datetime
    #     extract_start_datetime = get_last_run_end_date(sparksession, hive_context, config_db_name, logs_table,
    #                                                    'CONVERT', table_name, source, sns_arn)
    #
    #     # ADD DIRECTORY PATH INTO CONFIG TO AVOID PROBLEMS WITH MULTIPLE SOURCES!
    #     # raw_path = raw_path + directory_path + '/'
    #     refined_path = refined_path + source + '/' + table_name + '/'
    #
    #     write_logs_df = incremental_to_raw(sparksession, hive_context, batch_id, source, table_name, raw_db,
    #                                        extract_start_datetime, extract_end_datetime, write_logs_df, landing_path,
    #                                        landing_format, raw_path, partition_key, execution_type, data_validation,
    #                                        validation_exception, anonymized_fields, raw_transform, date_column,
    #                                        date_column_format, primary_key, row_tag, header, csv_separator,
    #                                        csv_multi_line)
    #
    #     i += 1

    # write_logs_to_table(sparksession, write_logs_df, 's3://rx-uk-datalake-service-logs/etl_execution_log_history/')
    # write_logs_to_table(sparksession, write_logs_df, log_history_s3_path, sns_arn)

    sparksession.stop()


