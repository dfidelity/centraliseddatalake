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

if __name__ == '__main__':
    # Reading arguments
    aws_account_id = sys.argv[1]
    sns_arn = sys.argv[2]
    config_db_name = sys.argv[3]
    config_table = sys.argv[4]
    table_db = sys.argv[5]
    table_name = sys.argv[5]
    logs_table = sys.argv[6]
    log_history_s3_path = sys.argv[6]
    update_option = sys.argv[7]
    non_anonymized_bucket = sys.argv[8]
    non_anonymized_db = sys.argv[9]
    # Spark session creation
    # sparksession = spark_session_builder("Incremental-job-from-landing-to-raw", "337962473469")
    sparksession = spark_session_builder("Incremental-job-from-landing-to-raw-" + group_id, aws_account_id, sns_arn)
    sparksession.sparkContext.setLogLevel("ERROR")
    # sparksession.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInRead=LEGACY")
    # sparksession.sql("set spark.sql.legacy.parquet.datetimeRebaseModeInWrite=CORRECTED")
    # sparksession.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY")
    # sparksession.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
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
        send_notification(sns_arn, 'initial', err, 'Getting etl job config')
        sys.exit("--ERROR in getting etl job config--")

    # Getting batch id
    # batch_id = get_batch_id(sparksession, hive_context, 'ukconfig', 'etl_execution_log_history')
    batch_id = get_batch_id(sparksession, hive_context, config_db_name, logs_table, sns_arn)
    # Creating initial logs
    write_logs_df = initial_log_setup(sparksession, hive_context, config_db_name, logs_table, sns_arn)

    i = 0
    while i < config_df.count():
        table_info = {
            'landing_path': str(config_df.select('landing_path').collect()[i][0]).strip(),
            'landing_format': str(config_df.select('landing_format').collect()[i][0]).strip(),
            'schema_path': str(config_df.select('schema_path').collect()[i][0]).strip(),
            'table_name': str(config_df.select('table_name').collect()[i][0]).strip(),
            'source': str(config_df.select('source').collect()[i][0]).strip(),
            'execution_type': str(config_df.select('execution_type').collect()[i][0]).strip(),
            'partition_key': remove_special_from_value(str(config_df.select('partition_key').collect()[i][0]).strip()),
            'validation_exception': remove_special_from_value(str(config_df.select('validation_exception').collect()[i][0]).strip()),
            'anonymized_fields': remove_special_from_value(str(config_df.select('anonymized_fields').collect()[i][0]).strip()),
            'raw_bucket': str(config_df.select('raw_path').collect()[i][0]).strip(),
            'raw_db': str(config_df.select('raw_db').collect()[i][0]).strip(),
            'row_tag': str(config_df.select('row_tag').collect()[i][0]).strip(),
            'csv_sep': str(config_df.select('csv_separator').collect()[i][0]).strip(),
            'overwrite_mode': True if str(config_df.select('overwrite_mode').collect()[i][0]).strip().lower() == 'true' else False,
            'csv_multi_line': True if str(config_df.select('csv_multi_line').collect()[i][0]).strip().lower() == 'true' else False,
            'header': True if str(config_df.select('header').collect()[i][0]).strip().lower() == 'true' else False,
            'raw_transform': True if str(config_df.select('raw_data_transform').collect()[i][0]).strip().lower() == 'true' else False,
            'data_validation': True if str(config_df.select('data_validation').collect()[i][0]).strip().lower() == 'true' else False,
            'date_column': remove_special_from_value(str(config_df.select('update_date_column').collect()[i][0]).strip()),
            'date_column_format': str(config_df.select('update_date_format').collect()[i][0]).strip(),
            'source_date_format': str(config_df.select('source_date_format').collect()[i][0]).strip(),
            'primary_key': remove_special_from_value(str(config_df.select('unique_key').collect()[i][0]).strip())
        }


        # setting extract end datetime fot this run
        extract_end_datetime = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        # setting extract start datetime that depends from the previous run extract end datetime
        if table_info['overwrite_mode']:
            extract_start_datetime = '1990/02/09 12:32:22'
            # extract_start_datetime = '2022/02/14 12:32:22'
        else:
            extract_start_datetime = get_last_run_end_date(sparksession, hive_context, config_db_name, logs_table,
                                                           'CONVERT', table_info['table_name'], table_info['source'], sns_arn)


        # ADD DIRECTORY PATH INTO CONFIG TO AVOID PROBLEMS WITH MULTIPLE SOURCES!
        # raw_path = raw_path + directory_path + '/'
        table_info['raw_path'] = table_info['raw_bucket'] + table_info['source'] + '/' + table_info['table_name'] + '/'

        write_logs_df = incremental_to_raw(sparksession, hive_context, batch_id, table_info,
                                           extract_start_datetime, extract_end_datetime, write_logs_df,
                                           non_anonymized_bucket, non_anonymized_db)

        i += 1

    # write_logs_to_table(sparksession, write_logs_df, 's3://rx-uk-datalake-service-logs/etl_execution_log_history/')
    write_logs_to_table(sparksession, write_logs_df, log_history_s3_path, sns_arn)

    sparksession.stop()
