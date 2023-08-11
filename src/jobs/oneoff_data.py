import sys
from datetime import datetime
import traceback
from pyspark.sql import SparkSession
from etl_job_modules.utility_functions import send_notification
from etl_job_modules.logs_module import get_last_run_end_date, get_batch_id, initial_log_setup, \
        write_logs_to_table
from etl_job_modules.execution_logic import one_off_execution
from etl_job_modules.s3_functions import remove_special_from_value
from etl_job_modules.spark_session_builder import spark_session_builder
from pyspark.sql import HiveContext

if __name__ == '__main__':
    # Read user argument
    aws_account_id = sys.argv[1]
    sns_arn = sys.argv[2]
    config_db_name = sys.argv[3]
    config_table = sys.argv[4]
    logs_table = sys.argv[5]
    log_history_s3_path = sys.argv[6]
    group_id = sys.argv[7]
    non_anonymized_bucket = sys.argv[8]
    non_anonymized_db = sys.argv[9]
    # Create spark session
    spark_session = spark_session_builder("rk_uk_datalake_transformation", aws_account_id, sns_arn)
    spark_session.sparkContext.setLogLevel("ERROR")
    # Read from glue tables
    try:
        hive_context = HiveContext(spark_session.sparkContext)
        config_table = hive_context.table(config_db_name + "." + config_table)
        config_table.registerTempTable("etl_job_config_temp")
        config_df = hive_context.sql("select * from etl_job_config_temp " +
                                     "where one_off_to_raw = 1 and group_id = '" + group_id + "'").cache()
        config_df.printSchema()
    except Exception as err:
        print(traceback.print_tb(err.__traceback__))
        send_notification(sns_arn, 'initial', err, 'Getting etl job config')
        sys.exit("--ERROR in getting etl job config--")

    # Getting batch id
    batch_id = get_batch_id(spark_session, hive_context, config_db_name, logs_table, sns_arn)
    # Creating initial logs
    write_logs_df = initial_log_setup(spark_session, hive_context, config_db_name, logs_table, sns_arn)

    i = 0
    while i < config_df.count():
        table_info = {
            'landing_path': str(config_df.select('one_off_path').collect()[i][0]).strip(),
            'landing_format': str(config_df.select('one_off_format').collect()[i][0]).strip(),
            'schema_path': str(config_df.select('schema_path').collect()[i][0]).strip(),
            'table_name': str(config_df.select('table_name').collect()[i][0]).strip(),
            'source': str(config_df.select('source').collect()[i][0]).strip(),
            'partition_key': remove_special_from_value(str(config_df.select('partition_key').collect()[i][0]).strip().lower()),
            'validation_exception': remove_special_from_value(str(config_df.select('validation_exception').collect()[i][0]).strip().lower()),
            'anonymized_fields': remove_special_from_value(str(config_df.select('anonymized_fields').collect()[i][0]).strip()),
            'raw_bucket': str(config_df.select('raw_path').collect()[i][0]).strip(),
            'raw_db': str(config_df.select('raw_db').collect()[i][0]).strip(),
            'row_tag': str(config_df.select('row_tag').collect()[i][0]).strip(),
            'csv_sep': str(config_df.select('csv_separator').collect()[i][0]).strip(),
            'csv_multi_line': True if str(config_df.select('csv_multi_line').collect()[i][0]).strip().lower() == 'true' else False,
            'header': True if str(config_df.select('header').collect()[i][0]).strip().lower() == 'true' else False,
            'data_validation': True if str(config_df.select('data_validation').collect()[i][0]).strip().lower() == 'true' else False,
            'date_column': '',
            'date_column_format': '',
            'source_date_format': str(config_df.select('source_date_format').collect()[i][0]).strip(),
            'primary_key': remove_special_from_value(str(config_df.select('unique_key').collect()[i][0]).strip().lower()),
            'execution_type': 'one-off',
            'raw_transform': False
        }

        print('UNIQUE KEY ->', table_info['primary_key'])

        execution_date = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        print("Started processing source = " + table_info['source'] +" and table name = " + table_info['table_name'])

        table_info['raw_path'] = table_info['raw_bucket'] + table_info['source'] + '/' + table_info['table_name'] + '/'
        write_logs_df = one_off_execution(spark_session, hive_context, batch_id, table_info, execution_date,
                                          write_logs_df)

        i += 1

    write_logs_to_table(spark_session, write_logs_df, log_history_s3_path, sns_arn)

    print('WE ARE READY TO STOP SPARK SESSION!')
    spark_session.stop()
    print('SPARK SESSION STOPED!')
    # quit()
    # sys.exit(0)
