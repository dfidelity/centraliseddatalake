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
from etl_job_modules.execution_logic import from_raw_to_refined
from etl_job_modules.s3_functions import remove_special_from_value
from etl_job_modules.utility_functions import send_notification
from etl_job_modules.logs_module import get_batch_id, initial_log_setup, write_logs_to_table, get_last_run_end_date
from etl_job_modules.spark_session_builder import spark_session_builder


if __name__ == '__main__':
    # Reading arguments
    aws_account_id = sys.argv[1]
    sns_arn = sys.argv[2]
    config_db_name = sys.argv[3]
    config_table = sys.argv[4]
    logs_table = sys.argv[5]
    log_history_s3_path = sys.argv[6]
    group_id = sys.argv[7]
    # Spark session creation
    # sparksession = spark_session_builder("Incremental-job-from-landing-to-raw", "337962473469")
    sparksession = spark_session_builder("Incremental-job-from-raw-to-refined-" + group_id, aws_account_id, sns_arn)
    sparksession.sparkContext.setLogLevel("ERROR")
    # Reading config table
    try:
        hive_context = HiveContext(sparksession.sparkContext)
        # config_table = hive_context.table("ukconfig.etl_job_config")
        config_table = hive_context.table(config_db_name + '.' + config_table)
        config_table.registerTempTable("etl_job_config_temp")
        etl_config_df = hive_context.sql("select * from etl_job_config_temp "
                                         "where raw_to_refined = 1 and group_id = '" + group_id + "'").cache()
        # etl_config_df = hive_context.sql("select * from etl_job_config_temp where raw_to_refined = 1 " +
        #                                  "or one_off_to_raw = 1 or landing_to_raw = 1").cache()
        etl_config_df.printSchema()
    except Exception as err:
        send_notification(sns_arn, 'initial', err, 'Getting etl job config')
        sys.exit("--ERROR in getting etl job config--")

    # Getting batch id
    batch_id = get_batch_id(sparksession, hive_context, config_db_name, logs_table, sns_arn)
    # Creating initial logs
    write_logs_df = initial_log_setup(sparksession, hive_context, config_db_name, logs_table, sns_arn)

    i = 0
    while i < etl_config_df.count():
        table_name = str(etl_config_df.select('table_name').collect()[i][0]).strip()
        source = str(etl_config_df.select('source').collect()[i][0]).strip()
        raw_path = str(etl_config_df.select('raw_path').collect()[i][0]).strip()
        update_date_column = remove_special_from_value(str(etl_config_df.select('update_date_column').collect()[i][0]).lower().strip())
        partition_key_column = remove_special_from_value(str(etl_config_df.select('partition_key').collect()[i][0]).lower().strip())
        refined_path = str(etl_config_df.select('refined_path').collect()[i][0]).lower().strip()
        primary_key = remove_special_from_value(str(etl_config_df.select('unique_key').collect()[i][0]).lower().strip())
        raw_db = str(etl_config_df.select('raw_db').collect()[i][0]).strip()
        refined_db = str(etl_config_df.select('refined_db').collect()[i][0]).strip()
        non_hudi_mode = True if str(etl_config_df.select('non_hudi_mode').collect()[i][0]).strip().lower() == 'true' else False
        overwrite_mode = True if str(etl_config_df.select('overwrite_mode').collect()[i][0]).strip().lower() == 'true' else False
        raw_to_refined = True if str(etl_config_df.select('raw_to_refined').collect()[i][0]).strip() == '1' else False
        update_supported = True if str(etl_config_df.select('update_supported').collect()[i][0]).strip().lower() == 'true' else False
        refined_data_transform = True if str(etl_config_df.select('refined_data_transform').collect()[i][0]).strip().lower() == 'true' else False

        # setting extract end datetime fot this run
        extract_end_datetime = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        # setting extract start datetime that depends from the previous run extract end datetime
        extract_start_datetime = get_last_run_end_date(sparksession, hive_context, config_db_name, logs_table,
                                                       'MOVE', table_name, source, sns_arn)

        if raw_path:
            raw_path = raw_path + source + '/' + table_name + '/'
        refined_path = refined_path + source + '/' + table_name + '/'

        write_logs_df = from_raw_to_refined(sparksession, batch_id, source, table_name, write_logs_df, raw_db,
                                            refined_db, update_date_column, extract_start_datetime, extract_end_datetime,
                                            raw_path, refined_path, partition_key_column, update_supported, primary_key,
                                            refined_data_transform, raw_to_refined, overwrite_mode, non_hudi_mode)
        i += 1

    write_logs_to_table(sparksession, write_logs_df, log_history_s3_path, sns_arn)

    sparksession.stop()
