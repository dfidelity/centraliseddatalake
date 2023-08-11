from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import boto3
import json
import sys
import traceback
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from datetime import datetime
from etl_job_modules.utility_functions import send_notification


def write_logs_to_table(sparksession,write_logs_df,s3_log_history_path,sns_arn,sns_success_arn='',group_id=''):
    #Write all the logs to the table etl_execution_log and maintain a history in etl_execution_log_history
    try:
        # print(write_logs_df.head(30))
        write_logs_df.coalesce(1).write.parquet(path=s3_log_history_path, mode='append', compression='snappy')
        print("Successfully completed writing Etl execution logs to s3")
        read_etl_execution_log_for_errors(sparksession,write_logs_df,sns_arn,sns_success_arn,group_id)
    except Exception as err:
        print(err)
        send_notification(sns_arn, 'initial',err,'Writing logs',group_id)
        sys.exit("--ERROR in WRITING LOGS--")
    return


def read_etl_execution_log_for_errors(sparksession,write_logs_df,sns_arn,sns_success_arn,group_id):
    #Send notifications in case of any errors in the load table logs
    write_logs_df.createOrReplaceTempView('write_logs_table')
    table_names_list = sparksession.sql('select TABLE_NAME from write_logs_table where status="ERROR"').collect()
    #count=etl_log_count_df.count()
    if(len(table_names_list)==0):
        #debugging specific
        print("********************************************NO ERROR")
        if sns_success_arn:
            message = {
                "groupId": group_id,
                "status": "SUCCESS",
                "message": "Step function execution completed succesfully"
            }
            send_notification(sns_success_arn, 'success', message, '')
    else:
        table_names=', \n'.join([str(elem) for elem in table_names_list])
        message = {
            "groupId": group_id,
            "status": "FAILED",
            "message": "Failure in etl on tables: " + str(table_names)
        }
        #','.join(table_names_list)
        #debugging specific
        print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxERROR")
        send_notification(sns_arn,'final',message,'Error in table load')
    return


def write_success_logs(sparksession,batch_id,source,schema_table_name,job_type,start_job_time,extract_start_datetime,
                       extract_end_datetime,row_count,write_logs_df):
    #write a row for each successful table load
    print('SUCCESS!')
    end_job_time = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    status = "SUCCESS"
    message = "Load successfully completed"
    new_row = sparksession.createDataFrame([(batch_id,source,schema_table_name,job_type,status,message,start_job_time,
                                             end_job_time,extract_start_datetime,extract_end_datetime,str(row_count))],
                                           schema=['batch_id', 'source', 'table_name', 'job_type', 'status', 'message',
                                                   'job_start_date', 'job_end_date', 'extract_start_date',
                                                   'extract_end_date', 'row_count'])
    write_logs_df = write_logs_df.union(new_row)
    return write_logs_df


def write_error_logs(sparksession,batch_id,source,schema_table_name,job_type,start_job_time,extract_start_datetime,
                     extract_end_datetime,row_count,write_logs_df,err):
    #write a row for each failed table load
    print('ERROR ->')
    print(str(err))
    print(traceback.print_tb(err.__traceback__))
    end_job_time = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    status = "ERROR"
    message = "ERROR description: " + str(err)
    new_row = sparksession.createDataFrame([(batch_id,source,schema_table_name,job_type,status,message[:4000],start_job_time,
                                             end_job_time,extract_start_datetime,extract_end_datetime,str(row_count))],
                                           schema=['batch_id', 'source', 'table_name', 'job_type', 'status', 'message',
                                                   'job_start_date', 'job_end_date', 'extract_start_date',
                                                   'extract_end_date', 'row_count'])
    write_logs_df = write_logs_df.union(new_row)
    return write_logs_df


def get_batch_id(sparksession,hive_context,glue_db,logs_table_name,sns_arn):
    try:
        batch_id_initials=datetime.now().strftime("%Y%m%d%H%M%S")
        batch_id_table = hive_context.table(glue_db + "." + logs_table_name)
        batch_id_table.registerTempTable(logs_table_name+"_temp")
        last_batch_id_query = "SELECT batch_id FROM " + logs_table_name + "_temp ORDER BY batch_id DESC LIMIT 1"
        batch_df = sparksession.sql(last_batch_id_query)

        if batch_df.count() == 0:
            batch_id = batch_id_initials + "-" + "1"
        else:
            last_batch_id = batch_df.first()['batch_id']
            batch_id = batch_id_initials + "-" + str(int(last_batch_id.split("-")[-1]) + 1)

        return batch_id

    except Exception as err:
        print(err)
        send_notification(sns_arn,'initial',err,'Get Batch ID')
        sys.exit("--ERROR in setting up the batch ID--")


def get_last_run_end_date(sparksession, hive_context, glue_db, logs_table_name, job_type, table_name, source, sns_arn):
    try:
        log_table = hive_context.table(glue_db + "." + logs_table_name)
        log_table.registerTempTable(logs_table_name + "_temp")
        logs_query = "SELECT extract_end_date FROM " + logs_table_name + "_temp WHERE status = 'SUCCESS' and job_type = '" + \
                     job_type + "' and table_name = '" + table_name + "' and source = '" + \
                     source + "' ORDER BY extract_end_date DESC LIMIT 1"
        prev_log_df = sparksession.sql(logs_query)
        if prev_log_df.count() == 0:
            # extract_end_date = '1990/02/09 12:32:22'
            extract_end_date = '2022/02/14 12:32:22'
        else:
            extract_end_date = prev_log_df.first()['extract_end_date']
        return extract_end_date
    except Exception as err:
        print(err)
        send_notification(sns_arn,'initial', err, 'Get last run log')
        sys.exit("--ERROR in fetching the previous log values--")


def initial_log_setup(spark_session,hive_context,glue_db,logs_table_name,sns_arn):
    try:
        log_table = hive_context.table(glue_db + "." + logs_table_name)
        log_table.registerTempTable(logs_table_name + "_temp")
        schema_df = spark_session.sql("SELECT * FROM " + logs_table_name + "_temp LIMIT 1")
        write_logs_df = spark_session.createDataFrame([], schema_df.schema)
        return write_logs_df
    except Exception as err:
        print(err)
        send_notification(sns_arn,'initial', err, 'Get last run log')
        sys.exit("--ERROR in initial log setup--")


