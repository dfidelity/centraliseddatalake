from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession, HiveContext
import boto3
import sys
import traceback
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
import time
from datetime import datetime
from etl_job_modules.spark_session_builder import spark_session_builder
from etl_job_modules.s3_functions import check_schema_updates


if __name__ == '__main__':
    # Reading arguments
    aws_account_id = sys.argv[1]
    sns_arn = sys.argv[2]
    data_source_name = sys.argv[3]
    data_location = sys.argv[4]
    staging_location = sys.argv[5]
    cleanup_from = sys.argv[6]
    cleanup_to = sys.argv[7]
    timestamp_column = sys.argv[8]
    partition_column = sys.argv[9]
    unique_key = sys.argv[10]
    print('aws_account_id ->', aws_account_id)
    print('sns_arn ->', sns_arn)
    print('data_source_name ->', data_source_name)
    print('data_location ->', data_location)
    print('staging_location ->', staging_location)
    print('cleanup_from ->', cleanup_from)
    print('cleanup_to ->', cleanup_to)
    print('timestamp_column ->', timestamp_column)
    print('partition_column ->', partition_column)

    # Spark session creation
    sparksession = spark_session_builder("One-off-cleanup-job", aws_account_id, sns_arn)
    sparksession.sparkContext.setLogLevel("ERROR")

    print("reading initial data <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    data_df = sparksession.read.options(basePath=data_location, pathGlobFilter="*.snappy.parquet").format("parquet").load(data_location + "*")
    # data_df = sparksession.read.options(basePath=data_location).format("parquet").load(data_location + "*")
    print('ROW COUNT ->', data_df.count())
    data_df.printSchema()

    print("divide one-off period records and the else <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    filter_query = timestamp_column + " >= '" + cleanup_from + "' and " + timestamp_column + " <= '" + cleanup_to + "'"
    oneoff_period_data = data_df.filter(filter_query)
    print('oneoff_period_data ROW COUNT ->', oneoff_period_data.count())
    other_data = data_df.exceptAll(oneoff_period_data)

    print("cleanup one-off data <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    unique_key_list = unique_key.split(',')
    oneoff_period_data = oneoff_period_data.dropDuplicates(unique_key_list)
    print('oneoff_period_data ROW COUNT after deduplication ->', oneoff_period_data.count())

    print("union data back in one dataframe <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    data_df = other_data.unionByName(oneoff_period_data)

    print("writing data to staging <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    staging_path = staging_location + data_source_name + '/'
    check_mutiple_partition_keys = partition_column.find(',')
    if check_mutiple_partition_keys > 0:
        partition_key_list = partition_column.split(',')
        partition_key_tuple = tuple(partition_key_list)
        data_df.repartition(1).write.mode("overwrite").partitionBy(partition_key_tuple).parquet(path=staging_path, compression="snappy")
    else:
        data_df.repartition(1, partition_column).write.mode("overwrite").partitionBy(partition_column).parquet(path=staging_path, compression="snappy")

    split_path = data_location.split('//')[1]
    split_path = split_path.split('/')
    data_bucket = split_path.pop(0)
    data_prefix = '/'.join(split_path)
    s3 = boto3.resource('s3')
    s3bucket = s3.Bucket(data_bucket)

    if data_source_name == 'productsdata':
        copy_rule = []
        split_path = staging_path.split('//')[1]
        split_path = split_path.split('/')
        staging_bucket = split_path.pop(0)
        staging_prefix = '/'.join(split_path)
        for obj in s3bucket.objects.filter(Prefix=data_prefix):
            if not obj.key.endswith('.snappy.parquet') and obj.key.endswith('.parquet'):
                data_obj_path = {
                    'Bucket': data_bucket,
                    'Key': obj.key
                }
                data_obj_key = obj.key.replace(data_prefix, staging_prefix)
                staging_obj_path = {
                    'Bucket': staging_bucket,
                    'Key': data_obj_key
                }
                copy_rule.append((data_obj_path, staging_obj_path))
        print('COPY RULE ->', copy_rule)
        for obj in copy_rule:
            s3.meta.client.copy(obj[0], obj[1]['Bucket'], obj[1]['Key'])


    print("delete data from initial location <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")

    s3bucket.objects.filter(Prefix=data_prefix).delete()
    print('OLD DATA DELETED ->')

    print("writing data to initial location <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    split_path = staging_path.split('//')[1]
    split_path = split_path.split('/')
    staging_bucket = split_path.pop(0)
    staging_prefix = '/'.join(split_path)
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=staging_bucket, Prefix=staging_prefix)
    copy_rule = []
    for page in pages:
        for obj in page['Contents']:
            if not obj['Key'].endswith('/'):
                staging_obj_path = {
                    'Bucket': staging_bucket,
                    'Key': obj['Key']
                }
                data_obj_key = obj['Key'].replace(staging_prefix, data_prefix)
                # if check_mutiple_partition_keys > 0:
                #     for part_col in partition_key_list:
                #         data_obj_key = data_obj_key.replace(part_col + '=', '')
                # else:
                #     data_obj_key = data_obj_key.replace(partition_column + '=', '')
                data_obj_path = {
                    'Bucket': data_bucket,
                    'Key': data_obj_key
                }
                copy_rule.append((staging_obj_path, data_obj_path))
    print('COPY RULE ->', copy_rule)

    s3 = boto3.resource('s3')
    for obj in copy_rule:
        s3.meta.client.copy(obj[0], obj[1]['Bucket'], obj[1]['Key'])

    sparksession.stop()
