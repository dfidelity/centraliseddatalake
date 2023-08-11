from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import boto3
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
import time
#from pyathenajdbc import connect
from logs_module import write_success_logs, write_error_logs
from s3_functions import filter_by_period

def create_catalog_with_partition(sparksession,batch_id, source, table_name, write_logs_df,s3path,extract_start_datetime,
                                  extract_end_datetime,partition_key,gluedbname,glue_table_name,region_name,staging_path):
    try:
        sep=',' #will be used to define the table columns for the glue catalog
        check_mutiple_partition_keys = partition_key.find(',')
        if check_mutiple_partition_keys > 0:
            partition_key_list = partition_key.split(',')
            partition_key = tuple(partition_key_list)

        #Read the S3 path
        # split_path = s3path.split('//')[1]
        # split_path = split_path.split('/')
        # bucket = split_path.pop(0)
        # prefix = '/'.join(split_path)
        # paths = filter_by_period(bucket, prefix, extract_start_datetime, extract_end_datetime)
        #
        # paths_list = []
        # for path in paths:
        #     paths_list.append(path[0])

        print("*************Read s3 path*****************")
        # print("Parquet paths -> " + str(paths_list))
        # read_s3_data_df = sparksession.read.parquet(*paths_list)
        read_s3_data_df = sparksession.read.parquet(s3path)
        read_s3_data_df.printSchema()
        row_count = read_s3_data_df.count()
        read_s3_data_df.createOrReplaceTempView('read_s3_data_table') # Create a temp view of the dataframe
        sparksession.catalog.setCurrentDatabase(gluedbname) # Set the glue database name to create the table
        s3_data_schema_df=sparksession.sql("DESCRIBE TABLE read_s3_data_table") # read the schema of the data in s3 path
        s3_data_schema_df.createOrReplaceTempView('s3_data_schema_table')

        #Get the column names from schema apart from partitioning columns
        print("*************Read column names - concatenated*****************")
        sqlqueryforcolumns="select CONCAT(col_name, ' ',data_type) from s3_data_schema_table where col_name NOT IN "+str(partition_key)
        s3_data_schema_columns=sparksession.sql(sqlqueryforcolumns).collect()
        #Get the column names in the form of a list
        s3_data_schema_columns_list = [str(i['concat(col_name,  , data_type)']) for i in s3_data_schema_columns]
        print("schema_columns_list - >", s3_data_schema_columns_list)

        #Get the list of partitioning columns
        print(partition_key)
        sqlqueryforpartition="select CONCAT(col_name, ' ',data_type) from s3_data_schema_table where col_name IN "+str(partition_key)
        s3_data_schema_partition=sparksession.sql(sqlqueryforpartition).collect()
        #Get the partition column names in the form of a list
        s3_data_schema_partition_list = [str(j['concat(col_name,  , data_type)']) for j in s3_data_schema_partition]
        print("schema_partition_list - >", s3_data_schema_partition_list)

        #Create table in glue catalog
        print("*************Execute create table query*****************")
        sqlquerytocreatetable="CREATE external TABLE IF NOT EXISTS "+glue_table_name+" ("+sep.join(s3_data_schema_columns_list)+") PARTITIONED BY ("+sep.join(s3_data_schema_partition_list)+") STORED AS PARQUET LOCATION '"+s3path+"'"
        sparksession.sql(sqlquerytocreatetable)
        print("*************created table and call athena table repair*****************")
        #athena_table_repair(glue_table_name,gluedbname,s3path,region_name,staging_path)
        print("*************SUCCESS*****************")
        write_logs_df = write_success_logs(sparksession, batch_id, source, table_name, 'CATALOG', extract_end_datetime,
                                           extract_start_datetime, extract_end_datetime, row_count, write_logs_df)
        return write_logs_df
    except Exception as err:
        write_logs_df = write_error_logs(sparksession, batch_id, source, table_name, 'CATALOG', extract_end_datetime,
                                         extract_start_datetime, extract_end_datetime, 0, write_logs_df, err)
        return write_logs_df

def create_catalog_without_partition(sparksession,batch_id, source, table_name, write_logs_df,s3path,extract_start_datetime,extract_end_datetime,
                                     gluedbname,glue_table_name,region_name,staging_path):
    try:
        sep=',' #will be used to define the table columns for the glue catalog

        # split_path = s3path.split('//')[1]
        # split_path = split_path.split('/')
        # bucket = split_path.pop(0)
        # prefix = '/'.join(split_path)
        # paths = filter_by_period(bucket, prefix, extract_start_datetime, extract_end_datetime)
        #
        # paths_list = []
        # for path in paths:
        #     paths_list.append(path[0])

        print("*************Read s3 path*****************")
        # print("Parquet paths -> " + str(paths_list))
        # read_s3_data_df=sparksession.read.parquet(*paths_list)
        read_s3_data_df = sparksession.read.parquet(s3path)
        row_count = read_s3_data_df.count()
        read_s3_data_df.createOrReplaceTempView('read_s3_data_table') # Create a temp view of the dataframe
        sparksession.catalog.setCurrentDatabase(gluedbname) # Set the glue database name to create the table
        s3_data_schema_df=sparksession.sql("DESCRIBE TABLE read_s3_data_table") # read the schema of the data in s3 path
        s3_data_schema_df.createOrReplaceTempView('s3_data_schema_table')

        #Get the column names from schema
        print("*************Read column names - concatenated*****************")
        sqlqueryforcolumns="select CONCAT(col_name, ' ',data_type) from s3_data_schema_table"
        s3_data_schema_columns=sparksession.sql(sqlqueryforcolumns).collect()
        #Get the column names in the form of a list
        s3_data_schema_columns_list = [str(i['concat(col_name,  , data_type)']) for i in s3_data_schema_columns]

        #Create table in glue catalog
        print("*************Execute query to create table*****************")
        sqlquerytocreatetable="CREATE external TABLE IF NOT EXISTS "+glue_table_name+" ("+sep.join(s3_data_schema_columns_list)+") STORED AS PARQUET LOCATION '"+s3path+"'"
        sparksession.sql(sqlquerytocreatetable)
        print("*************created table and call athena table repair*****************")
        #athena_table_repair(glue_table_name,gluedbname,s3path,region_name,staging_path)
        print("*************SUCCESS*****************")
        write_logs_df = write_success_logs(sparksession, batch_id, source, table_name, 'CATALOG', extract_end_datetime,
                                           extract_start_datetime, extract_end_datetime, row_count, write_logs_df)
        return write_logs_df
    except Exception as err:
        write_logs_df = write_error_logs(sparksession, batch_id, source, table_name, 'CATALOG', extract_end_datetime,
                                         extract_start_datetime, extract_end_datetime, 0, write_logs_df, err)
        return write_logs_df

"""
def athena_table_repair(table_name,gluedbname,s3path,region_name,staging_path):
    sqlquerytablerepair = "MSCK REPAIR TABLE "+gluedbname+"."+table_name
    athenaconn = connect(s3_staging_dir=staging_path,region_name=region_name)
    print("*************Athena connectivity*****************")
    try:
        with athenaconn.cursor() as cursor:
            cursor.execute(sqlquerytablerepair)
    finally:
        athenaconn.close()

    return
"""