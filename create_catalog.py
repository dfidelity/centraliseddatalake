from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import boto3
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
import time
from pyathenajdbc import connect


def create_catalog_with_partition(sparksession,temp_table_name,s3path,data_format,partition_key,gluedbname,table_name,region_name,staging_path):
    sep=',' #will be used to define the table columns for the glue catalog
    check_mutiple_partition_keys = partition_key.find(',')
    if check_mutiple_partition_keys > 0:
        partition_key_list = partition_key.split(',')
        partition_key = tuple(partition_key_list)
    #Read the S3 path
    print("*************Create schema table*****************")
    sparksession.catalog.setCurrentDatabase(gluedbname) # Set the glue database name to create the table
    s3_data_schema_df=sparksession.sql("DESCRIBE TABLE " + temp_table_name) # read the schema of the data in s3 path
    s3_data_schema_df.createOrReplaceTempView('s3_data_schema_table')
    #Get the column names from schema apart from partitioning columns
    print("*************Read column names - concatenated*****************")
    sqlqueryforcolumns="select CONCAT(col_name, ' ',data_type) from s3_data_schema_table where col_name NOT IN "+str(partition_key)
    s3_data_schema_columns=sparksession.sql(sqlqueryforcolumns).collect()
    #Get the column names in the form of a list
    s3_data_schema_columns_list = [str(i['concat(col_name,  , data_type)']) for i in s3_data_schema_columns]

    #Get the list of partitioning columns
    sqlqueryforpartition="select CONCAT(col_name, ' ',data_type) from s3_data_schema_table where col_name IN "+str(partition_key)
    s3_data_schema_partition=sparksession.sql(sqlqueryforpartition).collect()
    #Get the partition column names in the form of a list
    s3_data_schema_partition_list = [str(j['concat(col_name,  , data_type)']) for j in s3_data_schema_partition]

    #Create table in glue catalog
    print("*************Execute create table query*****************")
    # sqlquerytocreatetable="CREATE external TABLE IF NOT EXISTS "+table_name+" ("+sep.join(s3_data_schema_columns_list)+") PARTITIONED BY ("+sep.join(s3_data_schema_partition_list)+") STORED AS PARQUET LOCATION '"+s3path+"'"
    sqlquerytocreatetable="CREATE external TABLE IF NOT EXISTS "+table_name+" ("+sep.join(s3_data_schema_columns_list)+") PARTITIONED BY ("+sep.join(s3_data_schema_partition_list)+") STORED AS " + data_format + " LOCATION '"+s3path+"'"
    sparksession.sql(sqlquerytocreatetable)
    print("*************created table and call athena table repair*****************")
    athena_table_repair(table_name,gluedbname,s3path,region_name,staging_path)
    print("*************SUCCESS*****************")
    return

def create_catalog_without_partition(sparksession,temp_table_name,s3path,data_format,gluedbname,table_name,region_name,staging_path):
    sep=',' #will be used to define the table columns for the glue catalog
    print("*************Create schema table*****************")
    sparksession.catalog.setCurrentDatabase(gluedbname) # Set the glue database name to create the table
    s3_data_schema_df=sparksession.sql("DESCRIBE TABLE " + temp_table_name) # read the schema of the data in s3 path
    s3_data_schema_df.createOrReplaceTempView('s3_data_schema_table')

    #Get the column names from schema
    print("*************Read column names - concatenated*****************")
    sqlqueryforcolumns="select CONCAT(col_name, ' ',data_type) from s3_data_schema_table"
    s3_data_schema_columns=sparksession.sql(sqlqueryforcolumns).collect()
    #Get the column names in the form of a list
    s3_data_schema_columns_list = [str(i['concat(col_name,  , data_type)']) for i in s3_data_schema_columns]

    #Create table in glue catalog
    print("*************Execute query to create table*****************")
    # sqlquerytocreatetable="CREATE external TABLE IF NOT EXISTS "+table_name+" ("+sep.join(s3_data_schema_columns_list)+") STORED AS PARQUET LOCATION '"+s3path+"'"
    sqlquerytocreatetable="CREATE external TABLE IF NOT EXISTS "+table_name+" ("+sep.join(s3_data_schema_columns_list)+") STORED AS " + data_format + " LOCATION '"+s3path+"'"
    sparksession.sql(sqlquerytocreatetable)
    print("*************created table and call athena table repair*****************")
    athena_table_repair(table_name,gluedbname,s3path,region_name,staging_path)
    print("*************SUCCESS*****************")
    return

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
