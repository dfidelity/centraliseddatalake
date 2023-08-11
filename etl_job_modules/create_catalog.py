from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import boto3
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
import time


def create_catalog_without_partition(sparksession, table_name, path, stagingDF, glue_db_name, write_mode):
    # session.sql("show databases").show()
    # debugging specific
    print("*****STARTED create_catalog_without_Partition*****")
    sparksession.catalog.setCurrentDatabase(glue_db_name)
    stagingDF.write.saveAsTable(table_name, path=path, mode=write_mode, compression="snappy")
    # debugging specific

    print("*****COMPLETED create_catalog_without_Partition*****")
    return


def create_catalog_with_partition(sparksession, table_name, path, partition_key, stagingDF, glue_db_name, write_mode,
                                  msck_repair_flag):
    # debugging specific
    print("*****STARTED create_catalog_with_Partition*****")
    print("**************TABLENAME, PATH, KEY,DB NAME", table_name, path, partition_key, glue_db_name)

    # session.sql("show databases").show()
    sparksession.catalog.setCurrentDatabase(glue_db_name)
    print("*******SET GLUEDB NAME")

    partition_key = partition_key.split(',')

    stagingDF.write.saveAsTable(table_name, path=path, mode=write_mode, partitionBy=partition_key, compression="snappy")

    if msck_repair_flag:
        msck_repair_query = 'msck repair table ' + glue_db_name + '.' + table_name
        sparksession.sql(msck_repair_query)
    # debugging specific
    print("*****COMPLETED create_catalog_with_Partition*****")
    return
