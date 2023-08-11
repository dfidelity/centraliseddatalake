from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from datetime import datetime
import sys
import json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from etl_job_modules.s3_functions import has_column, flatten_struct_columns, get_dtype


def procedure_transform(sparksession, data_df, glue_table_name):

    print("procedure_transform step")

    options = {
        'dbo_hello_world': dbo_nps_sheet_procedure
    }

    return options[glue_table_name](sparksession, data_df)


def dbo_nps_sheet_procedure(sparksession, df):
    # sparksession.catalog.setCurrentDatabase("uk_supportdb")
    # df = sparksession.sql('select * from dbo_nps_sheet')
    staging_df = df.filter("event = 'AIX'").withColumn('exhibitornps2017', lit("42")).withColumn('visitornps2017', lit("32"))
    # staging_df = data_df.filter("event = 'AIX'").withColumn('exhibitornps2017', lit("42")).withColumn('visitornps2017', lit("32"))

    # df = staging_df.subtract(df)
    return staging_df


