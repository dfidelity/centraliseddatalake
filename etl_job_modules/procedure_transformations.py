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
        'dbo_nps_sheet': dbo_nps_sheet_procedure
    }

    return options[glue_table_name](sparksession, data_df)


def dbo_nps_sheet_procedure(sparksession, df):

    return df


