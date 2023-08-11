import sys
from datetime import datetime
import traceback
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from py4j.protocol import Py4JJavaError

if __name__ == '__main__':
    # Read user argument
    # aws_account_id = sys.argv[1]
    # sns_arn = sys.argv[2]
    # glue_db_name = sys.argv[3]
    # glue_table = sys.argv[4]
    # logs_table = sys.argv[5]
    # log_history_s3_path = sys.argv[6]
    # Create spark session
    spark_session = SparkSession \
        .builder \
        .appName("rk_uk_datalake_transformation") \
        .getOrCreate()
    # spark_session = SparkSession.builder.appName("rk_uk_datalake_transformation") \
    #     .config("hive.metastore.connect.retries", 5) \
    #     .config("hive.metastore.client.factory.class",
    #             "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    #     .config("hive.metastore.glue.catalogid", aws_account_id) \
    #     .enableHiveSupport() \
    #     .getOrCreate()
    # spark_session.sparkContext.setLogLevel("ERROR")

    target_path = 's3://rx-uk-datalake-refinedzone/dbo/hello_world/'

    print('READING DATA ...')
    data_df = spark_session.read.parquet('s3://rx-uk-datalake-support-db/dbo/nps_sheet/')

    data_df = data_df.withColumn('partitiondate', lit("2021-08-03"))
    data_df = data_df.withColumn('loadinsertedtimestamp', lit('20210803'))

    hudiOptions = {
        'hoodie.table.name': 'dbo_hello_world',
        'hoodie.datasource.write.recordkey.field': 'event',
        'hoodie.datasource.write.partitionpath.field': 'partitiondate',
        'hoodie.datasource.write.precombine.field': 'loadinsertedtimestamp',  # w_update_dt
        'hoodie.datasource.hive_sync.enable': 'true',
        'hoodie.datasource.hive_sync.table': 'dbo_hello_world',
        'hoodie.datasource.hive_sync.partition_fields': 'partitiondate',
        'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
        'hoodie.datasource.write.hive_style_partitioning': 'true',
        'hoodie.datasource.hive_sync.database': 'uk_refinedzone',
        'hoodie.datasource.hive_sync.assume_date_partitioning': 'true',
        'hoodie.datasource.hive_sync.support_timestamp': 'true',
        'hoodie.index.type': 'GLOBAL_BLOOM',
        'hoodie.bloom.index.update.partition.path': 'true',
        'hoodie.cleaner.commits.retained': 30,
        'hoodie.keep.min.commits': 31,
        'hoodie.keep.max.commits': 35,
        'hoodie.parquet.compression.codec': 'snappy',
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator"
    }

    try:
        print('WRITING DATA ...')
        data_df.write.format('org.apache.hudi') \
            .option('hoodie.datasource.write.operation', 'upsert') \
            .options(**hudiOptions) \
            .mode("overwrite") \
            .save(target_path)
        # .mode("append") \
        # raise Exception("RAISED EXCEPTION!")
    except Exception as err:
        print(err)

    print('WE ARE READY TO STOP SPARK SESSION!')
    spark_session.stop()
    print('SPARK SESSION STOPED!')
    # quit()
    # sys.exit(0)
