import sys
from datetime import datetime
import traceback
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
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
        .appName("rk_uk_datalake_transformation1") \
        .getOrCreate()
    # spark_session = SparkSession.builder.appName("rk_uk_datalake_transformation") \
    #     .config("hive.metastore.connect.retries", 5) \
    #     .config("hive.metastore.client.factory.class",
    #             "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    #     .config("hive.metastore.glue.catalogid", aws_account_id) \
    #     .enableHiveSupport() \
    #     .getOrCreate()
    # spark_session.sparkContext.setLogLevel("ERROR")

    target_path = 's3://rx-gbs-datalake-test/edw/dw_isg_revn_category_lkp/'

    print('READING DATA ...')
    data_df = spark_session.read.parquet('s3://rx-gbs-datalake-landingzone/urxea3_dw/wc_isg_revn_category_lkp/')

    HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY = "hoodie.datasource.hive_sync.partition_extractor_class"
    NONPARTITION_EXTRACTOR_CLASS_OPT_VAL = "org.apache.hudi.hive.MultiPartKeysValueExtractor"
    KEYGENERATOR_CLASS_OPT_KEY = "hoodie.datasource.write.keygenerator.class"
    NONPARTITIONED_KEYGENERATOR_CLASS_OPT_VAL = "org.apache.hudi.keygen.ComplexKeyGenerator"

    hudiOptions = {
        'hoodie.table.name': 'test_data',
        'hoodie.datasource.write.recordkey.field': 'minorcategorynumber',
        'hoodie.datasource.write.partitionpath.field': '',
        'hoodie.datasource.hive_sync.enable': 'true',
        'hoodie.datasource.hive_sync.table': 'test_data',
        'hoodie.datasource.hive_sync.partition_fields': '',
        'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
        'hoodie.datasource.write.hive_style_partitioning': 'true',
        'hoodie.datasource.hive_sync.database': 'gbs_rawzone',
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
            .option(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, NONPARTITION_EXTRACTOR_CLASS_OPT_VAL) \
            .option(KEYGENERATOR_CLASS_OPT_KEY, NONPARTITIONED_KEYGENERATOR_CLASS_OPT_VAL) \
            .option('hoodie.datasource.write.operation', 'insert') \
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


# from pyspark.sql import SparkSession
#
# sparksession = SparkSession.builder.appName("rk_uk_datalake_transformation").getOrCreate()
#
# glue_db_name = "db_name"
# glue_table_name = "table_name"
# target_path = "s3://..."
#
# sparksession.catalog.setCurrentDatabase(glue_db_name)
# sql_query = "select * from " + glue_table_name
# data_df = sparksession.sql(sql_query)
#
# data_df.write.mode("append").parquet(path=target_path, compression="snappy")
#
# sparksession.stop()
