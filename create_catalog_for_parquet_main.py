import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import boto3
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from datetime import datetime
from create_catalog_for_parquet import *
from utility_functions import send_notification
from logs_module import initial_log_setup, get_batch_id, get_last_run_end_date, write_logs_to_table

if __name__ == '__main__':
    try:
        sparksession = SparkSession.builder.appName("Postgrest-parquet-gluecatalog") \
            .config("hive.metastore.connect.retries", 5) \
            .config("hive.metastore.client.factory.class",
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
            .config("hive.metastore.glue.catalogid", "337962473469").enableHiveSupport().getOrCreate()
        sparksession.sparkContext.setLogLevel("ERROR")
    except Exception as err:
        send_notification('initial', err, 'Spark session start')
        sys.exit("--ERROR in starting the spark session--")

    region_name='eu-west-1'
    staging_path='s3://rx-uk-datalake-processingzone/staging/'

    try:
        sparksession.catalog.setCurrentDatabase("ukconfig")
        #etl_config_df = sparksession.read.option("header","true").option("inferSchema", value=True).option("delimiter", ",").csv("s3://rx-gbs-datalake-cit/UK_config/rx-uk-datalake-etl-parameters-tables-raw-to-staging.csv")
        # config_df=sparksession.sql("select * from create_catalog_for_parquet_config")
        config_df=sparksession.sql("select * from etl_job_config where ignore_flag = 0")
    except Exception as err:
        send_notification('initial', err, 'Getting etl job config')
        sys.exit("--ERROR in getting etl job config--")

    batch_id = get_batch_id(sparksession, 'ukconfig', 'etl_execution_log_history')

    write_logs_df = initial_log_setup(sparksession, 'ukconfig', 'etl_execution_log_history')

    extract_end_datetime = datetime.now().strftime("%Y/%m/%d %H:%M:%S")

    zone = 'raw'

    if ("--zone" in sys.argv):
        zone = sys.argv[sys.argv.index("--zone") + 1]

    if zone == 'raw':
        gluedb_field = 'raw_db'
        path_field = 'raw_path'
    else:
        gluedb_field = 'refined_db'
        path_field = 'refined_path'

    print("********************************************START*********************************************")
    config_df.show()
    i = 0
    while i < config_df.count():
        s3path = str(config_df.select('raw_path').collect()[i][0]).strip()
        table_name = str(config_df.select('table_name').collect()[i][0]).strip()
        source = str(config_df.select('source').collect()[i][0]).strip()
        partitionkey = str(config_df.select('partition_key').collect()[i][0]).strip()
        gluedb = str(config_df.select('raw_db').collect()[i][0]).strip()

        # here need to check if glue_table_name exists in config
        #Get the shcema of the table in the database
        s3path_split_list=s3path.split('/')
        print(s3path_split_list)
        while("" in s3path_split_list) :
            s3path_split_list.remove("")

        print("schema_name:",s3path_split_list[-2])

        final_table_name=str(s3path_split_list[-2])+"_"+table_name
        # here need to update the config table with glue_table_name

        extract_start_datetime = get_last_run_end_date(sparksession, 'ukconfig', 'etl_execution_log_history',
                                                       'CATALOG', table_name, source)

        if(partitionkey==""):
            print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%CALLING WITHOUT PARTITION")
            write_logs_df = create_catalog_without_partition(sparksession,batch_id,source,table_name,write_logs_df,
                                                             s3path,extract_start_datetime,extract_end_datetime,gluedb,
                                                             final_table_name,region_name,staging_path)
        else:
            print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%CALLING WITH PARTITION")
            write_logs_df = create_catalog_with_partition(sparksession,batch_id,source,table_name,write_logs_df,s3path,
                                                          extract_start_datetime,extract_end_datetime,partitionkey,
                                                          gluedb,final_table_name,region_name,staging_path)
        i += 1

    write_logs_to_table(sparksession, write_logs_df, 's3://rx-uk-datalake-service-logs/etl_execution_log_history/')

    overwrite_s3_df=sparksession.createDataFrame([("test1","test2")],schema=['test1','test2'])
    overwrite_s3_df.write.parquet(path=staging_path, mode="overwrite",compression="snappy")
    sparksession.stop()
