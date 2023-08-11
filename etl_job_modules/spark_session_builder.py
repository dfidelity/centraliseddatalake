import sys
from pyspark.sql import SparkSession
from etl_job_modules.utility_functions import send_notification


def spark_session_builder(app_name, account_id, sns_arn):
    """
    This method used to create spark session builder
    :param app_name: application name for spark session builder
    :param account_id:  AWS account ID for getting glue catalog
    :return: return spark session
    """
    try:
        sparksession = SparkSession.builder.appName(app_name) \
            .config("hive.metastore.connect.retries", 5) \
            .config("hive.metastore.client.factory.class",
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
            .config("hive.metastore.glue.catalogid", account_id) \
            .enableHiveSupport() \
            .getOrCreate()
        return sparksession
    except Exception as err:
        send_notification(sns_arn, 'initial', err, 'Spark session start')
        sys.exit("--ERROR in starting the spark session--")
