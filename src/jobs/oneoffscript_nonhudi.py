from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from etl_job_modules.hudi_operations import hudi_snapshot_read
from etl_job_modules.spark_session_builder import spark_session_builder
from etl_job_modules.create_catalog import create_catalog_with_partition


if __name__ == '__main__':
    # Reading arguments
    aws_account_id = sys.argv[1]
    sns_arn = sys.argv[2]
    config_db_name = sys.argv[3]
    config_table = sys.argv[4]
    group_id = sys.argv[7]
    # Spark session creation
    sparksession = spark_session_builder("Incremental-job-from-landing-to-raw-" + group_id, aws_account_id, sns_arn)
    sparksession.sparkContext.setLogLevel("ERROR")
    # Reading config table
    try:
        hive_context = HiveContext(sparksession.sparkContext)
        config_table = hive_context.table(config_db_name + '.' + config_table)
        config_table.registerTempTable("etl_job_config_temp")
        config_df = hive_context.sql("select * from etl_job_config_temp " +
                                     "where landing_to_raw = 1 and group_id = '" + group_id + "'").cache()
        config_df.printSchema()
    except Exception as err:
        print(traceback.print_tb(err.__traceback__))
        sys.exit("--ERROR in getting etl job config--")


    i = 0
    while i < config_df.count():
        table_info = {
            'schema_path': str(config_df.select('schema_path').collect()[i][0]).strip(),
            'table_name': str(config_df.select('table_name').collect()[i][0]).strip(),
            'source': str(config_df.select('source').collect()[i][0]).strip(),
            'partition_key': remove_special_from_value(str(config_df.select('partition_key').collect()[i][0]).strip()),
            'raw_bucket': str(config_df.select('raw_path').collect()[i][0]).strip(),
            'raw_db': str(config_df.select('raw_db').collect()[i][0]).strip(),
            'primary_key': remove_special_from_value(str(config_df.select('unique_key').collect()[i][0]).strip()),
            'update_supported': True if str(config_df.select('update_supported').collect()[i][0]).strip().lower() == 'true' else False
        }

        if table_info['partition_key'] == '':
            table_info['partition_key'] = "partitiondate"

        table_info['raw_path'] = table_info['raw_bucket'] + table_info['source'] + '/' + table_info['table_name'] + '/'

        print("reading source data <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        data_df = hudi_snapshot_read(sparksession, table_info['raw_path'])

        print("transforming source data <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        data_df.createOrReplaceTempView(table_info['table_name'] + "_temp")
        # Perform the SQL query with the join operation
        valid_data_df = sparksession.sql(f"""
            SELECT wg.name as bu, npd.*
            FROM {table_info['table_name']}_temp npd
            LEFT JOIN {table_info['raw_db']}.efm_user_information ui ON npd.ProjectId = ui.projectid
            LEFT JOIN {table_info['raw_db']}.efm_workgroups wg ON ui.group = wg.id
            where wg.name is not null
        """)

        invalid_data_df = sparksession.sql(f"""
            SELECT wg.name as bu, npd.*
            FROM {table_info['table_name']}_temp npd
            LEFT JOIN {table_info['raw_db']}.efm_user_information ui ON npd.ProjectId = ui.projectid
            LEFT JOIN {table_info['raw_db']}.efm_workgroups wg ON ui.group = wg.id
            where wg.name is null
        """)

        invalid_data_df = invalid_data_df.withColumn("bu", lit("RXUK EA "))

        data_df = valid_data_df.unionByName(invalid_data_df)

        data_df.printSchema()


        print("writing data to staging <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        staging_path = table_info['raw_bucket'] + "oneoff/" + table_info['source'] + '/' + table_info['table_name'] + '/'
        data_df.repartition(1).write.mode("overwrite").parquet(path=staging_path, compression="snappy")


        print("reading staging data <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        data_df = sparksession.read.options(basePath=staging_path).format("parquet").load(staging_path + "*")
        # data_df = sparksession.read.format("parquet").load(staging_path)

        table_info['table_name'] = table_info['table_name'] + '_test'
        table_info['raw_path'] = table_info['raw_bucket'] + table_info['source'] + '/' + table_info['table_name'] + '/'

        print("dropping existing table <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        sparksession.sql("DROP TABLE IF EXISTS " + table_info['raw_db'] + "." + table_info['table_name'])


        print("writing to actual location <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        create_catalog_with_partition(sparksession, table_info['table_name'], table_info['raw_path'],
                                      table_info['partition_key'], data_df, table_info['raw_db'],
                                      "overwrite", False)

        i += 1

    sparksession.stop()
