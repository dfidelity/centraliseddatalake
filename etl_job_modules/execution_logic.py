from pyspark import SparkContext, SparkConf
import boto3
import re
import traceback
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from datetime import datetime
from etl_job_modules.logs_module import write_success_logs, write_error_logs
from etl_job_modules.hudi_operations import hudi_write_s3, hudi_incremental_read
from etl_job_modules.s3_functions import *
from etl_job_modules.utility_functions import anonymize_pii_data
from etl_job_modules.procedure_transformations_uk import procedure_transform
from etl_job_modules.center_data_transformation import raw_data_transform
from etl_job_modules.center_refined_data_transform import refined_data_transform
from etl_job_modules.create_catalog import create_catalog_with_partition
from etl_job_modules.nonhudi_operations import non_hudi_read, s3_write_parquet, \
    spark_multiple_format_read


def incremental_to_raw(sparksession, hive_context, batch_id, table_info, extract_start_datetime,
                       extract_end_datetime, write_logs_df, non_anonymized_bucket, non_anonymized_db):
    try:
        # glue_table_name = table_info['source'] + "_" + table_info['table_name']

        if table_info['overwrite_mode']:
            write_mode = "overwrite"
            sparksession.sql("DROP TABLE IF EXISTS " + table_info['raw_db'] + "." + table_info['table_name'])
        else:
            write_mode = "append"

        if table_info['partition_key'] == '':
            # table_info['partition_key'] = "date_year,date_month,date_day"
            table_info['partition_key'] = "partitiondate"
        # else:
        #     partition_key_list = table_info['partition_key'].split(',')
        #     read_s3_data_df = read_s3_data_df.na.fill(value="0", subset=partition_key_list)

        # read_s3_data_df = non_hudi_read(sparksession, hive_context, batch_id, table_info['source'], glue_table_name, table_info['raw_db'],
        read_s3_data_df = non_hudi_read(sparksession, hive_context, batch_id, table_info,
                                        extract_start_datetime, extract_end_datetime, raw_data_transform, None)
        if read_s3_data_df == -1:
            print("New data wasn't found ...")
            write_logs_df = write_success_logs(sparksession, batch_id, table_info['source'], table_info['table_name'], 'CONVERT',
                                               extract_end_datetime, extract_start_datetime, extract_end_datetime, 0,
                                               write_logs_df)
            return write_logs_df

        print('Counting rows ...')
        row_count = read_s3_data_df.count()
        print('ROW COUNT->', row_count)

        if row_count != 0:
            # read_s3_data_df = filter_valid_data(sparksession, read_s3_data_df, table_info['primary_key'])
            #
            # print('Count rows ...')
            # row_count = read_s3_data_df.count()
            # print('VALID ROW COUNT->', row_count)
            # print('Final DF -> ', read_s3_data_df)

            print('Writing to s3 - > ')

            # non_anonymized_target = non_anonymized_bucket + table_info['source'] + '/' + table_info['table_name'] + '/'
            # if table_info['anonymized_fields'] != '':
            #     hudi_write_s3(read_s3_data_df, non_anonymized_target, table_info['partition_key'], non_anonymized_db,
            #                   table_info['table_name'], table_info['primary_key'], "insert", write_mode)
            #     table_info['anonymized_fields'] = table_info['anonymized_fields'].split(',')
            #     for f in table_info['anonymized_fields']:
            #         read_s3_data_df = anonymize_pii_data(f, read_s3_data_df)

            if table_info['schema_path'] is not None and table_info['schema_path'] != '':
                read_s3_data_df = columns_order_by_schema(read_s3_data_df, table_info['schema_path'])

            read_s3_data_df.printSchema()

            operation = "upsert" if table_info['update_supported'] else "insert"

            # print('Cast wid columns to int type')
            # if table_info['source'] == 'edw':
            #     read_s3_data_df = read_s3_data_df.select([col(c).cast(IntegerType()) if 'wid' in c else col(c).cast("string") for c in data_df.columns])

            # s3_write_parquet(read_s3_data_df, table_info['raw_path'], table_info['partition_key'])
            # hudi_write_s3(read_s3_data_df, table_info['raw_path'], table_info['partition_key'], table_info['raw_db'], glue_table_name, table_info['primary_key'],
            if table_info['non_hudi_mode']:
                create_catalog_with_partition(sparksession, table_info['table_name'], table_info['raw_path'],
                                              table_info['partition_key'], read_s3_data_df, table_info['raw_db'],
                                              write_mode, False)
            else:
                hudi_write_s3(read_s3_data_df, table_info['raw_path'], table_info['partition_key'], table_info['raw_db'],
                          table_info['table_name'], table_info['primary_key'], operation, write_mode)

        write_logs_df = write_success_logs(sparksession, batch_id, table_info['source'], table_info['table_name'],
                                           'CONVERT', extract_end_datetime,
                                           extract_start_datetime, extract_end_datetime, row_count, write_logs_df)
        return write_logs_df
    except Exception as err:
        write_logs_df = write_error_logs(sparksession, batch_id, table_info['source'], table_info['table_name'],
                                         'CONVERT', extract_end_datetime,
                                         extract_start_datetime, extract_end_datetime, 0, write_logs_df, err)
        return write_logs_df


# def realtime_execution(sparksession, hive_context, batch_id, source, table_name, raw_db, refined_db,
#                        extract_start_datetime, extract_end_datetime, write_logs_df, source_path, source_format,
#                        file_path, inserted_date, raw_path, refined_path, partition_key_column, incremental_supported,
#                        update_supported, date_column, date_column_format, primary_key, row_tag="", header=True,
#                        csv_sep="|", csv_multi_line=True):
#     try:
#         glue_table_name = source + "_" + table_name
#
#         read_s3_data_df = non_hudi_realtime_read(sparksession, hive_context, batch_id, glue_table_name, raw_db,
#                                                  extract_end_datetime, source_path, source_format, file_path,
#                                                  inserted_date, date_column, date_column_format, row_tag, header,
#                                                  csv_sep, csv_multi_line)
#         if read_s3_data_df == -1:
#             print('no data in this file ...')
#             write_logs_df = write_success_logs(sparksession, batch_id, source, table_name, 'REALTIME',
#                                                extract_end_datetime, extract_start_datetime, extract_end_datetime, 0,
#                                                write_logs_df)
#             return write_logs_df
#
#         print('Counting rows ...')
#         row_count = read_s3_data_df.count()
#         print('ROW COUNT->', row_count)
#
#         if row_count != 0:
#             read_s3_data_df = filter_valid_data(sparksession, read_s3_data_df, primary_key)
#
#             print('Count rows ...')
#             row_count = read_s3_data_df.count()
#             print('VALID ROW COUNT->', row_count)
#             print('Final DF -> ', read_s3_data_df)
#
#             print('Writing to s3 - > ')
#             if partition_key_column == '':
#                 # partition_key_column = "date_year,date_month,date_day"
#                 partition_key_column = "partitiondate"
#             # s3_write_parquet(read_s3_data_df, target_path, partition_key_column, incremental_supported)
#             s3_hudi_write_raw(read_s3_data_df, raw_path, partition_key_column, incremental_supported, raw_db,
#                               glue_table_name, primary_key)
#             s3_hudi_write_refined(read_s3_data_df, refined_path, partition_key_column, update_supported, refined_db,
#                                   glue_table_name, primary_key)
#
#         write_logs_df = write_success_logs(sparksession, batch_id, source, table_name, 'REALTIME', extract_end_datetime,
#                                            extract_start_datetime, extract_end_datetime, row_count, write_logs_df)
#         return write_logs_df
#     except Exception as err:
#         write_logs_df = write_error_logs(sparksession, batch_id, source, table_name, 'REALTIME', extract_end_datetime,
#                                          extract_start_datetime, extract_end_datetime, 0, write_logs_df, err)
#         return write_logs_df


def from_raw_to_refined(sparksession, batch_id, source, table_name, write_logs_df, raw_db, refined_db, last_updated_date_column,
                    execution_start_date, execution_end_date, raw_path, target_path, partition_key, update_supported,
                    primary_key, refined_transform, raw_to_refined, overwrite_mode, non_hudi_mode):
    try:
        # sparksession.catalog.setCurrentDatabase(raw_db)

        # glue_table_name = source + "_" + table_name

        print('TABLE NAME -> ', table_name)

        print('COLLECTING THE DATA FROM RAW ...')
        if raw_path:
            df = hudi_incremental_read(sparksession, raw_path, execution_start_date)
            df.printSchema()
        else:
            df = None

        if raw_to_refined:
            if refined_transform:
                print("REFINED DATA TRANSFORMATION ...")
                df = refined_data_transform(sparksession, df, table_name, raw_db, refined_db)

            if raw_path:
                df = df.withColumn('loadetlid', lit(batch_id))
            else:
                print("Adding meta data for view!")
                df = add_meta_logic(source, "view", df, "", "", execution_end_date, batch_id, execution_end_date)

            # You can put your custom meta data here

            print('CALCULATING ROW COUNT ...')
            row_count = df.count()
            print('row_count -> ', row_count)

            if row_count != 0:
                if partition_key == '':
                    partition_key = "partitiondate"

                print('WRITING DATA INTO REFINED ...')
                if overwrite_mode:
                    write_mode = "overwrite"
                    sparksession.sql("DROP TABLE IF EXISTS " + refined_db + "." + table_name)
                else:
                    write_mode = "append"
                operation = "upsert" if update_supported else "insert"
                print('OPERATION -> ' + operation)
                print('target_path')
                if non_hudi_mode:
                    create_catalog_with_partition(sparksession, table_name, target_path, partition_key, df, refined_db,
                                                  write_mode, False)
                else:
                    hudi_write_s3(df, target_path, partition_key, refined_db, table_name, primary_key, operation,
                              write_mode)
        else:
            row_count = 0
            if partition_key == '':
                partition_key = "partitiondate"
            partition_key_list = partition_key.split(',')
            partition_key_tuple = tuple(partition_key_list)
            # check if table exists in refinedzone
            create_catalog_with_partition(sparksession, table_name, raw_path, partition_key_tuple, df, refined_db, 'ignore',
                                          'yes')

        write_logs_df = write_success_logs(sparksession, batch_id, source, table_name, 'MOVE', execution_end_date,
                                           execution_start_date, execution_end_date, row_count, write_logs_df)
        return write_logs_df
    except Exception as err:
        write_logs_df = write_error_logs(sparksession, batch_id, source, table_name, 'MOVE', execution_end_date,
                                         execution_start_date, execution_end_date, 0, write_logs_df, err)
        return write_logs_df


def one_off_execution(sparksession, hive_context, batch_id, table_info, execution_date, write_logs_df):
    try:
        # glue_table_name = table_info['source'] + "_" + table_info['table_name']

        # data_df = spark_multiple_format_read(sparksession, table_info['landing_path'], table_info['landing_path'],
        #                                       table_info['source_format'], table_info['row_tag'], table_info['header'],
        #                                      table_info['csv_sep'], table_info['csv_multi_line'])

        sparksession.sql("DROP TABLE IF EXISTS " + table_info['raw_db'] + "." + table_info['table_name'])

        if table_info['partition_key'] == '':
            # table_info['partition_key'] = "date_year,date_month,date_day"
            table_info['partition_key'] = "partitiondate"
        # else:
        #     partition_key_list = table_info['partition_key'].split(',')
        #     data_df = data_df.na.fill(value="0", subset=partition_key_list)

        # data_df = non_hudi_read(sparksession, hive_context, batch_id, table_info['source'], glue_table_name, table_info['raw_db'], execution_date,
        data_df = non_hudi_read(sparksession, hive_context, batch_id, table_info, execution_date,
                                execution_date, None, one_off_meta_data)

        row_count = data_df.count()
        print('ROW COUNT ->', row_count)

        if row_count != 0:
            # data_df = filter_valid_data(sparksession, data_df, table_info['primary_key'])
            # row_count = data_df.count()
            # print('VALID ROW COUNT ->', row_count)

            print('Writing to s3 - > ')

            # non_anonymized_target = non_anonymized_bucket + table_info['source'] + '/' + table_info['table_name'] + '/'
            # if table_info['anonymized_fields'] != '':
            #     hudi_write_s3(data_df, non_anonymized_target, table_info['partition_key'], non_anonymized_db,
            #                   table_info['table_name'], table_info['primary_key'], "insert", "overwrite")
            #     table_info['anonymized_fields'] = table_info['anonymized_fields'].split(',')
            #     for f in table_info['anonymized_fields']:
            #         data_df = anonymize_pii_data(f, data_df)

            print('UNIQUE KEY ->', table_info['primary_key'])

            if table_info['schema_path'] is not None and table_info['schema_path'] != '':
                data_df = columns_order_by_schema(data_df, table_info['schema_path'])

            data_df.printSchema()

            # s3_write_parquet(read_s3_data_df, table_info['raw_path'], table_info['partition_key'])
            # hudi_write_s3(data_df, table_info['raw_path'], table_info['partition_key'], table_info['raw_db'], glue_table_name, table_info['primary_key'], "insert",
            hudi_write_s3(data_df, table_info['raw_path'], table_info['partition_key'], table_info['raw_db'],
                          table_info['table_name'], table_info['primary_key'], "insert", "overwrite")

        write_logs_df = write_success_logs(sparksession, batch_id, table_info['source'], table_info['table_name'],
                                           'ONE-OFF-LOAD', execution_date, execution_date, execution_date, row_count,
                                           write_logs_df)
        return write_logs_df
    except Exception as err:
        print(traceback.print_tb(err.__traceback__))
        write_logs_df = write_error_logs(sparksession, batch_id, table_info['source'], table_info['table_name'],
                                         'ONE-OFF-LOAD', execution_date, execution_date, execution_date, 0,
                                         write_logs_df, err)
        return write_logs_df


def one_off_meta_data(sparksession, data_df, source):
    print('ONE-OFF meta added')
    if has_column(data_df, "odsinsertedby"):
        data_df = data_df.withColumn("loadinserteddate", date_format("odsinserteddate", "yyyy/MM/dd HH:mm:ss"))\
            .withColumn("loadinsertedby", col("odsinsertedby"))\
            .withColumn("loadupdateddate", date_format("odsupdateddate", "yyyy/MM/dd HH:mm:ss"))\
            .withColumn("loadupdatedby", col("odsupdatedby"))\
            .drop("odsetlloadid", "odsinserteddate", "odsupdateddate", "odsinsertedby", "odsupdatedby")
    if has_column(data_df, "odsdeleteflag"):
        data_df = data_df.withColumnRenamed("odsdeleteflag", "deleteflag")
    elif source in ('efm', 'gleanin'):
        data_df = data_df.withColumn("deleteflag", lit("N"))
    return data_df


def merge_test(sparksession, source, table_name, refined_db, refined_path, primary_key):
    try:
        glue_table_name = source + "_" + table_name
        print('TABLE NAME -> ', glue_table_name)

        print('COLLECTING THE DATA FROM REFINED ZONE ...')
        df = hudi_incremental_read(sparksession, refined_path, "1990/01/01 01:01:01")

        print('CALCULATING ROW COUNT ...')
        row_count = df.count()
        print('row_count -> ', row_count)

        print('GETTING UNIQUE ROWS ...')
        key_list = primary_key.split(',')
        df = df.dropDuplicates(key_list)
        unique_count = df.count()
        print('unique_count -> ', unique_count)

        result = 'PASSED'
        if row_count > unique_count:
            result = 'FAILED'

        print(glue_table_name, 'RESULT ->', result)
        return result
    except Exception as err:
        return 'EXCEPTION -> ' + str(err)


def refined_transformations(sparksession, batch_id, source, table_name, write_logs_df, refined_db, execution_end_date,
                            refined_path, partition_key, primary_key):
    try:
        glue_table_name = source + "_" + table_name
        print('TABLE NAME -> ', glue_table_name)

        print('COLLECTING THE DATA FROM REFINED ...')
        df = hudi_incremental_read(sparksession, refined_path)

        df.printSchema()

        updated_df = refined_data_transform(sparksession, source, table_name, df)

        updated_df = updated_df.withColumn('loadetlid', lit(batch_id))

        print('CALCULATING ROW COUNT ...')
        row_count = updated_df.count()
        print('row_count -> ', row_count)

        if row_count != 0:
            if partition_key == '':
                partition_key = "partitiondate"

            print('WRITING DATA INTO REFINED ...')
            hudi_write_s3(updated_df, refined_path, partition_key, refined_db, glue_table_name, primary_key, "upsert",
                          "append")

        write_logs_df = write_success_logs(sparksession, batch_id, source, table_name, 'REFINED-TRANSFORM', execution_end_date,
                                           execution_end_date, execution_end_date, row_count, write_logs_df)
        return write_logs_df
    except Exception as err:
        write_logs_df = write_error_logs(sparksession, batch_id, source, table_name, 'REFINED-TRANSFORM', execution_end_date,
                                         execution_end_date, execution_end_date, 0, write_logs_df, err)
        return write_logs_df


def data_procedures(sparksession, hive_context, batch_id, source, table_name, refined_db, extract_start_datetime,
                    extract_end_datetime, write_logs_df, refined_path, target_path, target_db, partition_key, primary_key):
    try:
        # reading the data
        print("READING THE DATA!")
        df = hudi_incremental_read(sparksession, refined_path, extract_start_datetime)
        # data transformations
        print("TRANSFORMING ...")
        data_df = procedure_transform(sparksession, df, source + "_" + table_name)
        row_count = data_df.count()
        print("ROW COUNT ->", row_count)
        data_df.show()
        # writing the data
        print("WRITING DATA!")
        hudi_write_s3(data_df, target_path, partition_key, refined_db, source + "_" + table_name, primary_key, "upsert",
                      "append")
        write_logs_df = write_success_logs(sparksession, batch_id, source, table_name, 'PROCEDURES',
                                           extract_end_datetime,
                                           extract_end_datetime, extract_end_datetime, row_count, write_logs_df)
        return write_logs_df
    except Exception as err:
        write_logs_df = write_error_logs(sparksession, batch_id, source, table_name, 'PROCEDURES',
                                         extract_end_datetime,
                                         extract_end_datetime, extract_end_datetime, 0, write_logs_df, err)
        return write_logs_df



# def refined_dependent_tables(sparksession, batch_id, source, table_name, write_logs_df, glue_db, last_updated_date_column,
#                     execution_start_date, execution_end_date, raw_path, target_path, partition_key, update_supported,
#                     primary_key):
#     try:
#         glue_table_name = source + "_" + table_name
#         df = sparksession.read.format("org.apache.hudi").load(raw_path)
#
#         row_count = df.count()
#
#         df = refined_dependent_tables_transform(sparksession, df, source, table_name)
#
#         s3_hudi_write_refined(df, target_path, partition_key, update_supported, glue_db, glue_table_name,
#                               primary_key)
#
#         write_logs_df = write_success_logs(sparksession, batch_id, source, table_name, 'MOVE', execution_end_date,
#                                            execution_start_date, execution_end_date, row_count, write_logs_df)
#     except Exception as err:
#         write_logs_df = write_error_logs(sparksession, batch_id, source, table_name, 'MOVE', execution_end_date,
#                                          execution_start_date, execution_end_date, 0, write_logs_df, err)
#         return write_logs_df

