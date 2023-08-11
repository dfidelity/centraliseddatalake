import sys
from datetime import datetime
import traceback
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from py4j.protocol import Py4JJavaError
from etl_job_modules.s3_functions import filter_by_period


if __name__ == '__main__':
    spark_session = SparkSession \
        .builder \
        .appName("rk_uk_datalake_transformation1") \
        .getOrCreate()
    target_path = 's3://rx-gbs-datalake-landingzone/urxea3_ps/wc_registration_ps/'
    extract_end_datetime = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    extract_start_datetime = '1990/02/09 12:32:22'
    split_path = target_path.split('//')[1]
    split_path = split_path.split('/')
    bucket = split_path.pop(0)
    prefix = '/'.join(split_path)
    paths = filter_by_period(bucket, prefix, extract_start_datetime, extract_end_datetime)
    print(paths)
    paths = sorted(paths, key=lambda x: x[1])
    print(paths)
    size_limit = 3221225472
    total_size = 0
    current_batch = []
    for path in paths:
        if total_size + path[2] > size_limit:
            break
        current_batch.append(path)
        total_size += path[2]
    print("CURRENT -> ", current_batch)
    paths = list(set(paths) - set(current_batch))
    print("REMANING -> ", paths)
    paths = sorted(paths, key=lambda x: x[1])
    print("REMANING -> ", paths)
    spark_session.stop()


def non_hudi_read(sparksession, hive_context, batch_id, source, glue_table_name, glue_db, extract_start_datetime,
                  extract_end_datetime, source_path, source_format, execution_type, raw_transform, date_column,
                  date_column_format, primary_key, data_validation, validation_exception, data_transformation,
                  custom_meta_logic, source_list, source_data_df, row_tag="", header=True, csv_sep="|", csv_multi_line=True):
    blank_df = create_blank_df(sparksession, hive_context, glue_db, glue_table_name)
    read_s3_data_df = blank_df

    remaining_paths = None
    if execution_type == "batch":
        if not source_list:
            split_path = source_path.split('//')[1]
            split_path = split_path.split('/')
            bucket = split_path.pop(0)
            prefix = '/'.join(split_path)
            print(execution_type)
            paths_list = filter_by_period(bucket, prefix, extract_start_datetime, extract_end_datetime)
            paths_list = sorted(paths_list, key=lambda x: x[1])
        else:
            paths_list = sorted(source_list, key=lambda x: x[1])
        # size_limit = 3221225472
        size_limit = 1073741824
        total_size = 0
        paths = []
        for path in paths_list:
            if total_size + path[2] > size_limit:
                break
            paths.append(path)
            total_size += path[2]
        print("CURRENT -> ", paths)
        remaining_paths = list(set(paths_list) - set(paths))
        print("REMANING -> ", remaining_paths)
    else:
        paths = [(source_path, extract_end_datetime)]

    print('Paths - > ', len(paths))
    print('Paths - > ', paths)

    if len(paths) == 0:
        return -1

    for i, path in enumerate(paths):
        print('PATH NUMBER -> ', i)
        print('PATH -> ', path[0])
        print('PATH LIST LEN -> ', len(paths))

        # print(sparksession.sparkContext.getConf().getAll())

        data_df = spark_multiple_format_read(sparksession, source_path, path[0], source_format, row_tag, header,
                                             csv_sep, csv_multi_line)

        source_partition = check_source_partition(source_path, path[0])

        if raw_transform:
            data_df = data_transformation(sparksession, data_df, glue_table_name, source_partition)
            if data_df == -1:
                print('no data in this file ...')
                continue

        # if data_df.rdd.isEmpty():
        #     continue
        if len(data_df.head(1)) == 0:
            continue

        print('Remove special char from cols ...')
        data_df = remove_special_from_col(data_df)
        # data_df = columns_camel_to_snake(data_df)

        print('Adding meta data ...')
        data_df = add_meta_logic(source, data_df, date_column, date_column_format, extract_end_datetime, batch_id, path[1])

        print('Adding custom meta data ...')
        if custom_meta_logic:
            data_df = custom_meta_logic(sparksession, data_df)

        print('Checking schema updates ..')
        data_df = check_schema_updates(data_df, blank_df, data_validation, validation_exception)

        if i == 0:
            read_s3_data_df = data_df
        else:
            data_df = check_schema_updates(data_df, read_s3_data_df, False, '')
            read_s3_data_df = check_schema_updates(read_s3_data_df, data_df, False, '')
            read_s3_data_df = read_s3_data_df.unionByName(data_df)

    read_s3_data_df = read_s3_data_df.drop('blank_df_field')
    read_s3_data_df = set_df_columns_nullable(sparksession, read_s3_data_df)
    print('NON-HUDI READ DONE!')

    if source_data_df:
        source_data_df = check_schema_updates(source_data_df, read_s3_data_df, False, '')
        read_s3_data_df = check_schema_updates(read_s3_data_df, source_data_df, False, '')
        read_s3_data_df = read_s3_data_df.unionByName(source_data_df)
    if remaining_paths:
        read_s3_data_df = non_hudi_read(sparksession, hive_context, batch_id, source, glue_table_name, glue_db,
                                        extract_start_datetime, extract_end_datetime, source_path, source_format,
                                        execution_type, raw_transform, date_column, date_column_format, primary_key,
                                        data_validation, validation_exception, data_transformation, custom_meta_logic,
                                        remaining_paths, read_s3_data_df, row_tag, header, csv_sep, csv_multi_line)

    return read_s3_data_df
