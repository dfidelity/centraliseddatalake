from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from datetime import datetime
from etl_job_modules.s3_functions import check_source_partition, filter_by_period, get_latest_one, create_blank_df,\
    add_meta_logic, check_schema_updates, remove_special_from_col, set_df_columns_nullable, attach_table_schema


def non_hudi_read(sparksession, hive_context, batch_id, table_info, extract_start_datetime,
                  extract_end_datetime, data_transformation, custom_meta_logic):
    blank_df = create_blank_df(sparksession, hive_context, table_info['raw_db'], table_info['table_name'], table_info['schema_path'])
    read_s3_data_df = blank_df

    if table_info['execution_type'] == "batch":
        split_path = table_info['landing_path'].split('//')[1]
        split_path = split_path.split('/')
        bucket = split_path.pop(0)
        prefix = '/'.join(split_path)
        paths = filter_by_period(bucket, prefix, extract_start_datetime, extract_end_datetime)
        paths = sorted(paths, key=lambda x: x[1])
    else:
        paths = [(table_info['landing_path'], extract_end_datetime)]

    print('Paths - > ', len(paths))
    print('Paths - > ', paths)

    if len(paths) == 0:
        return -1

    if table_info['source_partition_to_col']:
        for i, path in enumerate(paths):
            print('PATH NUMBER -> ', i)
            print('PATH -> ', path[0])
            print('PATH LIST LEN -> ', len(paths))

            # print(sparksession.sparkContext.getConf().getAll())

            data_df = spark_multiple_format_read(sparksession, table_info['landing_path'], [path[0]],
                                                 table_info['landing_format'], table_info['row_tag'], table_info['header'],
                                                 table_info['csv_sep'], table_info['csv_multi_line'])

            source_partition = check_source_partition(table_info['landing_path'], path[0])

            if table_info['raw_transform']:
                data_df = data_transformation(sparksession, data_df, table_info, source_partition)
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
            data_df = add_meta_logic(table_info['source'], table_info['landing_format'], data_df, table_info['date_column'],
                                     table_info['date_column_format'], extract_end_datetime, batch_id, path[1])

            print('Adding custom meta data ...')
            if custom_meta_logic:
                data_df = custom_meta_logic(sparksession, data_df, table_info['source'])

            print('Changing data types according the table schema ...')
            if table_info['schema_path'] is not None and table_info['schema_path'] != '':
                data_df = attach_table_schema(data_df, table_info['schema_path'], table_info['source_date_format'])

            print('Checking schema updates ..')
            data_df = check_schema_updates(data_df, read_s3_data_df, table_info['data_validation'], table_info['validation_exception'])

            if i == 0:
                read_s3_data_df = data_df
            else:
                data_df = check_schema_updates(data_df, read_s3_data_df, False, '')
                read_s3_data_df = check_schema_updates(read_s3_data_df, data_df, False, '')
                read_s3_data_df = read_s3_data_df.unionByName(data_df)
    else:
        print('reading all files together')
        path_list = [path[0] for path in paths]

        data_df = spark_multiple_format_read(sparksession, table_info['landing_path'], path_list,
                                             table_info['landing_format'], table_info['row_tag'], table_info['header'],
                                             table_info['csv_sep'], table_info['csv_multi_line'])

        if table_info['raw_transform']:
            data_df = data_transformation(sparksession, data_df, table_info, [])
            if data_df == -1:
                print('no data ...')
                read_s3_data_df = read_s3_data_df.drop('blank_df_field')
                return read_s3_data_df

        print('Remove special char from cols ...')
        data_df = remove_special_from_col(data_df)

        print('Adding meta data ...')
        data_df = add_meta_logic(table_info['source'], table_info['landing_format'], data_df, table_info['date_column'],
                                 table_info['date_column_format'], extract_end_datetime, batch_id, extract_end_datetime)

        print('Adding custom meta data ...')
        if custom_meta_logic:
            data_df = custom_meta_logic(sparksession, data_df, table_info['source'])

        print('Changing data types according the table schema ...')
        if table_info['schema_path'] is not None and table_info['schema_path'] != '':
            data_df = attach_table_schema(data_df, table_info['schema_path'], table_info['source_date_format'])

        print('Checking schema updates ..')
        data_df = check_schema_updates(data_df, read_s3_data_df, table_info['data_validation'],
                                       table_info['validation_exception'])

        read_s3_data_df = data_df

    read_s3_data_df = read_s3_data_df.drop('blank_df_field')
    # if table_info['source'] != 'edw':
    #     read_s3_data_df = set_df_columns_nullable(sparksession, read_s3_data_df)

    return read_s3_data_df


def spark_multiple_format_read(sparksession, source_path, path, source_format, row_tag, header, csv_sep, csv_multi_line):
    # basePath is not the best solution in our case because doesnt work with xml format
    print('Reading the data ...')
    if source_format == 'xml':
        data_df = sparksession.read.format("com.databricks.spark.xml") \
            .options(rowTag=row_tag, valueTag='valueField', mode='DROPMALFORMED', inferSchema=False).load(','.join(path))
        # data_df = sparksession.read.format("xml") \
        #     .options(rowTag=row_tag).load(path)
        # data_df.printSchema()
    elif source_format == 'json':
        data_df = sparksession.read.json(*path, multiLine=csv_multi_line)
        # data_df = sparksession.read.options(basePath=source_path).json(path, multiLine=csv_multi_line)
    elif source_format == 'csv':
        data_df = sparksession.read.csv(*path, header=header, inferSchema=True,
                                        sep=csv_sep, multiLine=csv_multi_line)
    elif source_format == 'parquet':
        data_df = sparksession.read.parquet(*path)
        # data_df.printSchema()
    else:
        data_df = sparksession.read.text(*path)

    # print('Data_df - > ', data_df)
    return data_df


def s3_write_parquet(df, target_path, partition_key_column):
    mode = "append"
    print('MODE -> ' + mode)
    if partition_key_column == '':
        print("no partition key")
        df.write.mode(mode).parquet(path=target_path, compression="snappy")
    else:
        check_mutiple_partition_keys = partition_key_column.find(',')
        if check_mutiple_partition_keys > 0:
            partition_key_list = partition_key_column.split(',')
            partition_key_tuple = tuple(partition_key_list)
            df.write.mode(mode).partitionBy(partition_key_tuple).parquet(path=target_path, compression="snappy")
        else:
            df.write.mode(mode).partitionBy(partition_key_column).parquet(path=target_path, compression="snappy")



