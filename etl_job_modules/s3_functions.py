from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
import boto3
import re
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from datetime import datetime


def string_to_timestamp(str_date, date_format):
    str_date = str_date[:19]
    element = datetime.strptime(str_date, date_format)
    timestamp = datetime.timestamp(element)
    return timestamp


def get_date_parts(date, part):
    date = date.split(' ')[0]
    if part == 'month':
        return date.split('/')[0]
    if part == 'day':
        return date.split('/')[1]
    # year
    return date.split('/')[2]


def filter_valid_data(sparksession, df, primary_key):
    print("START FILTER VALID DATA ...")
    print("CREATING FILTER QUERY ...")
    multiple_keys = primary_key.find(',')
    filter_query = ''
    if multiple_keys > 0:
        unique_key_list = primary_key.split(',')
        for key in unique_key_list:
            if has_column(df, key):
                filter_query += '( ' + key + " != '' and " + key + " is not null ) or "
        if filter_query != '':
            filter_query = filter_query[:-4]
    else:
        if has_column(df, primary_key):
            filter_query = primary_key + " != '' and " + primary_key + " is not null"
        # unique_key_list = [primary_key]
    if filter_query != '':
        print("FILTERING DATA ...")
        valid_df = df.filter(filter_query)
        print("GETTING INVALID DATA ...")
    else:
        # fix - mark all data as invalid
        valid_df = df
    return valid_df


def create_blank_df(sparksession, hive_context, db, table_name, schema_path):
    print("Creating blank DF...")
    try:
        current_table = hive_context.table(db + "." + table_name)
        current_table.registerTempTable(table_name + "_temp")
        schema_df = sparksession.sql("SELECT * FROM " + table_name + "_temp LIMIT 1")
        blank_df = sparksession.createDataFrame([], schema_df.schema)
        drop_list = []
        for c in schema_df.dtypes:
            if c[0][0] == '_':
                drop_list.append(c[0])
        blank_df = blank_df.drop(*drop_list)
        print("Table schema exists -> ", db, table_name)
        blank_df.printSchema()
    except Exception as err:
        print('No DB or table! -> ', db, table_name)
        blank_df = sparksession.createDataFrame(sparksession.sparkContext.emptyRDD(), StringType())
        blank_df = blank_df.withColumnRenamed('value', 'blank_df_field')
        if schema_path is not None and schema_path != '':
            json_schema = read_json_from_s3(schema_path)
            for col_name in json_schema:
                blank_df = blank_df.withColumn(col_name, lit(None).cast(json_schema[col_name]))
            blank_df.drop('blank_df_field')
            blank_df.printSchema()

    return blank_df


def get_schema_difference(df, table_df):
    print('Getting schema difference...')
    return set(table_df.dtypes) - set(df.dtypes)


def convert_to_partition_format(date, date_format):
    # print('CONVERT DATE TO PARTITION FORMAT')
    # print('DATE -> ', date)
    # print('FORMAT -> ', date_format)
    if date_format == "":
        date_format = "%Y/%m/%d %H:%M:%S"
    date = date[:19]
    partition_date = datetime.strptime(date, date_format)
    return partition_date.strftime("%Y-%m-%d")
    # return partition_date.strftime("%Y/%m/%d")


def filter_by_period(bucket, prefix, from_datetime, to_datetime):
    s3 = boto3.client('s3')
    result = []
    from_datetime = datetime.strptime(from_datetime, "%Y/%m/%d %H:%M:%S")
    to_datetime = datetime.strptime(to_datetime, "%Y/%m/%d %H:%M:%S")
    print("Bucket name -> " + bucket)
    print("prefix -> " + prefix)
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    # obj_response = s3.list_objects(Bucket=bucket, Prefix=prefix, MaxKeys=10000)
    # print(obj_response)
    # obj_list = obj_response['Contents']
    for page in pages:
        for obj in page['Contents']:
            obj_datetime = obj['LastModified'].replace(tzinfo=None)
            if obj['Size'] != 0 and obj_datetime > from_datetime and obj_datetime <= to_datetime:
                result.append(('s3://' + bucket + '/' + obj['Key'], obj_datetime.strftime("%Y/%m/%d %H:%M:%S"), obj['Size']))
    return result


def get_latest_one(bucket, prefix, from_datetime, to_datetime):
    s3 = boto3.client('s3')
    result = []
    from_datetime = datetime.strptime(from_datetime, "%Y/%m/%d %H:%M:%S")
    to_datetime = datetime.strptime(to_datetime, "%Y/%m/%d %H:%M:%S")
    latest_date = from_datetime
    print("Bucket name -> " + bucket)
    print("prefix -> " + prefix)
    obj_response = s3.list_objects(Bucket=bucket, Prefix=prefix)
    print(obj_response)
    obj_list = obj_response['Contents']
    for obj in obj_list:
        obj_datetime = obj['LastModified'].replace(tzinfo=None)
        if obj['Size'] != 0 and obj_datetime > from_datetime and obj_datetime <= to_datetime:
            if obj_datetime > latest_date:
                latest_date = obj_datetime
                result = [('s3://' + bucket + '/' + obj['Key'], obj_datetime.strftime("%Y/%m/%d %H:%M:%S"))]
    return result


def check_source_partition(source_path, file_path):
    print('Start checking source partitioning ...')
    source_partition = []
    file_path = file_path.replace(source_path, '')
    if '/' in file_path:
        col_list = file_path.split('/')
        col_list = col_list[:-1]
        for col in col_list:
            partition_col = {
                'title': col.split('=')[0],
                'value': col.split('=')[1]
            }
            source_partition.append(partition_col)
    print('source_partition -> ', source_partition)
    return source_partition


def clean_special(string):
    # return re.sub('\W+', '', string.lower())
    return re.sub('[^A-Za-z0-9]+', '', string.lower())


def remove_special_from_col(df):
    print('removing specials ... ')
    col_names = df.schema.names
    for col in col_names:
        new_name = clean_special(col)
        if new_name != '':
            df = df.withColumnRenamed(col, clean_special(col))
        else:
            df = df.drop(col)
    return df


def remove_special_from_value(field_value):
    value_list = field_value.split(',')
    updated_list = []
    for v in value_list:
        updated_list.append(clean_special(v))
    return ','.join(updated_list)


def camel_to_snake(title):
    pattern = re.compile('((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))')
    return pattern.sub(r'_\1', title).lower()


def columns_camel_to_snake(df):
    print('from camel to snake ... ')
    col_names = df.schema.names
    for col in col_names:
        df = df.withColumnRenamed(col, camel_to_snake(col))
    return df


def set_df_columns_nullable(sparksession, df):
    print('SETTING DF COLUMNS NULLABLE TRUE')
    for struct_field in df.schema:
        struct_field.nullable = True
    df_mod = sparksession.createDataFrame(df.rdd, df.schema)
    return df_mod


def get_dtype(df,colname):
    return [dtype for name, dtype in df.dtypes if name == colname][0]


def flatten_struct_columns(df):
    print("flatten_struct_columns...")
    flat_cols = [c[0] for c in df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in df.dtypes if c[1][:6] == 'struct']

    flat_df = df.select(flat_cols +
                        [col(nc + '.' + c).alias(nc + '_' + c)
                            for nc in nested_cols
                            for c in df.select(nc + '.*').columns])
    return flat_df


def remove_array_columns(df):
    print("remove_array_columns...")
    filtered_col = [c[0] for c in df.dtypes if c[1][:5] != 'array']
    filtered_df = df.select(filtered_col)
    return filtered_df


def read_json_from_s3(file_path):
    split_path = file_path.split('//')[1]
    split_path = split_path.split('/')
    bucket = split_path.pop(0)
    key = '/'.join(split_path)
    s3 = boto3.resource('s3')
    object_content = s3.Object(bucket, key)
    file_content = object_content.get()['Body'].read().decode('utf-8')
    json_data = json.loads(file_content)
    print("JSON data ->", json_data)
    return json_data


def attach_table_schema(data_df, schema_path, date_format):
    print("Schema path ->", schema_path)
    data_df.printSchema()
    json_schema = read_json_from_s3(schema_path)
    # print(data_df.dtypes)
    data_columns = data_df.dtypes
    update_columns = [name for name, dtype in data_columns if name in json_schema and json_schema[name] != dtype and json_schema[name] != 'timestamp']
    timestamp_columns = [name for name, dtype in data_columns if name in json_schema and json_schema[name] != dtype and json_schema[name] == 'timestamp']
    notype_columns = [name for name, dtype in data_columns if name not in json_schema]
    print("NEW COLUMNS -> ", notype_columns)
    data_df = data_df.select([col(c).cast(json_schema[c]) if c in update_columns else col(c) for c in data_df.columns])
    # data_df = data_df.select([col(c).cast("string") if c in notype_columns else col(c) for c in data_df.columns])
    # data_df.show()
    print('timestamp_columns', timestamp_columns)
    if date_format == 'unix':
        print('unix')
        for col_name in timestamp_columns:
            data_df = data_df.withColumn(col_name, to_timestamp(col(col_name)))
    elif date_format != '':
        print('date_format', date_format)
        for col_name in timestamp_columns:
            # data_df = data_df.withColumn(col_name, to_timestamp(col(col_name), 'M/dd/yyyy h:mm:ss a'))
            data_df = data_df.withColumn(col_name, to_timestamp(col(col_name), date_format))
    else:
        print('no format')
        data_df = data_df.select([col(c).cast("timestamp") if c in timestamp_columns else col(c) for c in data_df.columns])
    data_df = data_df.select([col(c) for c in data_df.columns if c not in notype_columns])
    # get_dtype(data_df, c) != json_schema[c]
    data_df.printSchema()
    # data_df.show()
    return data_df


def columns_order_by_schema(data_df, schema_path):
    json_schema = read_json_from_s3(schema_path)
    schema_columns = [*json_schema]
    data_df = data_df.select([col(c) for c in schema_columns if c in data_df.columns])
    return data_df


def try_parsing_date(text):
    for fmt in ('%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y'):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError('no valid date format found')


def add_meta_logic(source, data_format, data_df, date_column, date_column_format, extract_end_datetime, batch_id, inserted_date):
    data_df = data_df.withColumn('loadetlid', lit(batch_id))

    if date_column != "":
        fill_datetime = datetime.strptime(inserted_date, "%Y/%m/%d %H:%M:%S").strftime(date_column_format)
        data_df = data_df.na.fill(value=fill_datetime, subset=[date_column])
        print("DATE COLUMN EXISTS!")
        data_df = data_df.withColumn('loadinserteddate', data_df[date_column])
    else:
        print("NO DATE COLUMN!")
        print("INSERTED DATE -> ", inserted_date)
        data_df = data_df.withColumn('loadinserteddate', lit(inserted_date))

    data_df = data_df.withColumn('loadinsertedby', lit("INCREMENTAL"))
    data_df = data_df.withColumn('loadupdateddate', lit(extract_end_datetime))
    data_df = data_df.withColumn('loadupdatedby', lit("CONVERT_JOB"))
    # data_df = data_df.withColumn('date_year', year(data_df['loadinserteddate']))
    # data_df = data_df.withColumn('date_month', month(data_df['loadinserteddate']))
    # data_df = data_df.withColumn('date_day', dayofmonth(data_df['loadinserteddate']))
    data_df = data_df.withColumn('loaddatasource', lit(source))
    # data_df = data_df.withColumn("deleteflag", lit("N"))

    print('SETTING PARTITION DATE ...')
    to_partition_format_udf = udf(lambda date_value: convert_to_partition_format(date_value, date_column_format),
                                  StringType())
    data_df = data_df.withColumn('partitiondate', to_partition_format_udf('loadinserteddate'))

    # print('Cast all columns to string type')
    #     data_df = data_df.select([col(c).cast("string") for c in data_df.columns])
    if source == 'edw':
        data_df.printSchema()
        data_df = data_df.select([col(name).cast("long") if dtype[:13] == 'decimal(10,0)' else col(name) for name, dtype in data_df.dtypes])
        data_df.printSchema()

    print('Adding load inserted timestamp ...')
    date_column_format = "%Y/%m/%d %H:%M:%S" if date_column_format == '' else date_column_format
    to_timestamp_format_udf = udf(lambda date_value: string_to_timestamp(date_value, date_column_format), FloatType())
    data_df = data_df.withColumn('loadinsertedtimestamp', to_timestamp_format_udf('loadinserteddate'))
    return data_df


def has_column(df, col_name):
    try:
        df[col_name]
        return True
    except Exception as err:
        print(err)
        return False


def check_schema_updates(data_df, source_df, validation, validation_exception):
    diff_set = get_schema_difference(data_df, source_df)
    print('Schema difference -> ', diff_set)
    if len(diff_set) > 0:
        for d in diff_set:
            # if d[0] == 'blank_df_field':
            #     continue
            if validation and d[0] != 'blank_df_field' and d[0] not in validation_exception:
                err = 'INVALID SCHEMA! field ' + d[0] + ' not found ...'
                raise Exception(err)
            data_df = data_df.withColumn(d[0], lit(None).cast(d[1]))
    new_columns = set(data_df.columns) - set(source_df.columns)
    data_df = data_df.select(source_df.columns + list(new_columns))
    print('Schema updated!')
    return data_df


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


# need to be tested with more data
# if (schema_exists):
#     schema = schema_tools.load(schema_path)
#     read_s3_data_df = sparksession.read.format("com.databricks.spark.xml") \
#         .options(rowTag=row_tag).load(s3path, schema=schema)
# else:
#     read_s3_data_sample = sparksession.read.format("com.databricks.spark.xml") \
#         .options(rowTag=row_tag) \
#         .options(samplingRatio=0.1) \
#         .load(s3path)
#     sample_schema = read_s3_data_sample.schema
#     read_s3_data_df = sparksession.read.format("com.databricks.spark.xml") \
#         .options(rowTag=row_tag).load(s3path, schema=sample_schema)
#     # need also save schema to s3 for the future runs
