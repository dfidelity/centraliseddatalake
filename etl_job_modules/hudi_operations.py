from datetime import datetime

HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY="hoodie.datasource.hive_sync.partition_extractor_class"#Class used to extract partition field values into hive partition columns.
NONPARTITION_EXTRACTOR_CLASS_OPT_VAL="org.apache.hudi.hive.MultiPartKeysValueExtractor"
KEYGENERATOR_CLASS_OPT_KEY="hoodie.datasource.write.keygenerator.class"#Key generator class, that implements will extract the key out of incoming Row object
NONPARTITIONED_KEYGENERATOR_CLASS_OPT_VAL="org.apache.hudi.keygen.ComplexKeyGenerator"


def hudi_write_s3(df, target_path, partition_key_column, glue_db_name, table_name, primary_key, operation, mode):
    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': primary_key,  # row_wid,integration_id
        'hoodie.datasource.write.partitionpath.field': partition_key_column, # obu_name,event_alpha_code,event_edition_code
        'hoodie.datasource.write.precombine.field': 'loadinsertedtimestamp',  # w_update_dt
        'hoodie.datasource.write.hive_style_partitioning': 'true',
        # 'hoodie.datasource.write.reconcile.schema': 'true',
        'hoodie.datasource.hive_sync.enable': 'true',
        'hoodie.datasource.hive_sync.database': glue_db_name,
        'hoodie.datasource.hive_sync.table': table_name,
        'hoodie.datasource.hive_sync.partition_fields': partition_key_column, # obu_name,event_alpha_code,event_edition_code
        'hoodie.datasource.hive_sync.assume_date_partitioning': 'false',  # yyyy/mm/dd
        'hoodie.datasource.hive_sync.support_timestamp': 'true',
        'hoodie.index.type': 'GLOBAL_BLOOM',
        'hoodie.bloom.index.update.partition.path': 'true',
        'hoodie.cleaner.commits.retained': 30,
        'hoodie.keep.min.commits': 31,
        'hoodie.keep.max.commits': 35,
        'hoodie.parquet.compression.codec': 'snappy',
        # 'hoodie.parquet.max.file.size': '125MB'
    }
    df.write.format("org.apache.hudi") \
        .option(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, NONPARTITION_EXTRACTOR_CLASS_OPT_VAL) \
        .option(KEYGENERATOR_CLASS_OPT_KEY, NONPARTITIONED_KEYGENERATOR_CLASS_OPT_VAL) \
        .option('hoodie.datasource.write.operation', operation) \
        .options(**hudi_options).mode(mode).save(target_path)


def hudi_incremental_read(sparksession, s3_path, execution_start_date="1990/01/01 00:00:00"):
    # sparksession.read.format("hudi").load(raw_path).createOrReplaceTempView("hudi_trips_snapshot")
    # commits = sparksession.sql("select max(_hoodie_commit_time) as commitTime from  hudi_trips_snapshot").collect()[0]
    # i = int(commits[0]) - 1

    i = datetime.strptime(execution_start_date, "%Y/%m/%d %H:%M:%S").strftime("%Y%m%d%H%M%S")
    print("**********************************", i)
    # i=20201218124601
    read_options = {
        'hoodie.datasource.query.type': 'incremental',
        'hoodie.datasource.read.begin.instanttime': str(i)
    }

    hudi_inc_query_df = sparksession.read.format("org.apache.hudi").options(**read_options).load(s3_path)
    return hudi_inc_query_df


def hudi_snapshot_read(sparksession, s3_path):
    hudi_snapshot_df = sparksession.read.format("org.apache.hudi").load(s3_path)
    return hudi_snapshot_df
