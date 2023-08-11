from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from datetime import datetime
import sys
import json
import re
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from etl_job_modules.s3_functions import has_column, flatten_struct_columns, remove_array_columns, get_dtype, columns_order_by_schema
from etl_job_modules.hudi_operations import hudi_incremental_read, hudi_write_s3


def raw_data_transform(sparksession, data_df, table_info, source_partition):

    print("raw_data_transform step")

    options = {
        'salescampaign_member': edw_salescampaign_member_transform,
        'participatingorg': edw_participatingorg_transform,
        'participatingperson': edw_participatingperson_transform,
        'registration': edw_registration_transform,
        'atshowactivity': edw_atshowactivity_transform,
        'exhibitor_admin': edw_exhibitor_admin_transform,
        'workorderlines': edw_wordkorderlines_transform,
        'workorder': edw_workorder_transform,
        'standvisits': edw_standvisits_transform,
        'novapermissions': edw_novapermissions_transform,
        'subscription': edw_subscription_transform,
        'profile_master': edw_profile_master_transform,
        'salesactivity': edw_salesactivity_transform,
        'campaign': edw_campaign_transform,
        'unsubscribe_requests': edw_unsubscribe_requests_transform,
        'employee': edw_employee_transform,
        'gleanin_users': gleanin_users_raw_data_transform,
        'gleanin_events': gleanin_events_raw_data_transform,
        'efm_questionnaireqchoosemany': efm_questionnaireqchoosemany_raw_data_transform,
        'efm_questionnaireqchoosemanychoice': efm_questionnaireqchoosemanychoice_raw_data_transform,
        'efm_questionnaireqchooseone': efm_questionnaireqchooseone_raw_data_transform,
        'efm_questionnaireqchooseonechoice': efm_questionnaireqchooseonechoice_raw_data_transform,
        'efm_questionnaireqessay': efm_questionnaireqessay_raw_data_transform,
        'efm_questionnaireqfillin': efm_questionnaireqfillin_raw_data_transform,
        'efm_questionnaireqfillintextfield': efm_questionnaireqfillintextfield_raw_data_transform,
        'efm_questionnaireqmatrix': efm_questionnaireqmatrix_raw_data_transform,
        'efm_questionnaireqmatrixchoosemanychoice': efm_questionnaire_qmatrix_choosemanychoice_raw_data_transform,
        'efm_questionnaireqmatrixchoosemanytable': efm_questionnaire_qmatrix_choosemanytable_raw_data_transform,
        'efm_questionnaireqmatrixchooseonechoice': efm_questionnaire_qmatrix_chooseonechoice_raw_data_transform,
        'efm_questionnaireqmatrixchooseonetable': efm_questionnaire_qmatrix_chooseonetable_raw_data_transform,
        'efm_questionnaireqmatrixessaytable': efm_questionnaire_qmatrix_essaytable_raw_data_transform,
        'efm_questionnaireqmatrixfillintable': efm_questionnaire_qmatrix_fillintable_raw_data_transform,
        'efm_questionnaireqmatrixtopic': efm_questionnaire_qmatrix_topic_raw_data_transform,
        'efm_survey_auth_count': efm_survey_auth_count_raw_data_transform,
        'efm_survey_response_count': efm_survey_response_count_raw_data_transform,
        'efm_surveyinformation': efm_surveyinformation_raw_data_transform,
        'efm_surveyparticipant': efm_surveyparticipant_raw_data_transform,
        'efm_surveyparticipant_test': efm_surveyparticipant_test_raw_data_transform,
        'efm_surveyresponse': efm_surveyresponse_raw_data_transform,
        'efm_surveyresponseheader': efm_surveyresponseheader_raw_data_transform,
        'efm_user_information': efm_user_information_raw_data_transform,
        'efm_workgroups': efm_workgroups_raw_data_transform,
    }

    return options[table_info['table_name']](sparksession, data_df, source_partition, table_info)


def edw_default_raw_data_transform(sparksession, data_df, raw_bucket, raw_db, event_col=None, event_edition_col=None):
    print("edw_default_raw_data_transform step")

    sparksession.catalog.setCurrentDatabase(raw_db)
    # event_df = hudi_incremental_read(sparksession, raw_bucket + "edw/event/")
    # event_df.createOrReplaceTempView('event_df')
    # businessunit_df = hudi_incremental_read(sparksession, raw_bucket + "edw/businessunit/")
    # businessunit_df.createOrReplaceTempView('businessunit_df')

    # sparksession.catalog.setCurrentDatabase("test_db_raw")

    if event_edition_col:
        # evented_df = hudi_incremental_read(sparksession, raw_bucket + "edw/eventedition/")
        # evented_df.createOrReplaceTempView('evented_df')
        storage_df = sparksession.sql("""
            select distinct t1.rowwid as event_edition_wid,
            t2.rowwid as event_wid,
            t2.eventalphacode as event_alpha_code,
            t1.eventeditioncode as event_edition_code,
            t3.obuname as obu_name
            from eventedition t1
            join event t2 on t1.eventwid = t2.rowwid
            join businessunit t3 on t2.obuwid = t3.rowwid
        """)
        print("event_edition_col exists")
        data_df = data_df.alias('stg') \
            .join(storage_df.alias('str'), [data_df[event_edition_col] == storage_df.event_edition_wid], how='left') \
            .select('stg.*', 'str.event_edition_code', 'str.event_alpha_code', 'str.obu_name')
    elif event_col:
        storage_df = sparksession.sql("""
            select distinct t2.rowwid as event_wid,
            t2.eventalphacode as event_alpha_code,
            t3.obuname as obu_name
            from event t2
            join businessunit t3 on t2.obuwid = t3.rowwid
        """)
        print("event_col exists")
        data_df = data_df.alias('stg') \
            .join(storage_df.alias('str'), data_df[event_col] == storage_df.event_wid, how='left') \
            .select('stg.*', 'str.event_alpha_code', 'str.obu_name')

    print("UPDATED DATA COUNT ->", data_df.count())

    return data_df


def edw_salescampaign_member_transform(sparksession, data_df, source_partition, table_info):

    data_df = edw_default_raw_data_transform(sparksession, data_df, table_info['raw_bucket'], table_info['raw_db'], None, "event_edition_wid")

    return data_df


def edw_participatingorg_transform(sparksession, data_df, source_partition, table_info):

    data_df = edw_default_raw_data_transform(sparksession, data_df, table_info['raw_bucket'], table_info['raw_db'], None, "event_ed_wid")

    return data_df


def edw_participatingperson_transform(sparksession, data_df, source_partition, table_info):

    data_df = edw_default_raw_data_transform(sparksession, data_df, table_info['raw_bucket'], table_info['raw_db'], None, "event_ed_wid")

    return data_df


def edw_registration_transform(sparksession, data_df, source_partition, table_info):

    data_df = edw_default_raw_data_transform(sparksession, data_df, table_info['raw_bucket'], table_info['raw_db'], None, "event_ed_wid")

    return data_df


def edw_atshowactivity_transform(sparksession, data_df, source_partition, table_info):

    data_df = edw_default_raw_data_transform(sparksession, data_df, table_info['raw_bucket'], table_info['raw_db'], None, "event_edition_wid")

    return data_df


def edw_exhibitor_admin_transform(sparksession, data_df, source_partition, table_info):

    data_df = edw_default_raw_data_transform(sparksession, data_df, table_info['raw_bucket'], table_info['raw_db'], None, "event_edition_wid")

    return data_df


def edw_wordkorderlines_transform(sparksession, data_df, source_partition, table_info):

    data_df = edw_default_raw_data_transform(sparksession, data_df, table_info['raw_bucket'], table_info['raw_db'], None, "event_edition_wid")

    return data_df


def edw_workorder_transform(sparksession, data_df, source_partition, table_info):

    data_df = edw_default_raw_data_transform(sparksession, data_df, table_info['raw_bucket'], table_info['raw_db'], None, "event_edition_wid")

    return data_df


def edw_standvisits_transform(sparksession, data_df, source_partition, table_info):

    data_df = edw_default_raw_data_transform(sparksession, data_df, table_info['raw_bucket'], table_info['raw_db'], None, "event_ed_wid")

    return data_df


def edw_novapermissions_transform(sparksession, data_df, source_partition, table_info):

    data_df = edw_default_raw_data_transform(sparksession, data_df, table_info['raw_bucket'], table_info['raw_db'], None, "event_ed_wid")

    return data_df


def edw_subscription_transform(sparksession, data_df, source_partition, table_info):

    data_df = edw_default_raw_data_transform(sparksession, data_df, table_info['raw_bucket'], table_info['raw_db'], "event_wid", None)

    return data_df


def edw_profile_master_transform(sparksession, data_df, source_partition, table_info):

    data_df = edw_default_raw_data_transform(sparksession, data_df, table_info['raw_bucket'], table_info['raw_db'], "event_wid", None)

    return data_df


def edw_salesactivity_transform(sparksession, data_df, source_partition, table_info):

    data_df = edw_default_raw_data_transform(sparksession, data_df, table_info['raw_bucket'], table_info['raw_db'], None, "event_ed_wid")

    return data_df


def edw_campaign_transform(sparksession, data_df, source_partition, table_info):

    data_df = edw_default_raw_data_transform(sparksession, data_df, table_info['raw_bucket'], table_info['raw_db'], None, "event_ed_wid")

    return data_df


def edw_unsubscribe_requests_transform(sparksession, data_df, source_partition, table_info):

    data_df = edw_default_raw_data_transform(sparksession, data_df, table_info['raw_bucket'], table_info['raw_db'], None, "event_ed_wid")

    return data_df


def edw_employee_transform(sparksession, data_df, source_partition, table_info):
    print("edw_employee_transform step")

    update_columns = [
        'adj_service_dt_wid',
        'contract_end_dt_wid',
        'contract_st_dt_wid',
        'emp_hire_dt_wid',
        'orig_hire_dt_wid',
        'vis_pr_postn_dh_wid'
    ]

    data_df = data_df.select([col(c).cast("long") if c in update_columns else col(c) for c in data_df.columns])

    # data_df = data_df.select(
    #     [col(name).cast("long") if dtype[:14] == 'decimal(22,10)' else col(name) for name, dtype in data_df.dtypes])

    return data_df


def gleanin_users_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("gleanin_users_raw_data_transform step")

    if get_dtype(data_df, "data")[:12] == 'array<struct':
        staging_df = data_df.withColumn('exploded', explode('data')).select('exploded.*')
    else:
        return -1
    staging_df = flatten_struct_columns(staging_df)
    staging_df = staging_df.drop('attributes_advocacy_channels')

    staging_df = staging_df.withColumn('attributes_registered_at', ts_to_datetime('attributes_registered_at'))
    staging_df = staging_df.withColumn('attributes_updated_at', ts_to_datetime('attributes_updated_at'))

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


ts_to_datetime = udf(lambda ts_date: datetime.utcfromtimestamp(ts_date).strftime("%Y/%m/%d %H:%M:%S"), StringType())


def gleanin_events_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("gleanin_events_raw_data_transform step")

    if get_dtype(data_df, "data")[:12] == 'array<struct':
        staging_df = data_df.withColumn('exploded', explode('data')).select('exploded.*')
    else:
        return -1
    staging_df = flatten_struct_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def efm_questionnaireqchoosemany_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("efm_questionnaireqchoosemany_raw_data_transform step")

    staging_df = data_df.select('QuestionnairePage.*')
    questionnaire_type = get_dtype(staging_df, "QuestionnairePage")
    if questionnaire_type[:5] == 'array':
        staging_df = staging_df.withColumn('exploded', explode('QuestionnairePage'))\
            .select('exploded.*')
    elif questionnaire_type[:6] == 'struct':
        staging_df = staging_df.select('QuestionnairePage.*')
    else:
        return -1

    try:
        staging_df = staging_df.na.drop(subset=["QChooseMany"])
    except Exception as err:
        print(err)
        return -1

    col_type = get_dtype(staging_df, "QChooseMany")

    if staging_df.count() == 0 or (col_type[:5] != 'array' and col_type[:6] != 'struct'):
        return -1

    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('QChooseMany', explode('QChooseMany'))
    staging_df = staging_df.select('QChooseMany.*')
    staging_df = flatten_struct_columns(staging_df)
    staging_df = staging_df.drop('ChoiceList_Choice')

    text_fields_list = ('Body_Text', 'Heading_Text', 'Instructions_Text', 'ListCaption_Text',)
    for f in text_fields_list:
        if has_column(staging_df, f):
            f_type = get_dtype(staging_df, f)
            if f_type[:6] == 'struct':
                staging_df = staging_df.withColumn(f, col(f + ".valueField"))
            elif f_type[:12] == 'array<struct':
                staging_df = staging_df.withColumn(f, col(f)[0]['valueField'])
            elif f_type[:5] == 'array':
                staging_df = staging_df.withColumn(f, col(f)[0])

    staging_df = flatten_struct_columns(staging_df)

    staging_df = clean_html_tags(staging_df, ('Heading_Text', 'Body_Text',))

    staging_df = remove_array_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def efm_questionnaireqchoosemanychoice_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("efm_questionnaireqchoosemanychoice_raw_data_transform step")

    staging_df = data_df.select('QuestionnairePage.*')
    questionnaire_type = get_dtype(staging_df, "QuestionnairePage")
    if questionnaire_type[:5] == 'array':
        staging_df = staging_df.withColumn('exploded', explode('QuestionnairePage')) \
            .select('exploded.*')
    elif questionnaire_type[:6] == 'struct':
        staging_df = staging_df.select('QuestionnairePage.*')
    else:
        return -1

    try:
        staging_df = staging_df.na.drop(subset=["QChooseMany"])
    except Exception as err:
        print(err)
        return -1

    col_type = get_dtype(staging_df, "QChooseMany")

    if staging_df.count() == 0 or (col_type[:5] != 'array' and col_type[:6] != 'struct'):
        return -1

    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('QChooseMany', explode('QChooseMany'))

    staging_df = staging_df.select('QChooseMany.ChoiceList.Choice',
                                   col("QChooseMany._dbheading").alias("QuestionnaireQChooseManyDbHeading"))

    if get_dtype(staging_df, "Choice")[:5] == 'array':
        staging_df = staging_df.withColumn('Choice', explode('Choice'))
    staging_df = staging_df.select('QuestionnaireQChooseManyDbHeading', 'Choice.*')
    staging_df = flatten_struct_columns(staging_df)

    text_fields_list = ('Label_Text',)
    for f in text_fields_list:
        if has_column(staging_df, f):
            f_type = get_dtype(staging_df, f)
            if f_type[:6] == 'struct':
                staging_df = staging_df.withColumn(f, col(f + ".valueField"))
            elif f_type[:12] == 'array<struct':
                staging_df = staging_df.withColumn(f, col(f)[0]['valueField'])
            elif f_type[:5] == 'array':
                staging_df = staging_df.withColumn(f, col(f)[0])

    staging_df = flatten_struct_columns(staging_df)

    staging_df = clean_html_tags(staging_df, ('Label_Text',))

    staging_df = remove_array_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def efm_questionnaireqchooseone_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("efm_questionnaireqchooseone_raw_data_transform step")

    staging_df = data_df.select('QuestionnairePage.*')
    questionnaire_type = get_dtype(staging_df, "QuestionnairePage")
    if questionnaire_type[:5] == 'array':
        staging_df = staging_df.withColumn('exploded', explode('QuestionnairePage')) \
            .select('exploded.*')
    elif questionnaire_type[:6] == 'struct':
        staging_df = staging_df.select('QuestionnairePage.*')
    else:
        return -1

    try:
        staging_df = staging_df.na.drop(subset=["QChooseOne"])
    except Exception as err:
        print(err)
        return -1

    col_type = get_dtype(staging_df, "QChooseOne")

    if staging_df.count() == 0 or (col_type[:5] != 'array' and col_type[:6] != 'struct'):
        return -1

    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('QChooseOne', explode('QChooseOne'))
    staging_df = staging_df.select('QChooseOne.*')
    staging_df = flatten_struct_columns(staging_df)
    staging_df = staging_df.drop('ChoiceList_Choice')

    text_fields_list = ('Heading_Text', 'Body_Text',)
    for f in text_fields_list:
        if has_column(staging_df, f):
            f_type = get_dtype(staging_df, f)
            if f_type[:6] == 'struct':
                staging_df = staging_df.withColumn(f, col(f + ".valueField"))
            elif f_type[:12] == 'array<struct':
                staging_df = staging_df.withColumn(f, col(f)[0]['valueField'])
            elif f_type[:5] == 'array':
                staging_df = staging_df.withColumn(f, col(f)[0])

    staging_df = flatten_struct_columns(staging_df)

    staging_df = clean_html_tags(staging_df, ('Heading_Text', 'Body_Text',))

    staging_df = remove_array_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def efm_questionnaireqchooseonechoice_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("efm_questionnaireqchooseonechoice_raw_data_transform step")

    staging_df = data_df.select('QuestionnairePage.*')
    questionnaire_type = get_dtype(staging_df, "QuestionnairePage")
    if questionnaire_type[:5] == 'array':
        staging_df = staging_df.withColumn('exploded', explode('QuestionnairePage')) \
            .select('exploded.*')
    elif questionnaire_type[:6] == 'struct':
        staging_df = staging_df.select('QuestionnairePage.*')
    else:
        return -1

    try:
        staging_df = staging_df.na.drop(subset=["QChooseOne"])
    except Exception as err:
        print(err)
        return -1

    col_type = get_dtype(staging_df, "QChooseOne")

    if staging_df.count() == 0 or (col_type[:5] != 'array' and col_type[:6] != 'struct'):
        return -1

    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('QChooseOne', explode('QChooseOne'))

    staging_df = staging_df.select('QChooseOne.ChoiceList.Choice',
                                   col("QChooseOne._dbheading").alias("QuestionnaireQChooseOneDbHeading"))

    if get_dtype(staging_df, "Choice")[:5] == 'array':
        staging_df = staging_df.withColumn('Choice', explode('Choice'))
    staging_df = staging_df.select('QuestionnaireQChooseOneDbHeading', 'Choice.*')
    staging_df = flatten_struct_columns(staging_df)

    text_fields_list = ('Label_Text',)
    for f in text_fields_list:
        if has_column(staging_df, f):
            f_type = get_dtype(staging_df, f)
            if f_type[:6] == 'struct':
                staging_df = staging_df.withColumn(f, col(f + ".valueField"))
            elif f_type[:12] == 'array<struct':
                staging_df = staging_df.withColumn(f, col(f)[0]['valueField'])
            elif f_type[:5] == 'array':
                staging_df = staging_df.withColumn(f, col(f)[0])

    staging_df = flatten_struct_columns(staging_df)

    staging_df = clean_html_tags(staging_df, ('Label_Text',))

    staging_df = remove_array_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def efm_questionnaireqessay_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("efm_questionnaireqessay_raw_data_transform step")

    staging_df = data_df.select('QuestionnairePage.*')
    questionnaire_type = get_dtype(staging_df, "QuestionnairePage")
    if questionnaire_type[:5] == 'array':
        staging_df = staging_df.withColumn('exploded', explode('QuestionnairePage')) \
            .select('exploded.*')
    elif questionnaire_type[:6] == 'struct':
        staging_df = staging_df.select('QuestionnairePage.*')
    else:
        return -1

    try:
        staging_df = staging_df.na.drop(subset=["QEssay"])
    except Exception as err:
        print(err)
        return -1

    col_type = get_dtype(staging_df, "QEssay")

    if staging_df.count() == 0 or (col_type[:5] != 'array' and col_type[:6] != 'struct'):
        return -1

    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('QEssay', explode('QEssay'))
    staging_df = staging_df.select('QEssay.*')
    staging_df = flatten_struct_columns(staging_df)

    text_fields_list = ('Heading_Text', 'Body_Text',)
    for f in text_fields_list:
        if has_column(staging_df, f):
            f_type = get_dtype(staging_df, f)
            if f_type[:6] == 'struct':
                staging_df = staging_df.withColumn(f, col(f + ".valueField"))
            elif f_type[:12] == 'array<struct':
                staging_df = staging_df.withColumn(f, col(f)[0]['valueField'])
            elif f_type[:5] == 'array':
                staging_df = staging_df.withColumn(f, col(f)[0])

    staging_df = flatten_struct_columns(staging_df)

    staging_df = clean_html_tags(staging_df, ('Heading_Text', 'Body_Text',))

    staging_df = remove_array_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def efm_questionnaireqfillin_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("efm_questionnaireqfillin_raw_data_transform step")

    staging_df = data_df.select('QuestionnairePage.*')
    questionnaire_type = get_dtype(staging_df, "QuestionnairePage")
    if questionnaire_type[:5] == 'array':
        staging_df = staging_df.withColumn('exploded', explode('QuestionnairePage')) \
            .select('exploded.*')
    elif questionnaire_type[:6] == 'struct':
        staging_df = staging_df.select('QuestionnairePage.*')
    else:
        return -1

    try:
        staging_df = staging_df.na.drop(subset=["QFillIn"])
    except Exception as err:
        print(err)
        return -1

    col_type = get_dtype(staging_df, "QFillIn")

    if staging_df.count() == 0 or (col_type[:5] != 'array' and col_type[:6] != 'struct'):
        return -1

    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('QFillIn', explode('QFillIn'))
    staging_df = staging_df.select('QFillIn.*')
    staging_df = flatten_struct_columns(staging_df)
    staging_df = staging_df.drop('TextField')

    text_fields_list = ('Heading_Text', 'Body_Text',)
    for f in text_fields_list:
        if has_column(staging_df, f):
            f_type = get_dtype(staging_df, f)
            if f_type[:6] == 'struct':
                staging_df = staging_df.withColumn(f, col(f + ".valueField"))
            elif f_type[:12] == 'array<struct':
                staging_df = staging_df.withColumn(f, col(f)[0]['valueField'])
            elif f_type[:5] == 'array':
                staging_df = staging_df.withColumn(f, col(f)[0])

    staging_df = flatten_struct_columns(staging_df)

    staging_df = clean_html_tags(staging_df, ('Heading_Text', 'Body_Text',))

    staging_df = remove_array_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def efm_questionnaireqfillintextfield_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("efm_questionnaireqfillintextfield_raw_data_transform step")

    staging_df = data_df.select('QuestionnairePage.*')
    questionnaire_type = get_dtype(staging_df, "QuestionnairePage")
    if questionnaire_type[:5] == 'array':
        staging_df = staging_df.withColumn('exploded', explode('QuestionnairePage')) \
            .select('exploded.*')
    elif questionnaire_type[:6] == 'struct':
        staging_df = staging_df.select('QuestionnairePage.*')
    else:
        return -1

    try:
        staging_df = staging_df.na.drop(subset=["QFillIn"])
    except Exception as err:
        print(err)
        return -1

    col_type = get_dtype(staging_df, "QFillIn")

    if staging_df.count() == 0 or (col_type[:5] != 'array' and col_type[:6] != 'struct'):
        return -1

    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('QFillIn', explode('QFillIn'))
    staging_df = staging_df.select('QFillIn.TextField',
                                   col("QFillIn._dbheading").alias("QuestionnaireQFillInDbHeading"))

    if get_dtype(staging_df, "TextField")[:5] == 'array':
        staging_df = staging_df.withColumn('TextField', explode('TextField'))
    staging_df = staging_df.select('QuestionnaireQFillInDbHeading', 'TextField.*')
    staging_df = flatten_struct_columns(staging_df)

    text_fields_list = ('Label_Text',)
    for f in text_fields_list:
        if has_column(staging_df, f):
            f_type = get_dtype(staging_df, f)
            if f_type[:6] == 'struct':
                staging_df = staging_df.withColumn(f, col(f + ".valueField"))
            elif f_type[:12] == 'array<struct':
                staging_df = staging_df.withColumn(f, col(f)[0]['valueField'])
            elif f_type[:5] == 'array':
                staging_df = staging_df.withColumn(f, col(f)[0])

    staging_df = flatten_struct_columns(staging_df)

    staging_df = clean_html_tags(staging_df, ('Label_Text',))

    staging_df = remove_array_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def efm_questionnaireqmatrix_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("efm_questionnaireqmatrix_raw_data_transform step")

    staging_df = data_df.select('QuestionnairePage.*')
    questionnaire_type = get_dtype(staging_df, "QuestionnairePage")
    if questionnaire_type[:5] == 'array':
        staging_df = staging_df.withColumn('exploded', explode('QuestionnairePage')) \
            .select('exploded.*')
    elif questionnaire_type[:6] == 'struct':
        staging_df = staging_df.select('QuestionnairePage.*')
    else:
        return -1

    try:
        staging_df = staging_df.na.drop(subset=["QMatrix"])
    except Exception as err:
        print(err)
        return -1

    col_type = get_dtype(staging_df, "QMatrix")

    if staging_df.count() == 0 or (col_type[:5] != 'array' and col_type[:6] != 'struct'):
        return -1

    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('QMatrix', explode('QMatrix'))
    staging_df = staging_df.select('QMatrix.*')
    staging_df = staging_df.drop('Condition', 'ChooseOneTable', 'ChooseManyTable')
    staging_df = flatten_struct_columns(staging_df)
    staging_df = staging_df.drop('TopicList_Topic')

    text_fields_list = ('Heading_Text', 'Instructions_Text', 'Body_Text', 'ListCaption_Text', 'TopicCell_Text',
                        'TotalLabel_Text', 'Summary_Text', 'Caption_Text',)
    for f in text_fields_list:
        if has_column(staging_df, f):
            f_type = get_dtype(staging_df, f)
            if f_type[:6] == 'struct':
                staging_df = staging_df.withColumn(f, col(f+".valueField"))
            elif f_type[:12] == 'array<struct':
                staging_df = staging_df.withColumn(f, col(f)[0]['valueField'])
            elif f_type[:5] == 'array':
                staging_df = staging_df.withColumn(f, col(f)[0])

    staging_df = flatten_struct_columns(staging_df)

    staging_df = clean_html_tags(staging_df, ('Heading_Text', 'Body_Text',))

    staging_df = remove_array_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def efm_questionnaire_qmatrix_chooseonetable_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("efm_questionnaire_qmatrix_chooseonetable_raw_data_transform step")

    staging_df = data_df.select('QuestionnairePage.*')
    questionnaire_type = get_dtype(staging_df, "QuestionnairePage")
    if questionnaire_type[:5] == 'array':
        staging_df = staging_df.withColumn('exploded', explode('QuestionnairePage')) \
            .select('exploded.*')
    elif questionnaire_type[:6] == 'struct':
        staging_df = staging_df.select('QuestionnairePage.*')
    else:
        return -1

    try:
        staging_df = staging_df.na.drop(subset=["QMatrix"])
    except Exception as err:
        print(err)
        return -1

    col_type = get_dtype(staging_df, "QMatrix")
    if staging_df.count() == 0 or (col_type[:5] != 'array' and col_type[:6] != 'struct'):
        return -1
    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('QMatrix', explode('QMatrix'))
    staging_df = staging_df.select('QMatrix.*').withColumnRenamed('_dbheading', 'QuestionnaireQMatrixDBHeading')

    try:
        staging_df = staging_df.na.drop(subset=["ChooseOneTable"])
    except Exception as err:
        print(err)
        return -1
    col_type = get_dtype(staging_df, "ChooseOneTable")
    if staging_df.count() == 0 or (col_type[:5] != 'array' and col_type[:6] != 'struct'):
        return -1
    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('ChooseOneTable', explode('ChooseOneTable'))
    staging_df = staging_df.select('QuestionnaireQMatrixDBHeading', 'ChooseOneTable.*')
    staging_df = staging_df.drop('Choice')
    staging_df = flatten_struct_columns(staging_df)

    text_fields_list = ('Category_Text',)
    for f in text_fields_list:
        if has_column(staging_df, f):
            f_type = get_dtype(staging_df, f)
            if f_type[:6] == 'struct':
                staging_df = staging_df.withColumn(f, col(f+".valueField"))
            elif f_type[:12] == 'array<struct':
                staging_df = staging_df.withColumn(f, col(f)[0]['valueField'])
            elif f_type[:5] == 'array':
                staging_df = staging_df.withColumn(f, col(f)[0])

    staging_df = flatten_struct_columns(staging_df)

    staging_df = clean_html_tags(staging_df, ('Category_Text',))

    staging_df = remove_array_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def efm_questionnaire_qmatrix_chooseonechoice_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("efm_questionnaire_qmatrix_chooseonechoice_raw_data_transform step")

    staging_df = data_df.select('QuestionnairePage.*')
    questionnaire_type = get_dtype(staging_df, "QuestionnairePage")
    if questionnaire_type[:5] == 'array':
        staging_df = staging_df.withColumn('exploded', explode('QuestionnairePage')) \
            .select('exploded.*')
    elif questionnaire_type[:6] == 'struct':
        staging_df = staging_df.select('QuestionnairePage.*')
    else:
        return -1

    try:
        staging_df = staging_df.na.drop(subset=["QMatrix"])
    except Exception as err:
        print(err)
        return -1

    col_type = get_dtype(staging_df, "QMatrix")
    if staging_df.count() == 0 or (col_type[:5] != 'array' and col_type[:6] != 'struct'):
        return -1
    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('QMatrix', explode('QMatrix'))
    staging_df = staging_df.select('QMatrix.*')

    try:
        staging_df = staging_df.na.drop(subset=["ChooseOneTable"])
    except Exception as err:
        print(err)
        return -1
    col_type = get_dtype(staging_df, "ChooseOneTable")
    if staging_df.count() == 0 or (col_type[:5] != 'array' and col_type[:6] != 'struct'):
        return -1
    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('ChooseOneTable', explode('ChooseOneTable'))
    staging_df = staging_df.select('ChooseOneTable.*').withColumnRenamed('_dbheading', 'QuestionnaireQMatrixChooseOneTableDBHeading')

    col_type = get_dtype(staging_df, "Choice")
    if staging_df.count() == 0 or (col_type[:5] != 'array' and col_type[:6] != 'struct'):
        return -1
    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('Choice', explode('Choice'))
    staging_df = staging_df.select('QuestionnaireQMatrixChooseOneTableDBHeading', 'Choice.*')
    staging_df = flatten_struct_columns(staging_df)

    staging_df = flatten_struct_columns(staging_df)
    staging_df = remove_array_columns(staging_df)
    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def efm_questionnaire_qmatrix_choosemanytable_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("efm_questionnaire_qmatrix_choosemanytable_raw_data_transform step")

    staging_df = data_df.select('QuestionnairePage.*')
    questionnaire_type = get_dtype(staging_df, "QuestionnairePage")
    if questionnaire_type[:5] == 'array':
        staging_df = staging_df.withColumn('exploded', explode('QuestionnairePage')) \
            .select('exploded.*')
    elif questionnaire_type[:6] == 'struct':
        staging_df = staging_df.select('QuestionnairePage.*')
    else:
        return -1

    try:
        staging_df = staging_df.na.drop(subset=["QMatrix"])
    except Exception as err:
        print(err)
        return -1
    col_type = get_dtype(staging_df, "QMatrix")
    if staging_df.count() == 0 or (col_type[:5] != 'array' and col_type[:6] != 'struct'):
        return -1
    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('QMatrix', explode('QMatrix'))
    staging_df = staging_df.select('QMatrix.*').withColumnRenamed('_dbheading', 'QuestionnaireQMatrixDBHeading')

    try:
        staging_df = staging_df.na.drop(subset=["ChooseManyTable"])
    except Exception as err:
        print(err)
        return -1
    col_type = get_dtype(staging_df, "ChooseManyTable")
    if staging_df.count() == 0 or (col_type[:5] != 'array' and col_type[:6] != 'struct'):
        return -1
    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('ChooseManyTable', explode('ChooseManyTable'))
    staging_df = staging_df.select('QuestionnaireQMatrixDBHeading', 'ChooseManyTable.*')
    staging_df = staging_df.drop('Choice')
    staging_df = flatten_struct_columns(staging_df)

    text_fields_list = ('Category_Text',)
    for f in text_fields_list:
        if has_column(staging_df, f):
            f_type = get_dtype(staging_df, f)
            if f_type[:6] == 'struct':
                staging_df = staging_df.withColumn(f, col(f + ".valueField"))
            elif f_type[:12] == 'array<struct':
                staging_df = staging_df.withColumn(f, col(f)[0]['valueField'])
            elif f_type[:5] == 'array':
                staging_df = staging_df.withColumn(f, col(f)[0])

    staging_df = flatten_struct_columns(staging_df)

    staging_df = clean_html_tags(staging_df, ('Category_Text',))

    staging_df = remove_array_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def efm_questionnaire_qmatrix_choosemanychoice_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("efm_questionnaire_qmatrix_choosemanychoice_raw_data_transform step")

    staging_df = data_df.select('QuestionnairePage.*')
    questionnaire_type = get_dtype(staging_df, "QuestionnairePage")
    if questionnaire_type[:5] == 'array':
        staging_df = staging_df.withColumn('exploded', explode('QuestionnairePage')) \
            .select('exploded.*')
    elif questionnaire_type[:6] == 'struct':
        staging_df = staging_df.select('QuestionnairePage.*')
    else:
        return -1

    try:
        staging_df = staging_df.na.drop(subset=["QMatrix"])
    except Exception as err:
        print(err)
        return -1

    col_type = get_dtype(staging_df, "QMatrix")
    if staging_df.count() == 0 or (col_type[:5] != 'array' and col_type[:6] != 'struct'):
        return -1
    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('QMatrix', explode('QMatrix'))
    staging_df = staging_df.select('QMatrix.*')

    try:
        staging_df = staging_df.na.drop(subset=["ChooseManyTable"])
    except Exception as err:
        print(err)
        return -1
    col_type = get_dtype(staging_df, "ChooseManyTable")
    if staging_df.count() == 0 or (col_type[:5] != 'array' and col_type[:6] != 'struct'):
        return -1
    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('ChooseManyTable', explode('ChooseManyTable'))
    staging_df = staging_df.select('ChooseManyTable.*')\
        .withColumnRenamed('_dbheading', 'QuestionnaireQMatrixChooseManyTableDBHeading')

    col_type = get_dtype(staging_df, "Choice")
    if staging_df.count() == 0 or (col_type[:5] != 'array' and col_type[:6] != 'struct'):
        return -1
    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('Choice', explode('Choice'))
    staging_df = staging_df.select('QuestionnaireQMatrixChooseManyTableDBHeading', 'Choice.*')
    staging_df = flatten_struct_columns(staging_df)

    staging_df = flatten_struct_columns(staging_df)
    staging_df = remove_array_columns(staging_df)
    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def efm_questionnaire_qmatrix_essaytable_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("efm_questionnaire_qmatrix_essaytable_raw_data_transform step")

    staging_df = data_df.select('QuestionnairePage.*')
    questionnaire_type = get_dtype(staging_df, "QuestionnairePage")
    if questionnaire_type[:5] == 'array':
        staging_df = staging_df.withColumn('exploded', explode('QuestionnairePage')) \
            .select('exploded.*')
    elif questionnaire_type[:6] == 'struct':
        staging_df = staging_df.select('QuestionnairePage.*')
    else:
        return -1

    try:
        staging_df = staging_df.na.drop(subset=["QMatrix"])
    except Exception as err:
        print(err)
        return -1

    col_type = get_dtype(staging_df, "QMatrix")
    if staging_df.count() == 0 or (col_type[:5] != 'array' and col_type[:6] != 'struct'):
        return -1
    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('QMatrix', explode('QMatrix'))
    staging_df = staging_df.select('QMatrix.*').withColumnRenamed('_dbheading', 'QuestionnaireQMatrixDBHeading')

    try:
        staging_df = staging_df.na.drop(subset=["EssayTable"])
    except Exception as err:
        print(err)
        return -1
    col_type = get_dtype(staging_df, "EssayTable")
    if staging_df.count() == 0 or (col_type[:5] != 'array' and col_type[:6] != 'struct'):
        return -1
    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('EssayTable', explode('EssayTable'))
    staging_df = staging_df.select('QuestionnaireQMatrixDBHeading', 'EssayTable.*')
    staging_df = flatten_struct_columns(staging_df)

    text_fields_list = ('Category_Text', 'Preselection_Text')
    for f in text_fields_list:
        if has_column(staging_df, f):
            f_type = get_dtype(staging_df, f)
            if f_type[:6] == 'struct':
                staging_df = staging_df.withColumn(f, col(f + ".valueField"))
            elif f_type[:12] == 'array<struct':
                staging_df = staging_df.withColumn(f, col(f)[0]['valueField'])
            elif f_type[:5] == 'array':
                staging_df = staging_df.withColumn(f, col(f)[0])

    staging_df = flatten_struct_columns(staging_df)

    staging_df = clean_html_tags(staging_df, ('Category_Text',))

    staging_df = remove_array_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def efm_questionnaire_qmatrix_fillintable_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("efm_questionnaire_qmatrix_fillintable_raw_data_transform step")

    staging_df = data_df.select('QuestionnairePage.*')
    questionnaire_type = get_dtype(staging_df, "QuestionnairePage")
    if questionnaire_type[:5] == 'array':
        staging_df = staging_df.withColumn('exploded', explode('QuestionnairePage')) \
            .select('exploded.*')
    elif questionnaire_type[:6] == 'struct':
        staging_df = staging_df.select('QuestionnairePage.*')
    else:
        return -1

    try:
        staging_df = staging_df.na.drop(subset=["QMatrix"])
    except Exception as err:
        print(err)
        return -1
    col_type = get_dtype(staging_df, "QMatrix")
    if staging_df.count() == 0 or (col_type[:5] != 'array' and col_type[:6] != 'struct'):
        return -1
    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('QMatrix', explode('QMatrix'))
    staging_df = staging_df.select('QMatrix.*').withColumnRenamed('_dbheading', 'QuestionnaireQMatrixDBHeading')

    try:
        staging_df = staging_df.na.drop(subset=["FillInTable"])
    except Exception as err:
        print(err)
        return -1
    col_type = get_dtype(staging_df, "FillInTable")
    if staging_df.count() == 0 or (col_type[:5] != 'array' and col_type[:6] != 'struct'):
        return -1
    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('FillInTable', explode('FillInTable'))
    staging_df = staging_df.select('QuestionnaireQMatrixDBHeading', 'FillInTable.*')
    staging_df = flatten_struct_columns(staging_df)

    text_fields_list = ('Category_Text',)
    for f in text_fields_list:
        if has_column(staging_df, f):
            f_type = get_dtype(staging_df, f)
            if f_type[:6] == 'struct':
                staging_df = staging_df.withColumn(f, col(f + ".valueField"))
            elif f_type[:12] == 'array<struct':
                staging_df = staging_df.withColumn(f, col(f)[0]['valueField'])
            elif f_type[:5] == 'array':
                staging_df = staging_df.withColumn(f, col(f)[0])

    staging_df = flatten_struct_columns(staging_df)

    staging_df = clean_html_tags(staging_df, ('Category_Text',))

    staging_df = remove_array_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def efm_questionnaire_qmatrix_topic_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("efm_questionnaire_qmatrix_topic_raw_data_transform step")

    staging_df = data_df.select('QuestionnairePage.*')
    questionnaire_type = get_dtype(staging_df, "QuestionnairePage")
    if questionnaire_type[:5] == 'array':
        staging_df = staging_df.withColumn('exploded', explode('QuestionnairePage')) \
            .select('exploded.*')
    elif questionnaire_type[:6] == 'struct':
        staging_df = staging_df.select('QuestionnairePage.*')
    else:
        return -1

    try:
        staging_df = staging_df.na.drop(subset=["QMatrix"])
    except Exception as err:
        print(err)
        return -1
    col_type = get_dtype(staging_df, "QMatrix")
    if staging_df.count() == 0 or (col_type[:5] != 'array' and col_type[:6] != 'struct'):
        return -1
    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('QMatrix', explode('QMatrix'))
    staging_df = staging_df.select('QMatrix.*').withColumnRenamed('_dbheading', 'QuestionnaireQMatrixDBHeading')

    try:
        staging_df = staging_df.na.drop(subset=["TopicList"])
    except Exception as err:
        print(err)
        return -1
    col_type = get_dtype(staging_df, "TopicList")
    if staging_df.count() == 0 or col_type[:6] != 'struct':
        return -1
    staging_df = staging_df.select('QuestionnaireQMatrixDBHeading', 'TopicList.*')

    try:
        staging_df = staging_df.na.drop(subset=["Topic"])
    except Exception as err:
        print(err)
        return -1
    col_type = get_dtype(staging_df, "Topic")
    if staging_df.count() == 0 or (col_type[:5] != 'array' and col_type[:6] != 'struct'):
        return -1
    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('Topic', explode('Topic'))
    staging_df = staging_df.select('QuestionnaireQMatrixDBHeading', 'Topic.*')
    staging_df = staging_df.drop('OptImage')
    staging_df = flatten_struct_columns(staging_df)

    text_fields_list = ('Label_Text',)
    for f in text_fields_list:
        if has_column(staging_df, f):
            f_type = get_dtype(staging_df, f)
            if f_type[:6] == 'struct':
                staging_df = staging_df.withColumn(f, col(f + ".valueField"))
            elif f_type[:12] == 'array<struct':
                staging_df = staging_df.withColumn(f, col(f)[0]['valueField'])
            elif f_type[:5] == 'array':
                staging_df = staging_df.withColumn(f, col(f)[0])

    staging_df = flatten_struct_columns(staging_df)

    staging_df = clean_html_tags(staging_df, ('Label_Text',))

    staging_df = remove_array_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def efm_survey_auth_count_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("efm_survey_auth_count_raw_data_transform step")

    for source_col in source_partition:
        staging_df = data_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def efm_survey_response_count_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("efm_survey_response_count_raw_data_transform step")

    for source_col in source_partition:
        staging_df = data_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def efm_surveyinformation_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("efm_surveyinformation_raw_data_transform step")

    staging_df = flatten_struct_columns(data_df)

    # for source_col in source_partition:
    #     staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    staging_df.printSchema()

    staging_df = staging_df.withColumnRenamed("_id", "ProjectId")

    staging_df.printSchema()

    sparksession.catalog.setCurrentDatabase(table_info['raw_db'])

    staging_df.createOrReplaceTempView('survey_info')

    print("Getting additional data dataframe!")

    additional_data_df = sparksession.sql(f"""
        select q.projectid, q.headingtext, qc.labeltext
        from efm_questionnaireqchooseone q
        inner join survey_info pr on pr.ProjectId = q.projectid
        inner join efm_questionnaireqchooseonechoice qc on qc.questionnaireqchooseonedbheading = q.dbheading 
            and qc.preselection = 1 and qc.projectid = q.projectid
        where q.headingtext IN ('EventAlphaCode', 'EventEditionCode', 'SurveyType', 'SegmentType', 'TargetLevel')
        order by q.projectid
        """)

    print("From DF to list")

    data_array = [{'projectid': row['projectid'], 'headingtext': row['headingtext'],
                   'labeltext': row['labeltext']} for row in additional_data_df.rdd.collect()]

    print("Getting counts columns!")

    staging_df = sparksession.sql(f"""
        SELECT pr.*, 
            ac.value AS authorizedparticipantcount, 
            rc.value AS responsecount
        FROM survey_info pr
        LEFT JOIN efm_survey_auth_count ac on ac.projectid = pr.ProjectId
        LEFT JOIN efm_survey_response_count rc on rc.projectid = pr.ProjectId
        """)

    staging_df.printSchema()

    # auth_count_df = sparksession.sql(f"""
    #         select * from {table_info['raw_db']}.efm_survey_auth_count
    #         """)
    #
    # staging_df = staging_df.alias('st') \
    #     .join(auth_count_df.alias('ac'),
    #           [staging_df.ProjectId == auth_count_df.projectid],
    #           how='left') \
    #     .select('st.*', col('ac.value').alias('authorizedparticipantcount'))
    #
    # response_count_df = sparksession.sql(f"""
    #         select * from {table_info['raw_db']}.efm_survey_response_count
    #         """)
    #
    # staging_df = staging_df.alias('st') \
    #     .join(response_count_df.alias('rc'),
    #           [staging_df.ProjectId == response_count_df.projectid],
    #           how='left') \
    #     .select('st.*', col('rc.value').alias('responsecount'))

    # project_tuple = tuple(project_list)

    # print("Project id tuple ----->", project_tuple)

    if len(data_array) != 0:
        columns_dict = {
            'EventAlphaCode': 1,
            'EventEditionCode': 2,
            'SurveyType': 3,
            'SegmentType': 4,
            'TargetLevel': 5
        }

        column_fields = [StructField(col, StringType(), True) for col in columns_dict]
        column_fields.insert(0, StructField('projectid', StringType(), True))
        data_schema = StructType(column_fields)

        rows_data = []
        row_project_id = data_array[0]['projectid']
        current_row = [None] * 6
        for cell in data_array:
            if cell['projectid'] != row_project_id:
                current_row[0] = row_project_id
                row = tuple(current_row)
                rows_data.append(row)
                current_row = [None] * 6
                row_project_id = cell['projectid']
            current_row[columns_dict[cell['headingtext']]] = cell['labeltext']
        current_row[0] = row_project_id
        row = tuple(current_row)
        rows_data.append(row)

        additional_df = sparksession.createDataFrame(rows_data, data_schema)

        staging_df = staging_df.withColumn("ProjectId", staging_df["ProjectId"].cast("long"))

        staging_df = staging_df.alias('st') \
            .join(additional_df.alias('ad'),
                  [staging_df.ProjectId == additional_df.projectid],
                  how='left') \
            .select('st.*', col('ad.EventAlphaCode').alias('eventalphacode'),
                    col('ad.EventEditionCode').alias('eventeditioncode'),
                    col('ad.SurveyType').alias('surveytype'),
                    col('ad.SegmentType').alias('segmenttype'),
                    col('ad.TargetLevel').alias('targetlevel'))

    # staging_df.show()

    return staging_df


def efm_surveyparticipant_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("efm_surveyparticipant_raw_data_transform step")

    staging_df = flatten_struct_columns(data_df)

    staging_df = staging_df.withColumnRenamed("_recordid", "ParticipantID")

    project_id = None
    for source_col in source_partition:
        if source_col['title'] == 'ProjectId':
            project_id = source_col['value']
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    staging_df = staging_df.withColumn('deleteFlag', lit('N'))

    # print("getting old data ...")
    # old_data_df = sparksession.sql(f"""
    #     SELECT * FROM {table_info['raw_db']}.efm_surveyparticipant
    #     WHERE projectid = {project_id} and deleteflag = 'N'
    # """)
    # old_data_df = old_data_df.select([col(c) for c in old_data_df.columns if '_hoodie' not in c])
    #
    # # old_data_df.show()
    #
    # if old_data_df.count() != 0:
    #     print("deleteflag update ...")
    #     # old_data_df = old_data_df.withColumn('deleteflag', lit('Y'))
    #     old_data_df = old_data_df.withColumn("deleteflag",
    #                                          when(old_data_df.deleteflag != "Y", "Y").otherwise(old_data_df.deleteflag))
    #     old_data_df = columns_order_by_schema(old_data_df, table_info['schema_path'])
    #     hudi_write_s3(old_data_df, table_info['raw_path'], table_info['partition_key'], table_info['raw_db'],
    #                   'efm_surveyparticipant', table_info['primary_key'], "upsert", "append")

    final_df = staging_df
    return final_df


def efm_surveyparticipant_test_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("efm_surveyparticipant_test_raw_data_transform step")

    staging_df = flatten_struct_columns(data_df)

    staging_df = staging_df.withColumnRenamed("_recordid", "ParticipantID")

    project_id = None
    for source_col in source_partition:
        if source_col['title'] == 'ProjectId':
            project_id = source_col['value']
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    staging_df = staging_df.withColumn('deleteFlag', lit('N'))

    staging_df.createOrReplaceTempView("new_participants_data")

    # Perform the SQL query with the join operation
    staging_df = sparksession.sql(f"""
        SELECT wg.name as bu, npd.*
        FROM new_participants_data npd
        LEFT JOIN {table_info['raw_db']}.efm_user_information ui ON npd.ProjectId = ui.projectid
        LEFT JOIN {table_info['raw_db']}.efm_workgroups wg ON ui.group = wg.id
    """)

    staging_df.printSchema()

    # print("getting old data ...")
    # old_data_df = sparksession.sql(f"""
    #     SELECT * FROM {table_info['raw_db']}.efm_surveyparticipant_test
    #     WHERE projectid = {project_id} and deleteflag = 'N'
    # """)
    # old_data_df = old_data_df.select([col(c) for c in old_data_df.columns if '_hoodie' not in c])
    #
    # # old_data_df.show()
    #
    # if old_data_df.count() != 0:
    #     print("deleteflag update ...")
    #     # old_data_df = old_data_df.withColumn('deleteflag', lit('Y'))
    #     old_data_df = old_data_df.withColumn("deleteflag",
    #                                          when(old_data_df.deleteflag != "Y", "Y").otherwise(old_data_df.deleteflag))
    #     old_data_df = columns_order_by_schema(old_data_df, table_info['schema_path'])
    #     hudi_write_s3(old_data_df, table_info['raw_path'], table_info['partition_key'], table_info['raw_db'],
    #                   'efm_surveyparticipant_test', table_info['primary_key'], "upsert", "append")

    final_df = staging_df
    return final_df


def efm_surveyresponse_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("efm_surveyresponse_raw_data_transform step")

    staging_df = flatten_struct_columns(data_df)

    if not has_column(staging_df, 'responseid'):
        return -1

    distinct_responses = [record.responseid for record in data_df.select('responseid').distinct().collect()]

    project_id = None
    for source_col in source_partition:
        if source_col['title'] == 'ProjectId':
            project_id = source_col['value']
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    staging_df = staging_df.withColumn('deleteflag', lit('N'))

    print("getting old data ...")
    old_data_df = sparksession.sql(f"""
            SELECT * FROM {table_info['raw_db']}.efm_surveyresponse
            WHERE projectid = {project_id} and 
                responseid IN ({", ".join((str(id) for id in distinct_responses))}) and deleteflag = 'N'
        """)
    old_data_df = old_data_df.select([col(c) for c in old_data_df.columns if '_hoodie' not in c])

    if old_data_df.count() != 0:
        print("deleteflag update ...")
        # old_data_df = old_data_df.withColumn('deleteflag', lit('Y'))
        old_data_df = old_data_df.withColumn("deleteflag",
                                             when(old_data_df.deleteflag != "Y", "Y").otherwise(old_data_df.deleteflag))
        old_data_df = columns_order_by_schema(old_data_df, table_info['schema_path'])
        hudi_write_s3(old_data_df, table_info['raw_path'], table_info['partition_key'], table_info['raw_db'],
                      'efm_surveyresponse', table_info['primary_key'], "upsert", "append")

    final_df = staging_df
    return final_df


def efm_surveyresponseheader_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("efm_surveyresponseheader_raw_data_transform step")

    staging_df = flatten_struct_columns(data_df)

    fields_list = ('responseid', 'projectid', 'started', 'completed', 'branched_out', 'over_quota', 'modified',
                   'campaign_status', 'culture', 'http_referer', 'http_user_agent', 'remote_addr', 'remote_host',
                   'last_page', 'fi_loaded', 'last_page_number', 'modifier', 'email')

    fields_dict = {
        'started': 2, # 26/04/2013 10:26:46
        'completed': 3,
        'branched_out': 4,
        'over_quota': 5,
        'modified': 6,
        'campaign_status': 7,
        'culture': 8,
        'http_referer': 9,
        'http_user_agent': 10,
        'remote_addr': 11,
        'remote_host': 12,
        'last_page': 13,
        'fi_loaded': 14,
        'last_page_number': 15,
        'modifier': 16,
        'email': 17
    }

    column_fields = [StructField(title, StringType(), True) for title in fields_list]
    print("creating the schema")
    data_schema = StructType(column_fields)

    data_list = [{'Code': row['Code'], 'ProjectId': row['ProjectId'], 'ResponseId': row['ResponseId'],
                  'Value': row['Value']} for row in staging_df.rdd.collect()]

    if len(data_list) == 0:
        return -1

    rows_data = []
    response_id = data_list[0]['ResponseId']
    current_row = [None] * 18
    current_row[0] = data_list[0]['ResponseId']
    current_row[1] = data_list[0]['ProjectId']
    for cell in data_list:
        if cell['ResponseId'] != response_id:
            row = tuple(current_row)
            rows_data.append(row)
            response_id = cell['ResponseId']
            current_row = [None] * 18
            current_row[0] = cell['ResponseId']
            current_row[1] = cell['ProjectId']
        if cell['Code'] in fields_dict:
            current_row[fields_dict[cell['Code']]] = cell['Value']
    row = tuple(current_row)
    rows_data.append(row)

    final_df = sparksession.createDataFrame(rows_data, data_schema)

    return final_df


def efm_user_information_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("efm_user_information_raw_data_transform step")

    staging_df = flatten_struct_columns(data_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def efm_workgroups_raw_data_transform(sparksession, data_df, source_partition, table_info):
    print("efm_workgroups_raw_data_transform step")

    staging_df = flatten_struct_columns(data_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def clean_html_tags(data_df, fields):
    f_list = []
    for f in fields:
        if has_column(data_df, f):
            f_list.append(f)
            data_df = data_df.withColumn(f, striphtml_udf(f))
            data_df = data_df.withColumn(f, efm_clean_white_spaces_udf(f))
    if len(f_list) > 0:
        data_df.select(*f_list).show(10, False)
    return data_df


def striphtml(string_data):
    if string_data is None or string_data == '':
        return None
    p = re.compile(r'<.*?>')
    clean_data = p.sub('', string_data)
    clean_data = re.sub(r'&nbsp;', ' ', clean_data)
    return re.sub(r'&amp;', '&', clean_data)


striphtml_udf = udf(lambda string_data: striphtml(string_data), StringType())


def efm_clean_white_spaces(string_data):
    if string_data is None or string_data == '':
        return None
    string_data = re.sub('\s+', ' ', string_data)
    string_data = string_data.strip()
    return re.sub(' +', ' ', string_data)


efm_clean_white_spaces_udf = udf(lambda string_data: efm_clean_white_spaces(string_data), StringType())
