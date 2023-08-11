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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from etl_job_modules.s3_functions import has_column, flatten_struct_columns, get_dtype


def raw_data_transform(sparksession, data_df, glue_table_name, source_partition):

    print("raw_data_transform step")

    options = {
        'smartsheet_regtracker': smartsheets_regtracker_data_transform,
        'smartsheet_2021regtracker': smartsheets_regtracker_data_transform,
        'smartsheet_exhibitorenquirysfdcintegrationstatus': smartsheets_raw_data_transform,
        'smartsheet_showpostponementsandcancellations': smartsheets_raw_data_transform,
        'smartsheet_ukmatchmakingstats': smartsheets_raw_data_transform,
        'smartsheet_virtualeventsfdcintegration': smartsheets_raw_data_transform,
        'smartsheet_webinarsfdcintegrationstatus': smartsheets_raw_data_transform,
        'smartsheet_2021marketingtechnologyoverview': smartsheets_raw_data_transform,
        '123form_fields': forms_fields_raw_data_transform,
        '123form_forms': forms_forms_raw_data_transform,
        '123form_submissions': forms_submissions_raw_data_transform,
        'brighttalk_channelsubscribers': bright_talk_channel_sub_raw_data_transform,
        'brighttalk_channels': bright_talk_channels_raw_data_transform,
        'brighttalk_subscriberswebcastactivity': bright_talk_sub_web_activity_raw_data_transform,
        'brighttalk_surveyresponses': bright_talk_survey_resp_raw_data_transform,
        'brighttalk_surveys': bright_talk_surveys_raw_data_transform,
        'brighttalk_viewingreport': bright_talk_view_reports_raw_data_transform,
        'brighttalk_viewingreportfeedbacks': bright_talk_view_reports_raw_data_transform,
        'brighttalk_viewingreportquestions': bright_talk_view_reports_raw_data_transform,
        'brighttalk_webcastregistration': bright_talk_webcast_reg_raw_data_transform,
        'brighttalk_webcastviews': bright_talk_webcast_view_raw_data_transform,
        'brighttalk_webcast': bright_talk_webcasts_raw_data_transform,
        'certain_appointmentpreferences': certain_app_pref_raw_data_transform,
        'certain_appointments': certain_appointments_raw_data_transform,
        'certain_registration': certain_registration_raw_data_transform,
        'certain_registration_response': certain_registration_resp_raw_data_transform,
        'certain_registration_agenda': certain_reg_agenda_raw_data_transform,
        'certain_event': certain_event_raw_data_transform,
        'certain_agendaitem': certain_agenda_item_raw_data_transform,
        'certain_profile': certain_profile_raw_data_transform,
        'sector_activities': sector_default_raw_data_transform,
        'sector_bookings': sector_default_raw_data_transform,
        'sector_commslogs': sector_comms_logs_raw_data_transform,
        'sector_companies': sector_companies_raw_data_transform,
        'sector_companyattributes': sector_companyattributes_raw_data_transform,
        'sector_conference': sector_default_raw_data_transform,
        'sector_contacts': sector_contacts_raw_data_transform,
        'sector_contactattributes': sector_contact_attributes_raw_data_transform,
        'sector_contactattributevalues': sector_contact_attribute_values_raw_data_transform,
        'sector_contactproducts': sector_contact_products_raw_data_transform,
        'sector_contactpayments': sector_contact_payments_raw_data_transform,
        'sector_contactpurchases': sector_contact_purchases_raw_data_transform,
        'sector_dpa': sector_default_raw_data_transform,
        'sector_dropinmeetingrooms': sector_drop_in_raw_data_transform,
        'sector_dropinmeetingrooms_meetings': sector_drop_in_meetings_raw_data_transform,
        'sector_invites': sector_default_raw_data_transform,
        'sector_meetings': sector_meetings_raw_data_transform,
        'sector_meetingratings': sector_default_raw_data_transform,
        'sector_attendance': sector_default_raw_data_transform,
        'sector_favourites': sector_default_raw_data_transform,
        'sector_favouritables': sector_default_raw_data_transform,
        'sector_products': sector_default_raw_data_transform,
        'sector_questions': sector_questions_raw_data_transform,
        'sector_answers': sector_answers_raw_data_transform,
        'sector_registrations': sector_registrations_raw_data_transform,
        'sector_registration_product_interests': sector_registration_pr_interest_raw_data_transform,
        'sector_searchhistory': sector_default_raw_data_transform,
        'obergine_entrant': obergine_default_raw_data_transform,
        'obergine_entrantwineupload': obergine_default_raw_data_transform,
        'obergine_noandlowawardsentrant': obergine_default_raw_data_transform,
        'obergine_noandlowawardsentry': obergine_default_raw_data_transform,
        'obergine_subscriber': obergine_default_raw_data_transform,
        'ga_googleanalyticsdata': ga_googleanalyticsdata_raw_data_transform
    }

    return options[glue_table_name](sparksession, data_df, source_partition)


def smartsheets_raw_data_transform(sparksession, data_df, source_partition):

    print("smartsheets_raw_data_transform step")

    print("getting columns dataframe")
    columns_df = data_df.withColumn('exploded', explode('columns')).select('exploded.*')

    # columns_data = [{'title': col['title'], 'type': col['type']} for col in columns_df.rdd.collect()]
    # need to think about column types

    print("getting fields from columns")
    column_fields = [StructField(col['title'], StringType(), True) for col in columns_df.rdd.collect()]
    column_fields.append(StructField('id', StringType(), True))
    print("creating the schema")
    data_schema = StructType(column_fields)

    print("getting rows dataframe")
    rows_df = data_df.withColumn('exploded', explode('rows')).select('exploded.*').withColumn('exploded', explode(
        'cells')).select('id', 'rowNumber', 'exploded.*')

    print("getting cells")
    cells_data = [{'id': row['id'], 'rowNumber': row['rowNumber'],
                   'columnId': row['columnId'], 'value': row['value']} for row in rows_df.rdd.collect()]

    print("converting cells")
    rows_data = []
    row_number = cells_data[0]['rowNumber']
    row_id = cells_data[0]['id']
    current_row = []
    for cell in cells_data:
        if cell['rowNumber'] != row_number:
            current_row.append(row_id)
            row = tuple(current_row)
            rows_data.append(row)
            current_row = []
            row_number = cell['rowNumber']
            row_id = cell['id']
        current_row.append(cell['value'])
    current_row.append(row_id)
    row = tuple(current_row)
    rows_data.append(row)

    print("creating final dataframe")
    final_df = sparksession.createDataFrame(rows_data, data_schema)

    for source_c in source_partition:
        final_df = final_df.withColumn(source_c['title'], lit(source_c['value']))

    return final_df


def smartsheets_regtracker_data_transform(sparksession, data_df, source_partition):
    data_df = smartsheets_raw_data_transform(sparksession, data_df, source_partition)

    data_df = data_df.withColumnRenamed('id', 'rowid')

    return data_df


def forms_fields_raw_data_transform(sparksession, data_df, source_partition):
    print("forms_fields_raw_data_transform step")

    columns = (
        ('FormId', 'form_id'),
        ('FieldId', 'id'),
        ('FieldTitle', 'name'),
        ('FieldInstructions', 'instruction'),
        ('FieldType', 'type'),
        ('FieldValues', 'values'),
        ('FieldRequired', 'required')
    )

    final_df = data_df.select('data.controls.data').withColumn('exploded', explode('data')).select('exploded.*')

    for source_col in source_partition:
        final_df = final_df.withColumn(source_col['title'], lit(source_col['value']))

    for r_col in columns:
        final_df = final_df.withColumnRenamed(r_col[1], r_col[0])

    return final_df


def forms_forms_raw_data_transform(sparksession, data_df, source_partition):
    print("forms_forms_raw_data_transform step")

    drop_list = [
        'permission_granted_users', 'accessible_by_users', 'form_sharing_options',
        'form_sharing_custom_options', 'latest_submission'
    ]
    columns = (
        ('FormId', 'id'),
        ('FormName', 'name'),
        ('FormSubmissionsTotal', 'submissions_count')
    )

    staging_df = data_df.withColumn('exploded', explode('data')).select('exploded.*')

    col_type = get_dtype(staging_df, "accessible_by_users")

    if col_type[:5] == 'array':
        staging_df = staging_df.withColumn('formemails', col('accessible_by_users')[0]['email'])
    elif col_type[:6] == 'struct':
        staging_df = staging_df.withColumn('formemails', 'accessible_by_users.0.email')
    elif col_type[:6] == 'string':
        to_user_email = udf(lambda str_data: get_user_email(str_data), StringType())
        staging_df = staging_df.withColumn('formemails', to_user_email('accessible_by_users'))

    staging_df = staging_df.drop(*drop_list)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    for r_col in columns:
        staging_df = staging_df.withColumnRenamed(r_col[1], r_col[0])

    return staging_df


def get_user_email(str_data):
    try:
        accessible_by = json.loads(str_data)
        if type(accessible_by) is list:
            return accessible_by[0]['email']
        elif type(accessible_by) is dict:
            return accessible_by['0']['email']
        return None
    except Exception as err:
        return None


def forms_submissions_raw_data_transform(sparksession, data_df, source_partition):
    print("forms_submissions_raw_data_transform step")

    columns = (
        ('FormId', 'form_id'),
        ('xmlid', 'id'),
        ('Date', 'date'),
        ('Datestart', 'datestart'),
        ('IP', 'ip'),
        ('CC', 'cc'),
        ('RefId', 'refid'),
        ('PaymSum', 'paymsum'),
        ('PaymSel', 'paymsel'),
        ('PaymType', 'paymtype'),
        ('PaymCur', 'paymcur'),
        ('CouponCode', 'couponcode'),
        ('Browser', 'browser'),
        ('QuizScore', 'quizScore'),
        ('Lang', 'lang'),
        ('Referer', 'referer'),
        ('FormHost', 'formhost'),
        ('UserAgent', 'useragent'),
        # ('Approved'),
        # ('PaymDone'),
        ('FieldValue', 'fieldvalue'),
        ('FieldId', 'fieldid')
    )

    staging_df = data_df.withColumn('exploded', explode('data')).select('exploded.*').drop('date') \
        .select('*', 'content.*').drop('content').withColumn('exploded', explode('fields.field'))\
        .select('*', 'exploded.*').drop('exploded', 'fields')

    staging_df = staging_df.withColumn('fieldvalue', when(col('fieldvalue') == "{}", lit('')).otherwise(col('fieldvalue')))

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    for r_col in columns:
        staging_df = staging_df.withColumnRenamed(r_col[1], r_col[0])

    final_df = staging_df
    return final_df


def bright_talk_channel_sub_raw_data_transform(sparksession, data_df, source_partition):
    print("bright_talk_channel_sub_raw_data_transform step")
    # row_tag - channelSubscriber
    
    columns = (
        ('embedurl', 'embed_url'),
        ('userid', 'user_id'),
        ('userfirstName', 'firstName'),
        ('userlastName', 'lastName'),
        ('useremail', 'email'),
        ('usertimeZone', 'timeZone'),
        ('userphone', 'phone'),
        ('userjobTitle', 'jobTitle'),
        ('userlevel', 'level'),
        ('usercompanyName', 'companyName'),
        ('usercompanySize', 'companySize'),
        ('userindustry', 'industry'),
        ('usercountry', 'country'),
        ('linkhref', 'link_href'),
        ('userstateProvince', 'stateProvince')
    )

    staging_df = data_df.withColumnRenamed('_id', 'id').select('*', 'user.*').withColumnRenamed('_id', 'user_id')\
        .withColumn('embed_url', data_df['embed.url']) \
        .withColumn('link_href', data_df['link._href']).withColumn('link_rel', data_df['link._rel']) \
        .drop('user', 'embed', 'link')

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    for r_col in columns:
        staging_df = staging_df.withColumnRenamed(r_col[1], r_col[0])

    final_df = staging_df
    return final_df


def bright_talk_channels_raw_data_transform(sparksession, data_df, source_partition):
    print("bright_talk_channels_raw_data_transform step")
    print("THIS TABLE IS STATIC")
    # row_tag - channel
    
    # columns = ()

    staging_df = data_df.withColumnRenamed('_id', 'id') \
        .withColumn('link0_href', data_df['link'].getItem(0)['_href']).withColumn('link0_rel', data_df['link'].getItem(0)['_rel']) \
        .withColumn('link1_href', data_df['link'].getItem(1)['_href']).withColumn('link1_rel', data_df['link'].getItem(1)['_rel']) \
        .withColumn('link2_href', data_df['link'].getItem(2)['_href']).withColumn('link2_rel', data_df['link'].getItem(2)['_rel']) \
        .drop('link')

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def bright_talk_sub_web_activity_raw_data_transform(sparksession, data_df, source_partition):
    print("bright_talk_sub_web_activity_raw_data_transform step")
    # row_tag - subscriberWebcastActivity
    
    columns = (
        ('userfirstName', 'firstName'),
        ('userlastName', 'lastName'),
        ('useremail', 'email'),
        ('usertimeZone', 'timeZone'),
        ('userphone', 'phone'),
        ('userjobTitle', 'jobTitle'),
        ('userlevel', 'level'),
        ('usercompanyName', 'companyName'),
        ('usercompanySize', 'companySize'),
        ('userindustry', 'industry'),
        ('usercountry', 'country'),
        ('userstateprovince', 'stateProvince'),
        ('link0href', 'link0_href'),
        ('link1href', 'link1_href')
    )

    staging_df = data_df.withColumnRenamed('_id', 'id').withColumn('webcastid', data_df['webcast._id']) \
        .select('*', 'user.*').withColumnRenamed('_id', 'userid')\
        .withColumn('surveyresponseid', data_df['surveyResponse._id']) \
        .withColumn('link0_href', data_df['link'].getItem(0)['_href']).withColumn('link0_rel', data_df['link'].getItem(0)['_rel']) \
        .withColumn('link1_href', data_df['link'].getItem(1)['_href']).withColumn('link1_rel', data_df['link'].getItem(1)['_rel']) \
        .drop('user', 'webcast', 'surveyResponse', 'link')

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    for r_col in columns:
        staging_df = staging_df.withColumnRenamed(r_col[1], r_col[0])

    final_df = staging_df
    return final_df


def bright_talk_survey_resp_raw_data_transform(sparksession, data_df, source_partition):
    print("bright_talk_survey_resp_raw_data_transform step")
    # row_tag - surveyResponse
    
    columns = (
        ('channel_id', 'channelId'),
        ('user_firstname', 'firstName'),
        ('user_lastname', 'lastName'),
        ('user_email', 'email'),
        ('user_timeZone', 'timeZone'),
        ('user_phone', 'phone'),
        ('user_jobTitle', 'jobTitle'),
        ('user_level', 'level'),
        ('user_companyName', 'companyName'),
        ('user_companySize', 'companySize'),
        ('user_industry', 'industry'),
        ('user_country', 'country'),
        ('user_stateProvince', 'stateProvince')
    )

    staging_df = data_df.withColumnRenamed('_id', 'id').withColumn('survey_id', data_df['survey._id']) \
        .select('*', 'user.*').withColumnRenamed('_id', 'user_id') \
        .withColumn('link0_href', data_df['link'].getItem(0)['_href']).withColumn('link0_rel', data_df['link'].getItem(0)['_rel']) \
        .withColumn('link1_href', data_df['link'].getItem(1)['_href']).withColumn('link1_rel', data_df['link'].getItem(1)['_rel']) \
        .drop('user', 'survey', 'link') \
        .withColumn('exploded', explode('questions.question')).select('*', 'exploded.*').drop('questions', 'exploded') \
        .withColumnRenamed('_reportingId', 'question_reportingId').withColumnRenamed('_id', 'question_id') \
        .withColumn('answer', explode('answers.answer')).drop('answers')

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    for r_col in columns:
        staging_df = staging_df.withColumnRenamed(r_col[1], r_col[0])

    final_df = staging_df
    return final_df


def bright_talk_surveys_raw_data_transform(sparksession, data_df, source_partition):
    print("bright_talk_surveys_raw_data_transform step")
    # row_tag - survey

    staging_df = data_df.withColumnRenamed('_id', 'id').withColumn('exploded', explode('question')) \
        .select('*', 'exploded.*').withColumnRenamed('_id', 'question_id').withColumnRenamed('_type', 'question_type') \
        .withColumnRenamed('_reportingId', 'question_reportingId').withColumnRenamed('_deleted', 'question_deleted') \
        .withColumnRenamed('_required', 'question_required').withColumnRenamed('text', 'question_text') \
        .withColumn('link0_href', data_df['link'].getItem(0)['_href']).withColumn('link0_rel', data_df['link'].getItem(0)['_rel']) \
        .withColumn('link1_href', data_df['link'].getItem(1)['_href']).withColumn('link1_rel', data_df['link'].getItem(1)['_rel']) \
        .drop('question', 'options', 'exploded', 'link')
    # need to test

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def bright_talk_view_reports_raw_data_transform(sparksession, data_df, source_partition):
    print("bright_talk_view_reports_raw_data_transform step")
    # row_tag - view

    staging_df = data_df.withColumnRenamed('_id', 'id')\
        .select('*', 'user.*').withColumnRenamed('_id', 'userid').drop('user', 'feedback', 'questions')
    # need to test

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    return staging_df


def bright_talk_view_report_feedbacks_raw_data_transform(sparksession, data_df, source_partition):
    print("bright_talk_view_report_feedbacks_raw_data_transform step")
    # row_tag - view

    staging_df = data_df.withColumnRenamed('_id', 'ViewingReportId') \
        .select('*', 'feedback.*').withColumnRenamed('_id', 'id').drop('user', 'feedback', 'questions')
    # need to test

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    return staging_df


def bright_talk_view_report_questions_raw_data_transform(sparksession, data_df, source_partition):
    print("bright_talk_view_report_questions_raw_data_transform step")
    # row_tag - view

    staging_df = data_df.withColumnRenamed('_id', 'ViewingReportId') \
        .withColumn('question', explode('questions.question')).select('*', 'question.*') \
        .withColumnRenamed('_id', 'id').drop('user', 'questions', 'feedback')
    # need to check questions
    # need to test

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    return staging_df


def bright_talk_webcast_reg_raw_data_transform(sparksession, data_df, source_partition):
    print("bright_talk_webcast_reg_raw_data_transform step")
    # row_tag - webcastRegistration
    
    columns = (
        ('userfirstName', 'firstName'),
        ('userlastName', 'lastName'),
        ('useremail', 'email'),
        ('usertimeZone', 'timeZone'),
        ('userphone', 'phone'),
        ('userjobTitle', 'jobTitle'),
        ('userlevel', 'level'),
        ('usercompanyName', 'companyName'),
        ('usercompanySize', 'companySize'),
        ('userindustry', 'industry'),
        ('usercountry', 'country'),
        ('userstateProvince', 'stateProvince')
    )

    staging_df = data_df.withColumnRenamed('_id', 'id').withColumn('webcast_id', data_df['webcast._id']) \
        .withColumn('embedurl', data_df['embed.url']) \
        .withColumn('link0href', data_df['link'].getItem(0)['_href']).withColumn('link0_rel', data_df['link'].getItem(0)['_rel']) \
        .withColumn('link1href', data_df['link'].getItem(1)['_href']).withColumn('link1_rel', data_df['link'].getItem(1)['_rel']) \
        .select('*', 'user.*').withColumnRenamed('_id', 'user_id').drop('user', 'webcast', 'embed', 'link')
    # need to test

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    for r_col in columns:
        staging_df = staging_df.withColumnRenamed(r_col[1], r_col[0])

    final_df = staging_df
    return final_df


def bright_talk_webcast_view_raw_data_transform(sparksession, data_df, source_partition):
    print("bright_talk_webcast_view_raw_data_transform step")
    # row_tag - webcastViewing
    
    columns = (
        ('userfirstName', 'firstName'),
        ('userlastName', 'lastName'),
        ('useremail', 'email'),
        ('usertimeZone', 'timeZone'),
        ('userphone', 'phone'),
        ('userjobTitle', 'jobTitle'),
        ('userlevel', 'level'),
        ('usercompanyName', 'companyName'),
        ('usercompanySize', 'companySize'),
        ('userindustry', 'industry'),
        ('usercountry', 'country'),
        ('userstateProvince', 'stateProvince')
    )

    staging_df = data_df.withColumnRenamed('_id', 'id').withColumn('webcast_id', data_df['webcast._id']) \
        .withColumn('embedurl', data_df['embed.url']) \
        .withColumn('link0href', data_df['link'].getItem(0)['_href']).withColumn('link0rel', data_df['link'].getItem(0)['_rel']) \
        .withColumn('link1href', data_df['link'].getItem(1)['_href']).withColumn('link1rel', data_df['link'].getItem(1)['_rel']) \
        .select('*', 'user.*').withColumnRenamed('_id', 'user_id').drop('user', 'webcast', 'embed', 'link')
    # need to test

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    for r_col in columns:
        staging_df = staging_df.withColumnRenamed(r_col[1], r_col[0])

    final_df = staging_df
    return final_df


def bright_talk_webcasts_raw_data_transform(sparksession, data_df, source_partition):
    print("bright_talk_webcasts_raw_data_transform step")
    # row_tag - webcast

    columns = (
        ('ChannelId', 'channelId'),
        ('Active', 'active'),
        ('Created', 'created'),
        ('Description', 'description'),
        ('Duration', 'duration'),
        ('Keywords', 'keywords'),
        ('LastUpdated', 'lastUpdated'),
        ('Presenter', 'presenter'),
        ('Published', 'published'),
        ('Start', 'start'),
        ('Status', 'status'),
        ('Title', 'title'),
        ('Url', 'url'),
        ('Visbility', 'visibility')
    )

    staging_df = data_df.withColumnRenamed('_id', 'Id') \
        .withColumn('Link0href', data_df['link'].getItem(0)['_href']).withColumn('link0_rel', data_df['link'].getItem(0)['_rel']) \
        .withColumn('Link1href', data_df['link'].getItem(1)['_href']).withColumn('link1_rel', data_df['link'].getItem(1)['_rel']) \
        .withColumn('Link2href', data_df['link'].getItem(2)['_href']).withColumn('link2_rel', data_df['link'].getItem(2)['_rel']) \
        .withColumn('Link3href', data_df['link'].getItem(3)['_href']).withColumn('link3_rel', data_df['link'].getItem(3)['_rel']) \
        .drop('link')
    # need to test

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    for r_col in columns:
        staging_df = staging_df.withColumnRenamed(r_col[1], r_col[0])

    final_df = staging_df
    return final_df


def certain_app_pref_raw_data_transform(sparksession, data_df, source_partition):
    print("certain_app_pref_raw_data_transform step")

    staging_df = flatten_struct_columns(data_df)
    staging_df = flatten_struct_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def certain_appointments_raw_data_transform(sparksession, data_df, source_partition):
    print("certain_appointments_raw_data_transform step")

    staging_df = data_df.withColumn('exploded', explode('appointments')).select('exploded.*')
    staging_df = flatten_struct_columns(staging_df)
    staging_df = staging_df.withColumn('appointmentRating', staging_df['appointmentRating'][0]) \
        .withColumn('calendarName', staging_df['calendar'][0]['calendarName']).drop('calendar') \
        .withColumn('registration_calendarList', staging_df['registration_calendarList'][0]) \
        .withColumn('targetRegistration_calendarList', staging_df['targetRegistration_calendarList'][0])
    staging_df = flatten_struct_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def certain_registration_raw_data_transform(sparksession, data_df, source_partition):
    print("certain_registration_raw_data_transform step")

    reg_columns = (
        ('Addressline1', 'line1'),
        ('Addressline2', 'line2'),
        ('Addressline3', 'line3'),
        ('Addressline4', 'line4')
    )

    staging_df = data_df.withColumn('exploded', explode('registrations')).select('exploded.*')\
        .select('*', 'profile.*').drop('profile').select('*', 'address.*')\
        .drop('address', 'registrationQuestions', 'eventCode')

    # if get_dtype(registration_df, 'altAddress')[:6] == 'struct':
    # just in case we left some nested fields
    staging_df = flatten_struct_columns(staging_df)
    staging_df = staging_df.drop('altAddress')

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    for r_col in reg_columns:
        staging_df = staging_df.withColumnRenamed(r_col[1], r_col[0])

    return staging_df


def certain_registration_resp_raw_data_transform(sparksession, data_df, source_partition):
    print("certain_registration_resp_raw_data_transform step")

    staging_df = data_df.withColumn('exploded', explode('registrations')).select('exploded.*')\
        .withColumn('exploded', explode('registrationQuestions.question'))

    if get_dtype(staging_df, "exploded")[:6] != 'struct':
        return -1

    staging_df = staging_df.select('registrationCode', 'eventCode', 'dateModified', 'exploded.*')\
        .withColumn('exploded', explode('answers.answer'))\
        .select('*', 'exploded.*').drop('answers', 'exploded')
    # just in case we left some nested fields
    staging_df = flatten_struct_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    return staging_df


def certain_reg_agenda_raw_data_transform(sparksession, data_df, source_partition):
    print("certain_reg_agenda_raw_data_transform step")

    staging_df = data_df.withColumn('exploded', explode('registrationAgendas')).select('exploded.*').drop('tag')
    # just in case we left some nested fields
    staging_df = flatten_struct_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def certain_event_raw_data_transform(sparksession, data_df, source_partition):
    print("certain_event_raw_data_transform step")

    re_columns = (
        ('contact_contactName', 'contactName'),
        ('location_locationName', 'locationName'),
        ('location_locationCode', 'locationCode'),
        ('location_locationCity', 'locationCity'),
        ('location_locationCountry', 'locationCountry')
    )

    staging_df = data_df.withColumn('exploded', explode('events')).select('exploded.*')
    staging_df = flatten_struct_columns(staging_df)
    # just in case we left some nested fields
    staging_df = flatten_struct_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    for re_col in re_columns:
        staging_df = staging_df.withColumnRenamed(re_col[1], re_col[0])

    final_df = staging_df
    return final_df


def certain_agenda_item_raw_data_transform(sparksession, data_df, source_partition):
    print("certain_agenda_item_raw_data_transform step")

    staging_df = data_df.withColumn('exploded', explode('agendaItems')).select('exploded.*').drop('eventCode')
    staging_df = flatten_struct_columns(staging_df)
    # just in case we left some nested fields
    staging_df = flatten_struct_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def certain_profile_raw_data_transform(sparksession, data_df, source_partition):
    print("certain_profile_raw_data_transform step")

    drop_list = ['profileQuestions', 'events']

    staging_df = data_df.withColumn('exploded', explode('profiles')).select('exploded.*').drop(*drop_list)
    staging_df = flatten_struct_columns(staging_df)
    # just in case we left some nested fields
    staging_df = flatten_struct_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def sector_default_raw_data_transform(sparksession, data_df, source_partition):
    print("sector_default_raw_data_transform step")

    staging_df = data_df.withColumn('exploded', explode('data')).select('exploded.*')
    staging_df = flatten_struct_columns(staging_df)
    staging_df = staging_df.withColumnRenamed('system_functions_time_taken', 'time_taken')

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def sector_meetings_raw_data_transform(sparksession, data_df, source_partition):
    print("sector_default_raw_data_transform step")

    staging_df = data_df.withColumn('exploded', explode('data')).select('exploded.*')

    if get_dtype(staging_df, "slot")[:12] == 'array<struct':
        staging_df = staging_df.withColumn('slot', explode('slot'))
        staging_df = flatten_struct_columns(staging_df)
    else:
        staging_df = staging_df.drop('slot')

    staging_df = flatten_struct_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def sector_comms_logs_raw_data_transform(sparksession, data_df, source_partition):
    print("sector_comms_logs_raw_data_transform step")

    staging_df = data_df.withColumn('exploded', explode('data'))\
        .select('exploded.*', col('exploded.tags')[0].alias('tags0'), col('exploded.tags')[1].alias('tags1')).drop('tags')
    staging_df = flatten_struct_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def sector_companies_raw_data_transform(sparksession, data_df, source_partition):
    print("sector_companies_raw_data_transform step")

    staging_df = data_df.withColumn('exploded', explode('data')).select('exploded.*').drop("attributes")
    staging_df = flatten_struct_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def sector_companyattributes_raw_data_transform(sparksession, data_df, source_partition):
    print("sector_companyattributes_raw_data_transform step")

    if get_dtype(data_df, "data")[:12] == 'array<struct':
        staging_df = data_df.withColumn('exploded', explode('data')).select('exploded.*')
    else:
        return -1

    staging_df = staging_df.withColumnRenamed('id', 'companyid').drop('name')

    if get_dtype(data_df, "data")[:12] == 'array<struct':
        staging_df = data_df.withColumn('exploded', explode('data')).select('exploded.*')
    else:
        return -1

    staging_df = staging_df.withColumnRenamed('id', 'companyid').drop('name')\
        .withColumn('attributes', explode('attributes'))\
        .select('companyid', 'instance_id', 'updated_at', 'attributes.*')

    if get_dtype(staging_df, "values")[:12] == 'array<struct':
        staging_df = staging_df.withColumn('values', explode('values')) \
            .withColumnRenamed('name', 'attribute_name').select('*', 'values.*').drop('values')\
            .withColumnRenamed('name', 'attributevalue_name').withColumnRenamed('attribute_name', 'name')
    else:
        return -1

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def sector_contacts_raw_data_transform(sparksession, data_df, source_partition):
    print("sector_contacts_raw_data_transform step")

    if get_dtype(data_df, "data")[:12] == 'array<struct':
        staging_df = data_df.withColumn('exploded', explode('data')).select('exploded.*')
    else:
        return -1

    phones_type = get_dtype(staging_df, "phones")
    print(phones_type)

    if phones_type[:12] == 'array<struct':
        print("phones - array of struct")
        try:
            staging_df = staging_df.select('*',
                                           col('phones')[0]['phonetype_code'].alias('phonetype_code'),
                                           col('phones')[0]['number'].alias('phone_number'))
        except Exception as err:
            print(err)
            staging_df = staging_df.select('*').withColumn("phonetype_code", lit("")) \
                .withColumn("phone_number", lit(""))
    else:
        print("phones - string")
        staging_df = staging_df.select('*').withColumn("phonetype_code", lit(""))\
            .withColumn("phone_number", lit(""))

    staging_df = staging_df.drop('phones', 'products', 'payments', 'purchases', 'attributes')
    staging_df = flatten_struct_columns(staging_df)

    staging_df = staging_df.filter("last_name not like '%mistake%'")

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def sector_contact_products_raw_data_transform(sparksession, data_df, source_partition):
    print("sector_contact_products_raw_data_transform step")

    if get_dtype(data_df, "data")[:12] == 'array<struct':
        staging_df = data_df.withColumn('exploded', explode('data')).select('exploded.*')
    else:
        return -1
    staging_df = staging_df.withColumnRenamed('id', 'contactid')
    if get_dtype(data_df, "products")[:12] == 'array<struct':
        staging_df = staging_df.withColumn('exploded', explode('products'))\
            .select('contactid', 'instance_id', 'updated_at', 'exploded.*').withColumnRenamed('product_id', 'id')
    else:
        return -1

    staging_df = flatten_struct_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def sector_contact_payments_raw_data_transform(sparksession, data_df, source_partition):
    print("sector_contact_payments_raw_data_transform step")

    staging_df = data_df.withColumn('exploded', explode('data')).select('exploded.*')

    # payments field can be null
    if get_dtype(staging_df, "payments")[:6] != 'struct':
        return -1

    staging_df = staging_df.select('payments.*')

    staging_df = flatten_struct_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def sector_contact_purchases_raw_data_transform(sparksession, data_df, source_partition):
    print("sector_contact_purchases_raw_data_transform step")

    staging_df = data_df.withColumn('exploded', explode('data')).select('exploded.*') \
        .withColumn('exploded', explode('purchases'))

    # purchases field can be []
    if get_dtype(staging_df, "exploded")[:6] != 'struct':
        return -1

    staging_df = staging_df.select('exploded.*')

    staging_df = flatten_struct_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def sector_contact_attributes_raw_data_transform(sparksession, data_df, source_partition):
    print("sector_contact_attributes_raw_data_transform step")

    staging_df = data_df.withColumn('exploded', explode('data')).select('exploded.*') \
        .filter("last_name not like '%mistake%'").withColumn('exploded', explode('attributes'))\
        .select(col('id').alias('ContactId'), 'instance_id', 'updated_at', 'exploded.*').drop('values')
    staging_df = flatten_struct_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def sector_contact_attribute_values_raw_data_transform(sparksession, data_df, source_partition):
    print("sector_contact_attribute_values_raw_data_transform step")

    staging_df = data_df.withColumn('exploded', explode('data')).select('exploded.*') \
        .filter("last_name not like '%mistake%'").withColumn('exploded', explode('attributes'))\
        .select(col('id').alias('contact_id'), 'instance_id', 'updated_at', 'exploded.*') \
        .withColumn('exploded', explode('values'))

    if get_dtype(staging_df, "exploded")[:6] != 'struct':
        return -1

    staging_df = staging_df.select('contact_id', 'instance_id', 'attribute_id', 'updated_at', 'exploded.*')
    staging_df = flatten_struct_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def sector_drop_in_raw_data_transform(sparksession, data_df, source_partition):
    print("sector_drop_in_raw_data_transform step")

    staging_df = data_df.withColumn('exploded', explode('data')).select('exploded.*').drop('meetings')
    staging_df = flatten_struct_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def sector_drop_in_meetings_raw_data_transform(sparksession, data_df, source_partition):
    print("sector_drop_in_meetings_raw_data_transform step")

    staging_df = data_df.withColumn('exploded', explode('data')).select('exploded.*')

    if get_dtype(staging_df, "meetings")[:12] == 'array<struct':
        staging_df = staging_df.withColumn('exploded', explode('meetings')).select('exploded.*')
    else:
        return -1

    staging_df = flatten_struct_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def sector_questions_raw_data_transform(sparksession, data_df, source_partition):
    print("sector_questions_raw_data_transform step")

    staging_df = data_df.withColumn('exploded', explode('data')).select('exploded.*').drop('answers')
    staging_df = flatten_struct_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def sector_answers_raw_data_transform(sparksession, data_df, source_partition):
    print("sector_answers_raw_data_transform step")

    staging_df = data_df.withColumn('exploded', explode('data')).select('exploded.*')

    if get_dtype(staging_df, "answers")[:12] == 'array<struct':
        staging_df = staging_df.withColumn('exploded', explode('answers')).select('exploded.*')
    else:
        return -1

    staging_df = flatten_struct_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def sector_registrations_raw_data_transform(sparksession, data_df, source_partition):
    print("sector_registrations_raw_data_transform step")

    staging_df = data_df.withColumn('exploded', explode('data')).select('exploded.*') \
        .withColumnRenamed('id', 'RegistrationId').drop('products_product_interest', 'products_global_sector')
    # check with column names conflicts
    staging_df = flatten_struct_columns(staging_df)

    staging_df = staging_df.withColumnRenamed('updated_at_date', 'updated_at')

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def sector_registration_pr_interest_raw_data_transform(sparksession, data_df, source_partition):
    print("sector_registration_pr_interest_raw_data_transform step")

    staging_df = data_df.withColumn('exploded', explode('data')).select('exploded.*') \
        .withColumn('Product', explode('products_product_interest'))\
        .select(col('id').alias('RegistrationId'), 'Product')
    staging_df = flatten_struct_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def obergine_default_raw_data_transform(sparksession, data_df, source_partition):
    print("obergine_default_raw_data_transform step")

    staging_df = data_df.withColumnRenamed('_id', 'id')
    staging_df = flatten_struct_columns(staging_df)

    for source_col in source_partition:
        staging_df = staging_df.withColumn(source_col['title'], lit(source_col['value']))

    final_df = staging_df
    return final_df


def ga_googleanalyticsdata_raw_data_transform(sparksession, data_df, source_partition):
    print("ga_googleanalyticsdata_raw_data_transform step")

    print('getting demensions_df')
    demensions_df = data_df.withColumn('exploded', explode('ColumnHeader.Dimensions')).select('exploded')
    demensions_fields = [StructField(col['exploded'][3:], StringType(), True) for col in demensions_df.rdd.collect()]

    print('getting metrics_df')
    metrics_df = data_df.withColumn('exploded', explode('ColumnHeader.MetricHeader.MetricHeaderEntries'))\
        .select('exploded.*')
    metrics_fields = [StructField(col['Name'], StringType(), True) for col in metrics_df.rdd.collect()]

    column_fields = demensions_fields + metrics_fields

    data_schema = StructType(column_fields)

    print('getting rows_df')
    rows_df = data_df.filter(col('Data.RowCount').isNotNull() & (col('Data.RowCount') > 0))
    if rows_df.count() == 0:
        return -1
    rows_df = rows_df.withColumn('exploded', explode('Data.Rows'))\
        .select('exploded.Dimensions', 'exploded.Metrics.Values').withColumn('Values', explode('Values'))

    rows_data = [tuple(field['Dimensions'] + field['Values']) for field in rows_df.rdd.collect()]

    final_df = sparksession.createDataFrame(rows_data, data_schema)

    for source_col in source_partition:
        final_df = final_df.withColumn(source_col['title'], lit(source_col['value']))

    return final_df

