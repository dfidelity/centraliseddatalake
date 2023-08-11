from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from datetime import datetime, timedelta
import sys
import json
import re
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def refined_data_transform(sparksession, source, table, data_df):

    print("refined_data_transform step")

    option = source + '_' + table

    options = {
        '123form_forms': forms_forms_refined_data_transform,
        '123form_submissions': forms_submissions_refined_data_transform,
        # 'brighttalk_channelsubscribers': bright_talk_channel_sub_refined_data_transform,
        # 'brighttalk_webcast': bright_talk_webcasts_refined_data_transform,
        'certain_registration': certain_registration_refined_data_transform,
        'sector_contactattributes': sector_contact_attributes_refined_data_transform,
        'test_table': test_table_procedure,
        # 'smartsheets_showpostponementsandcancellations': smartsheets_showpostponementsandcancellations_refined_data_transform,
    }

    if option in options:
        return options[option](sparksession, data_df)

    return -1


# def refined_dependent_tables_transform(sparksession, data_df, source, table):
#
#     print("refined_dependent_tables_transform step")
#
#     option = source + '_' + table
#
#     options = {
#         'brighttalk_webcast': bright_talk_webcasts_dependent_tables_transform
#     }
#
#     if option in options:
#         return options[option](sparksession)
#
#     return {}


def forms_forms_refined_data_transform(sparksession, data_df):
    sparksession.catalog.setCurrentDatabase("uk_refinedzone")
    # data_df = sparksession.sql("select * from 123form_forms")

    print('SETTING standenquiryflag ...')
    to_standenquiryflag_udf = udf(lambda form_name: set_forms_stand_enquiry_flag(form_name), StringType())
    staging_df = data_df.withColumn('standenquiryflag', when(col("standenquiryflag").isNull(), to_standenquiryflag_udf('formname')).otherwise(col("standenquiryflag")))

    print('GETTING data from globaldb_refined ...')
    sparksession.catalog.setCurrentDatabase("globaldb_refined")
    # print('GETTING data from globaldb_raw ...')
    # sparksession.catalog.setCurrentDatabase("globaldb_raw")
    event_df = sparksession.sql("select * from event")
    event_tl_df = sparksession.sql("select row_wid, event_name from event_tl")

    # raise Exception("Test run ...")

    code_list = [row['event_alpha_code'] for row in event_df.collect()]

    print('SETTING eventalphacode ...')
    to_eventalphacode_udf = udf(lambda form_name: set_event_alpha_code(form_name, code_list), StringType())
    staging_df = staging_df.withColumn("eventalphacode", when(col("eventalphacode").isNull(), to_eventalphacode_udf('formname')).otherwise(col("eventalphacode")))

    print('GETTING data from uk_metadb ...')
    sparksession.catalog.setCurrentDatabase("uk_metadb")
    forms_sfdc_virtual_campaigns = sparksession.sql("select formid from dbo_123formssfdcvirtualcampaigns")

    print('GETTING data from uk_refinedzone ...')
    sparksession.catalog.setCurrentDatabase("uk_refinedzone")
    staging_df.createOrReplaceTempView('f')
    event_df.createOrReplaceTempView('e')
    event_tl_df.createOrReplaceTempView('e_tl')
    forms_sfdc_virtual_campaigns.createOrReplaceTempView('dbo_123formssfdcvirtualcampaigns')

    # data_df.createOrReplaceTempView('123form_forms')

    print('integrationenableflag logic ...')
    enable_integration_df = sparksession.sql("""
        select campaignid, event_alpha_code, showname,eventedition, formname, eventeditioncode, formid 
        from  (select *, ROW_NUMBER() over(partition by event_alpha_code, eventedition order by formid desc) rn  from
        (select distinct i.campaignid,e.event_alpha_code, i.showname,i.eventedition,f.formname ,s.eventeditioncode,f.formid
        from smartsheet_exhibitorenquirysfdcintegrationstatus i
        join e_tl on e_tl.event_name = i.showname
        join e on e.row_wid = e_tl.row_wid
        left join f on f.eventalphacode = e.event_alpha_code and f.standenquiryflag = 1
        left join 123form_submissions s on s.formid = f.formid and s.eventeditioncode = i.eventedition
        where integrationstatus in ('Ready to go live','Live') and formname not like '% ES%' and formname not like '% PT%' and formname not like '% CN%' and formname not like 'ATM TF%' and formname not like 'ATM Wellness%'
        and f.formid not in (3231585,3231405)
        and f.formid not in (select formid from dbo_123formssfdcvirtualcampaigns)
        )a)aa
        where aa.rn = 1
    """)

    print('SETTING integrationenableflag ...')
    id_list = [row['formid'] for row in enable_integration_df.collect()]
    to_integrationenableflag_udf = udf(lambda form_id: '1' if form_id in id_list else '0', StringType())
    staging_df = staging_df.withColumn('integrationenableflag', to_integrationenableflag_udf('formid'))

    print('GETTING updated rows only ...')
    df = staging_df.subtract(data_df)
    return df


def set_event_alpha_code(form_name, code_list):
    for c in code_list:
        if c in form_name:
            return c
    return None


def set_forms_stand_enquiry_flag(form_name):
    regexes = [
        "Stand.*Enquiry",
        "Exhibitor.*nquiry",
        "Stand.*Application",
        "Enquire.*about.*exhibiting",
        "book.*stand",
        "Exhibitor.*Register.*Interest",
        "OE.*Exhibitor.*Form"
    ]
    combined = "(" + ")|(".join(regexes) + ")"
    if re.match(combined, form_name):
        return 1
    return 0


def forms_submissions_refined_data_transform(sparksession, data_df):
    print('GETTING data from uk_metadb ...')
    sparksession.catalog.setCurrentDatabase("uk_metadb")
    forms_privacy_fields = sparksession.sql('select * from dbo_123formsprivacyfields')
    froms_fields_metadata = sparksession.sql("""
        select formname, fieldtitle, `field mapping` as field_mapping from dbo_123fromsfieldsmetadata
    """)

    print('GETTING data from globaldb_refined ...')
    sparksession.catalog.setCurrentDatabase("globaldb_refined")
    event_df = sparksession.sql("select * from event")
    eventedition_df = sparksession.sql("select * from eventedition")

    print('GETTING data from uk_refinedzone ...')
    sparksession.catalog.setCurrentDatabase("uk_refinedzone")

    data_df.createOrReplaceTempView('123form_sub')
    event_df.createOrReplaceTempView('e')
    eventedition_df.createOrReplaceTempView('event_e')
    eventedition_df = eventedition_df.withColumn('event_edition_end_date', col('event_edition_end_date').cast("string"))

    previous_ee_df = sparksession.sql("""
            select ee.event_edition_short_desc,
                case
                when e.event_frequency = 'Annual' then ee.event_edition_code-1
                when e.event_frequency = 'Biennial' then ee.event_edition_code-2
                when e.event_frequency = 'Triennial' then ee.event_edition_code-3
                when e.event_frequency = 'Quadrennial' then ee.event_edition_code-4
                else 0 end as previous_ee
            from event_e ee
            left join e on ee.event_wid = e.row_wid
            where e.event_alpha_code <> 'Unspecified' and ee.event_edition_code is not null
                and ee.event_edition_code NOT LIKE '%[^0-9]%'
        """)

    eventedition_df = eventedition_df.alias('ee') \
        .join(previous_ee_df.alias('pee'),
              [eventedition_df.event_edition_short_desc == previous_ee_df.event_edition_short_desc],
              how='left') \
        .select('ee.*', col('pee.previous_ee').alias('previous_ee'))

    eventedition_df.createOrReplaceTempView('event_e')

    ee_df = sparksession.sql("""
            select e.event_alpha_code, ee.event_edition_code, nee.event_edition_end_date as prEndDate, e.event_frequency,
                ee.event_edition_end_date as SalesCycleEndDate, ee.event_edition_short_desc
            from event_e ee
            left join e on ee.event_wid = e.row_wid
            left join event_e nee on ee.previous_ee = nee.event_edition_code and ee.event_wid = nee.event_wid
        """)

    to_cycle_start_udf = udf(
        lambda end_date, event_frequency, current_end_date: get_cycle_start_date(end_date, event_frequency,
                                                                                 current_end_date), StringType())
    ee_df = ee_df.withColumn('SalesCycleStartDate',
                             to_cycle_start_udf('prEndDate', 'event_frequency', 'SalesCycleEndDate'))

    ee_df.createOrReplaceTempView('ee_df')

    forms_df = sparksession.sql('select * from 123form_forms')

    staging_df = data_df.alias('df') \
        .join(forms_df.alias('f'),
              [data_df.formid == forms_df.formid], how='left') \
        .select('df.*', col('f.eventalphacode').alias('eventalphacode'))

    staging_df = staging_df.alias('df') \
        .join(ee_df.alias('ee'),
              [staging_df.eventalphacode == ee_df.event_alpha_code,
               staging_df.date > ee_df.SalesCycleStartDate,
               staging_df.date < ee_df.SalesCycleEndDate], how='left') \
        .select('df.*', col('ee.event_edition_code').alias('codetoUpdate')) \
        .withColumn('eventeditioncode',
                    when(col('eventeditioncode').isNotNull(), col('eventeditioncode')).otherwise(
                        col('codetoUpdate'))) \
        .drop('codetoUpdate', 'eventalphacode')

    staging_df.createOrReplaceTempView('123form_sub')

    print('fieldtitleformatted logic - part 1...')
    staging_df.createOrReplaceTempView('123form_sub')
    forms_privacy_fields.createOrReplaceTempView('pf')
    fpf_df = sparksession.sql("""
        select pf.formid, pf.fieldid, pf.fieldtitleformatted
        from 123form_forms f
        join 123form_sub fs on fs.formid = f.formid
        join pf on f.formid = pf.formid and fs.fieldid = pf.fieldid
        where f.integrationenableflag = '1'
    """)

    print('SETTING fieldtitleformatted - part 1...')
    staging_df = staging_df.alias('df')\
        .join(fpf_df.alias('fpf'), [staging_df.formid == fpf_df.formid, staging_df.fieldid == fpf_df.fieldid], how='left') \
        .select('df.*', col('fpf.fieldtitleformatted').alias('fpf_fieldtitleformatted')) \
        .withColumn('fieldtitleformatted', when(col('fieldtitleformatted').isNotNull(), col('fieldtitleformatted')).otherwise(col('fpf_fieldtitleformatted'))) \
        .drop('fpf_fieldtitleformatted')

    # print('fieldtitleformatted logic - part 2...')
    # staging_df.createOrReplaceTempView('123form_sub')
    # froms_fields_metadata.createOrReplaceTempView('fm')
    # ffm_df = sparksession.sql("""
    #     select fs.formid, fs.fieldid, fm.field_mapping
    #     from 123form_sub fs
    #     left join 123form_forms f on f.formid = fs.formid
    #     left join 123form_fields ff on ff.formid = fs.formid and ff.fieldid = fs.fieldid
    #     left join fm on fm.formname = f.formname and fm.fieldtitle = ff.fieldtitle
    #     where fm.field_mapping is not null
    # """)
    #
    # print('SETTING fieldtitleformatted - part 2...')
    # staging_df = staging_df.alias('df')\
    #     .join(ffm_df.alias('ffm'), [staging_df.formid == ffm_df.formid, staging_df.fieldid == ffm_df.fieldid], how='left') \
    #     .select('df.*', col('ffm.field_mapping').alias('ffm_field_mapping')) \
    #     .withColumn('fieldtitleformatted', when(col('fieldtitleformatted').isNotNull(), col('fieldtitleformatted')).otherwise(col('ffm_field_mapping'))) \
    #     .drop('ffm_field_mapping')

    print('SETTING org_fieldid temporary field ...')
    to_org_fieldid_udf = udf(lambda field_id: get_original_field_id(field_id), StringType())
    staging_df = staging_df.withColumn('org_fieldid', to_org_fieldid_udf('fieldid'))
    print('fieldtitleformatted logic - part 3...')
    staging_df.createOrReplaceTempView('123form_sub')
    field_sub_df = sparksession.sql("""
        select fs.formid, fs.fieldid, fs.fieldvalue, fs.xmlid, ff.fieldid as ff_fieldid, ff.fieldtitle
        from 123form_sub fs
        left join 123form_fields ff on fs.formid = ff.formid and fs.org_fieldid = ff.fieldid
        where fs.fieldtitleformatted is null
    """)

    staging_df = staging_df.drop('org_fieldid')

    print('get_fieldtitleformatted_from_field_info ...')
    to_titleformated_udf = udf(lambda field_title, field_id: get_fieldtitleformatted_from_field_info(field_title, field_id), StringType())
    field_sub_df = field_sub_df.withColumn('ftf_fieldtitleformatted', to_titleformated_udf('fieldtitle', 'fieldid'))

    print('SETTING fieldtitleformatted - part 3...')
    staging_df = staging_df.alias('df')\
        .join(field_sub_df.alias('ftf'),
              [staging_df.formid == field_sub_df.formid,
              staging_df.fieldid == field_sub_df.fieldid,
              staging_df.fieldvalue == field_sub_df.fieldvalue,
              staging_df.xmlid == field_sub_df.xmlid], how='left') \
        .select('df.*', col('ftf.ftf_fieldtitleformatted').alias('ftf_fieldtitleformatted')) \
        .withColumn('fieldtitleformatted',
                    when(col('fieldtitleformatted').isNotNull(), col('fieldtitleformatted')).otherwise(col('ftf_fieldtitleformatted'))) \
        .drop('ftf_fieldtitleformatted')

    df = staging_df.subtract(data_df)
    return df


def get_cycle_start_date(previous_end_date, event_frequency, current_end_date):
    if previous_end_date:
        return previous_end_date
    year_delta = 1
    if event_frequency == 'Biennial':
        year_delta = 2
    elif event_frequency == 'Triennial':
        year_delta = 3
    elif event_frequency == 'Quadrennial':
        year_delta = 4
    if type(current_end_date) is str:
        current_end_date = current_end_date[:10]
        current_end_date = datetime.strptime(current_end_date, "%Y-%m-%d")
    if current_end_date.day == 29 and current_end_date.month == 2 and year_delta != 4:
        current_end_date = current_end_date - timedelta(days=1)
    end_date = current_end_date.replace(current_end_date.year - 1 * year_delta)
    return end_date.strftime("%Y-%m-%d")


def get_original_field_id(field_id):
    options = ['-', '_']
    for sep in options:
        if sep in field_id:
            return field_id.split(sep)[0]
    return field_id


def get_fieldtitleformatted_from_field_info(field_title, field_id):
    if not field_title:
        return None
    stand_size_list = ('Approximate Stand Size Required (SQM)',
        'What size stand are you interested in? ',
        'Approximate Stand Size Required (SQM)',
        'Approximate Stand Size (m&sup2;) Required',
        'Approximate Stand Size Required (SQM)',
        'Approximate Stand Size Required (ft2)',
        'Approximate Stand Size Required (SQM)',
        'Approximate Stand Size Required (SQM)',
        'What size stand are you interested in?',
        'What stand size do you require (m&sup2;)?',
        'Approximate Stand Size Required (SQM)',
        'What size stand are you interested in? (please indicate in m²)',
        'Approximate Stand Size Required (SQM)',
        'Estimated size of stand',
        'What size stand are you interested in? (minimum of 9m2)',
        'Approximate Stand Size Required (SQM)',
        'What size stand are you interested in?',
        'What size stand are you interested in?',
        'Stand size & dimensions',
        'Approximate Stand Size Required (SQM)',
        'What size stand are you interested in? (minimum of 9m2)',
        'What stand size do you require (m&sup2;)?',
        'Stand size & dimensions',
        'What stand size do you require (m&sup2;)?',
        'Approximate Stand Size Required (SQM)',
        'Approximate Stand Size Required (SQM)')

    if field_title in (
    'Company name', 'Name of exhibiting company', 'Nombre de la Empresa', 'Nome da Empresa', 'Organisation', 'Company',
    'Nome Fantasia para Divulga&ccedil;&atilde;o'):
        return 'Organisation Name'
    if field_title in ('Email', 'Email address', 'Contact Email', 'Company Email', 'Work Email', 'Correo Electrónico'):
        return 'Email'
    if field_title in ('name', 'Contact Name', 'Nome para contato', 'Nombre para contacto') and field_id.endswith('-3'):
        return 'Title'
    if (field_title in ('name','Contact Name','Nome para contato','Nombre para contacto') and field_id.endswith('-1')) or field_title == 'First name':
        return 'First Name'
    if (field_title in ('name','Contact Name','Nome para contato','Nombre para contacto') and field_id.endswith('-2')) or field_title in ('Last name','surname'):
        return 'Last Name'
    if (field_title in ('Company Address','Address','Organisation Address','Endereço do Empresa','Dirección') and field_id.endswith('-1')) or field_title in ('Address 1','Address Line 1'):
        return 'Address1'
    if (field_title in ('Company Address','Address','Organisation Address','Endereço do Empresa','Dirección') and field_id.endswith('-2')) or field_title in ('Address 2','Address Line 2'):
        return 'Address2'
    if (field_title in ('Company Address','Address','Organisation Address','Endereço do Empresa','Dirección') and field_id.endswith('-3')) or field_title == 'Town/City':
        return 'Address3'
    if field_title in ('Company Address','Address','Organisation Address','Endereço do Empresa','Dirección') and field_id.endswith('-4'):
        return 'Address4'
    if (field_title in ('Company Address','Address','Organisation Address','Endereço do Empresa','Dirección') and field_id.endswith('-5')) or field_title in ('Post / Zip Code','Post/Zip Code'):
        return 'Address5'
    if (field_title in ('Company Address','Address','Organisation Address','Endereço do Empresa','Dirección') and field_id.endswith('-6')) or field_title == 'Country':
        return 'Address6'
    if field_title == 'Job Title':
        return 'Job Title'
    if field_title in stand_size_list:
        return 'StandSize'
    if 'Name' in field_title and field_id.endswith('-3'):
        return 'PREFIX'
    if 'Title' in field_title and 'Job' not in field_title:
        return 'PREFIX'
    if 'Name' in field_title and field_id.endswith('-1'):
        return 'FIRST NAME'
    if 'First Name' in field_title:
        return 'FIRST NAME'
    if 'Last Name' in field_title or field_title == 'Family name':
        return 'LAST NAME'
    if 'Name' in field_title and field_id.endswith('-2'):
        return 'LAST NAME'
    if 'Address' in field_title and field_id.endswith('-1'):
        return 'ADDRESS LINE1'
    if 'Address' in field_title and '1' in field_title and 'Email' not in field_title:
        return 'ADDRESS LINE1'
    if 'Address' in field_title and field_id.endswith('-2'):
        return 'ADDRESS LINE2'
    if 'Address' in field_title and '2' in field_title and 'Email' not in field_title:
        return 'ADDRESS LINE2'
    if 'Address' in field_title and field_id.endswith('-3'):
        return 'ADDRESS LINE3'
    if 'Address' in field_title and '3' in field_title and 'Email' not in field_title:
        return 'ADDRESS LINE3'
    if 'Address' in field_title and field_id.endswith('-4'):
        return 'CITY'
    if 'Address' in field_title and field_id.endswith('-5'):
        return 'POSTCODE'
    if 'Address' in field_title and field_id.endswith('-6'):
        return 'COUNTRY'
    if 'Privacy' in field_title:
        return 'PRIVACY'
    return field_title


def bright_talk_channel_sub_refined_data_transform(sparksession):
    sparksession.catalog.setCurrentDatabase("uk_refined_zone")

    event_df = sparksession.sql("""
        select  b.portfolio, b.event_alpha_code, b.email, min(b.firstregdate) firstregdate
        from
        (select  btc.portfolio, e.event_alpha_code, isnull(r.rgt_per_email,c.email) email, min(isnull(r.rgt_dt,pco.publish_dt)) firstregdate
        from wc_participant_person_ps pp
        left join wc_registration_ps r on r.row_wid = pp.registration_wid
        join wc_participant_org_ps pco on pco.row_wid = pp.pco_wid
        join wc_event_ps e on e.row_wid = pp.event_wid
        join brighttalk_channels btc on btc.eventalphacode = e.event_alpha_code
        join wc_customer_contact_ps c on c.cust_person_wid = pp.person_wid
        where isnull(r.rgt_per_email,c.email) is not null
        group by btc.portfolio, e.event_alpha_code, isnull(r.rgt_per_email,c.email),(isnull(r.rgt_dt,pco.publish_dt))
        having (isnull(r.rgt_dt,pco.publish_dt)) >= (select convert(varchar,dateadd(year,-3,getdate())))
        union
        select btc.portfolio, e.event_alpha_code, l.email, l.w_insert_dt firstregdate
        from wc_lead_ps l
        join wc_event_edition_ps ee on ee.row_wid = l.event_ed_wid
        join wc_event_ps e on e.row_wid = ee.event_wid
        join brighttalk_channels btc on btc.eventalphacode = event_alpha_code
        where l.is_converted = 'N' and l.email is not null
        group by btc.portfolio, e.event_alpha_code, l.email,l.w_insert_dt
        having l.w_insert_dt >= (select convert(varchar,dateadd(year,-3,getdate())))
        )b group by b.portfolio, b.event_alpha_code, b.email
    """)

    portfolio_df = sparksession.sql("""
        select  b.portfolio, b.email, min(b.firstregdate) firstregdate
        from
        (select  btc.portfolio, isnull(r.rgt_per_email,c.email) email, min(isnull(r.rgt_dt,pco.publish_dt)) firstregdate
        from wc_participant_person_ps pp
        left join wc_registration_ps r on r.row_wid = pp.registration_wid
        join wc_participant_org_ps pco on pco.row_wid = pp.pco_wid
        join wc_event_ps e on e.row_wid = pp.event_wid
        join brighttalk_channels btc on btc.eventalphacode = e.event_alpha_code
        join wc_customer_contact_ps c on c.cust_person_wid = pp.person_wid
        where isnull(r.rgt_per_email,c.email) is not null
        group by btc.portfolio, isnull(r.rgt_per_email,c.email),(isnull(r.rgt_dt,pco.publish_dt))
        having (isnull(r.rgt_dt,pco.publish_dt)) >= (select convert(varchar,dateadd(year,-3,getdate())))
        union
        select btc.portfolio, l.email, l.w_insert_dt firstregdate
        from wc_lead_ps l
        join wc_event_edition_ps ee on ee.row_wid = l.event_ed_wid
        join wc_event_ps e on e.row_wid = ee.event_wid
        join brighttalk_channels btc on btc.eventalphacode = event_alpha_code
        where l.is_converted = 'N' and l.email is not null
        group by btc.portfolio, l.email,l.w_insert_dt
        having l.w_insert_dt >= (select convert(varchar,dateadd(year,-3,getdate())))
        )b group by b.portfolio, b.email
    """)

    bu_df = sparksession.sql("""
        select  b.email, min(b.firstregdate) firstregdate
        from
        (select isnull(r.rgt_per_email,c.email) email, min(isnull(r.rgt_dt,pco.publish_dt)) firstregdate
        from wc_participant_person_ps pp
        left join wc_registration_ps r on r.row_wid = pp.registration_wid
        join wc_participant_org_ps pco on pco.row_wid = pp.pco_wid
        join wc_event_ps e on e.row_wid = pp.event_wid
        join brighttalk_channels btc on btc.eventalphacode = e.event_alpha_code
        join wc_customer_contact_ps c on c.cust_person_wid = pp.person_wid
        where isnull(r.rgt_per_email,c.email) is not null
        group by isnull(r.rgt_per_email,c.email),(isnull(r.rgt_dt,pco.publish_dt))
        having (isnull(r.rgt_dt,pco.publish_dt)) >= (select convert(varchar,dateadd(year,-3,getdate())))
        union
        select l.email, l.w_insert_dt firstregdate
        from wc_lead_ps l
        join wc_event_edition_ps ee on ee.row_wid = l.event_ed_wid
        join wc_event_ps e on e.row_wid = ee.event_wid
        join brighttalk_channels btc on btc.eventalphacode = event_alpha_code
        where l.is_converted = 'N' and l.email is not null
        group by l.email,l.w_insert_dt
        having l.w_insert_dt >= (select convert(varchar,dateadd(year,-3,getdate())))
        )b group by b.email
    """)

    data_df = sparksession.sql('select * from brighttalk_channelsubscribers')
    data_df.createOrReplaceTempView('brighttalk_channelsub')
    event_df.createOrReplaceTempView('event_df')
    portfolio_df.createOrReplaceTempView('portfolio_df')
    bu_df.createOrReplaceTempView('bu_df')

    new_fields_df = sparksession.sql("""
        select (case when e.email is null then 1 else 0 end) as new_to_event, 
               (case when p.email is null then 1 else 0 end) as new_to_portfolio, 
               (case when bu.email is null then 1 else 0 end) as new_to_bu,
               s.channelid
        from brighttalk_channelsub s
        join brightTalk_channels ch on ch.id = s.channelid
        left join event_df e on e.event_alpha_code = ch.eventalphacode and e.email = s.useremail
        left join portfolio_df p on p.portfolio = ch.portfolio and p.email = s.useremail
        left join bu_df bu on bu.email = s.useremail
        where ch.eventalphacode <> 'Unspecified'
    """)

    staging_df = data_df.withColumn('newtoevent', when(col('newtoevent').isNull(), lit(0)).otherwise(col('newtoevent')))\
        .withColumn('newtoportfolio', when(col('newtoportfolio').isNull(), lit(0)).otherwise(col('newtoportfolio')))\
        .withColumn('newtobu', when(col('newtobu').isNull(), lit(0)).otherwise(col('newtobu')))

    staging_df = staging_df.alias('df').join(new_fields_df.alias('nfd'), df.channelid == nfd.channelid, how='left') \
        .select('df.*', col('nfd.new_to_event').alias('new_to_event'),
                col('nfd.new_to_portfolio').alias('new_to_portfolio'), col('nfd.new_to_bu').alias('new_to_bu'),) \
        .withColumn('newtoevent', when(col('new_to_event').isNotNull(), col('new_to_event')).otherwise(col('newtoevent'))) \
        .withColumn('newtoportfolio', when(col('new_to_portfolio').isNotNull(), col('new_to_portfolio')).otherwise(col('newtoportfolio'))) \
        .withColumn('newtobu', when(col('new_to_bu').isNotNull(), col('new_to_bu')).otherwise(col('newtobu'))) \
        .drop('new_to_event', 'new_to_portfolio', 'new_to_bu')

    df = staging_df.subtract(data_df)
    return df


def bright_talk_webcasts_refined_data_transform(sparksession):
    sparksession.catalog.setCurrentDatabase("uk_refined_zone")
    data_df = sparksession.sql('select * from brighttalk_webcast')

    staging_df = data_df.withColumn('webinarstatus', set_webinar_status(data_df['start']))

    df = staging_df.subtract(data_df)
    return df


def set_webinar_status(webinar_start):
    start_date = datetime.strptime(webinar_start, "%Y-%m-%dT%H:%M:%SZ").date()
    today_date = datetime.now().date()
    if start_date < today_date:
        return 'Completed'
    if start_date > today_date:
        return 'Scheduled'
    return 'Live'


def certain_registration_refined_data_transform(sparksession):
    sparksession.catalog.setCurrentDatabase("uk_refined_zone")
    data_df = sparksession.sql('select * from certain_registration')
    staging_df = data_df.withColumn('datecompletedutc', to_utc_timestamp(col('datecompleted'), 'PST'))

    df = staging_df.subtract(data_df)
    return df


def sector_contact_attributes_refined_data_transform(sparksession):
    sparksession.catalog.setCurrentDatabase("uk_refined_zone")
    data_df = sparksession.sql('select * from sector_contactattributes')

    sparksession.catalog.setCurrentDatabase("uk_metadb")
    sq_df = sparksession.sql('select * from dbo_sectorquestionsmapping')

    staging_df = data_df.alias('df').join(sq_df.alias('sq'), df.name == sq.question, how='left')\
        .select('df.*', col('sq.questioncleansed').alias('sq_namecleansed'))\
        .withColumn('namecleansed', when(col('sq_namecleansed').isNotNull(), col('sq_namecleansed')).otherwise(col('namecleansed')))\
        .drop('sq_namecleansed')

    df = staging_df.subtract(data_df)
    return df


# def smartsheets_showpostponementsandcancellations_refined_data_transform(sparksession, data_df):
#     # need to create meta table to convert show name into EventAlphaCode (convert_to_alpha_code as ctac)
#     data_df = data_df.alias('df').join(ctac.alias('ctac'), df.show == ctac.show_name, how='left') \
#         .select('df.*', col('ctac.event_alpha_code').alias('event_alpha_code'))
#
#     # need to check with shows cancellations for deventedition
#     return data_df

#
# def bright_talk_webcasts_dependent_tables_transform(sparksession):
#     return {}
