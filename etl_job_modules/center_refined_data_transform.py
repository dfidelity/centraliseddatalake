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


def refined_data_transform(sparksession, data_df, glue_table_name, raw_db, refined_db):

    print("raw_data_transform step")

    options = {
        'participatingorg_tl': edw_participatingorg_tl_transform,
        'opportunitysales_bridge': edw_opportunitysales_bridge_transform,
        'participatingorg_stand': edw_participatingorg_stand_transform,
        'participatingperson': edw_participatingperson_transform,
        'participatingperson_tl': edw_participatingperson_tl_transform,
        'atshowactivity': edw_atshowactivity_transform,
        'subscription_members': edw_subscription_members_transform,
        'isg_salestarget': edw_isg_salestarget_transform,
        'quoteline': edw_quoteline_transform,
        'opportunityteam': edw_opportunityteam_transform,
        'event_membership': edw_event_membership_transform,
        'participatingorg': edw_participatingorg_transform,
        'opportunity': edw_opportunity_transform,
        'salescampaign': edw_salescampaign_transform,
        'campaignmember_contact': edw_campaignmember_contact_transform,
        'registration': edw_registration_transform,
        'campaignmember_lead': edw_campaignmember_lead_transform,
        'mv_salesorder_line': mv_salesorder_line_data_transform,
        'mv_profile_response': mv_profile_response_data_transform
    }

    return options[glue_table_name](sparksession, data_df, raw_db, refined_db)


def edw_participatingorg_tl_transform(sparksession, data_df, raw_db, refined_db):
    print("edw_participatingorg_tl_transform step")

    # pco_wid_list = [row['pcowid'] for row in data_df.rdd.collect()]

    sparksession.catalog.setCurrentDatabase(raw_db)

    # ps_table = sparksession.sql("""
    # SELECT t.rowwid, t.eventedwid FROM participatingorg t
    # INNER JOIN (
    #   SELECT rowwid, MAX(loadinsertedtimestamp) as loadinsertedtimestamp
    #   from participatingorg
    #   group by rowwid
    # ) tm on t.rowwid = tm.rowwid and t.loadinsertedtimestamp = tm.loadinsertedtimestamp
    # """)
    ps_table = sparksession.sql("SELECT rowwid, eventedwid FROM participatingorg")
    # + "where t.rowwid in ('" + "', '".join(str(x) for x in pco_wid_list) + "')")


    staging_df = data_df.alias('df') \
        .join(ps_table.alias('ps'), [data_df.pcowid == ps_table.rowwid], how='left') \
        .select('df.*', col('ps.eventedwid').alias('eventedwid'))

    return staging_df


def edw_opportunitysales_bridge_transform(sparksession, data_df, raw_db, refined_db):
    print("edw_opportunitysales_bridge_transform step")

    sparksession.catalog.setCurrentDatabase(raw_db)

    ps_table = sparksession.sql("""
    SELECT t.rowwid, t.eventedwid FROM opportunity t
    INNER JOIN (
      SELECT rowwid, MAX(loadinsertedtimestamp) as loadinsertedtimestamp
      from opportunity
      group by rowwid
    ) tm on t.rowwid = tm.rowwid and t.loadinsertedtimestamp = tm.loadinsertedtimestamp
    """)

    staging_df = data_df.alias('df') \
        .join(ps_table.alias('ps'), [data_df.optywid == ps_table.rowwid], how='left') \
        .select('df.*', col('ps.eventedwid').alias('eventedwid'))

    return staging_df


def edw_participatingorg_stand_transform(sparksession, data_df, raw_db, refined_db):
    print("edw_participatingorg_stand_transform step")

    sparksession.catalog.setCurrentDatabase(raw_db)

    # ps_table = sparksession.sql("""
    # SELECT t.rowwid, t.eventedwid FROM participatingorg t
    # INNER JOIN (
    #   SELECT rowwid, MAX(loadinsertedtimestamp) as loadinsertedtimestamp
    #   from participatingorg
    #   group by rowwid
    # ) tm on t.rowwid = tm.rowwid and t.loadinsertedtimestamp = tm.loadinsertedtimestamp
    # """)
    ps_table = sparksession.sql("SELECT rowwid, eventedwid FROM participatingorg")

    staging_df = data_df.alias('df') \
        .join(ps_table.alias('ps'), [data_df.pcowid == ps_table.rowwid], how='left') \
        .select('df.*', col('ps.eventedwid').alias('eventedwid'))

    return staging_df


def edw_participatingperson_transform(sparksession, data_df, raw_db, refined_db):
    print("edw_participatingperson_transform step")

    sparksession.catalog.setCurrentDatabase(raw_db)

    # ps_table = sparksession.sql("""
    # SELECT t.rowwid, t.partpersontypeid, t.partpersontype, t.partpersontypedesc FROM participatingpersontype t
    # INNER JOIN (
    #   SELECT rowwid, MAX(loadinsertedtimestamp) as loadinsertedtimestamp
    #   from participatingpersontype
    #   group by rowwid
    # ) tm on t.rowwid = tm.rowwid and t.loadinsertedtimestamp = tm.loadinsertedtimestamp
    # """)
    ps_table = sparksession.sql("""
    SELECT rowwid, partpersontypeid, partpersontype, partpersontypedesc FROM participatingpersontype
    """)

    staging_df = data_df.alias('df') \
        .join(ps_table.alias('ps'), [data_df.partpersontypewid == ps_table.rowwid], how='left') \
        .select('df.*', col('ps.partpersontypeid').alias('partpersontypeid'),
                col('ps.partpersontype').alias('partpersontype'),
                col('ps.partpersontypedesc').alias('partpersontypedesc'))

    return staging_df


def edw_participatingperson_tl_transform(sparksession, data_df, raw_db, refined_db):
    print("edw_participatingperson_tl_transform step")

    sparksession.catalog.setCurrentDatabase(raw_db)

    # ps_table = sparksession.sql("""
    # SELECT t.rowwid, t.eventedwid FROM participatingperson t
    # INNER JOIN (
    #   SELECT rowwid, MAX(loadinsertedtimestamp) as loadinsertedtimestamp
    #   from participatingperson
    #   group by rowwid
    # ) tm on t.rowwid = tm.rowwid and t.loadinsertedtimestamp = tm.loadinsertedtimestamp
    # """)
    ps_table = sparksession.sql("SELECT rowwid, eventedwid FROM participatingperson")

    staging_df = data_df.alias('df') \
        .join(ps_table.alias('ps'), [data_df.pinwid == ps_table.rowwid], how='left') \
        .select('df.*', col('ps.eventedwid').alias('eventedwid'))

    return staging_df


def edw_atshowactivity_transform(sparksession, data_df, raw_db, refined_db):
    print("edw_atshowactivity_transform step")

    sparksession.catalog.setCurrentDatabase(raw_db)

    # ps_table = sparksession.sql("""
    #     SELECT t.rowwid, t.activitytypecode, t.activitytypedesc FROM salesactivitytype t
    #     INNER JOIN (
    #       SELECT rowwid, MAX(loadinsertedtimestamp) as loadinsertedtimestamp
    #       from salesactivitytype
    #       group by rowwid
    #     ) tm on t.rowwid = tm.rowwid and t.loadinsertedtimestamp = tm.loadinsertedtimestamp
    # """)
    ps_table = sparksession.sql("SELECT rowwid, activitytypecode, activitytypedesc FROM salesactivitytype")

    staging_df = data_df.alias('df') \
        .join(ps_table.alias('ps'), [data_df.activitytypewid == ps_table.rowwid], how='left') \
        .select('df.*', col('ps.activitytypecode').alias('activitytypecode'),
                col('ps.activitytypedesc').alias('activitytypedesc'))

    return staging_df


def edw_subscription_members_transform(sparksession, data_df, raw_db, refined_db):
    print("edw_subscription_members_transform step")

    sparksession.catalog.setCurrentDatabase(raw_db)

    # ps_table = sparksession.sql("""
    #     SELECT t.rowwid, t.eventwid FROM subscription t
    #     INNER JOIN (
    #       SELECT rowwid, MAX(loadinsertedtimestamp) as loadinsertedtimestamp
    #       from subscription
    #       group by rowwid
    #     ) tm on t.rowwid = tm.rowwid and t.loadinsertedtimestamp = tm.loadinsertedtimestamp
    # """)
    ps_table = sparksession.sql("SELECT rowwid, eventwid FROM subscription")

    staging_df = data_df.alias('df') \
        .join(ps_table.alias('ps'), [data_df.subscriptionwid == ps_table.rowwid], how='left') \
        .select('df.*', col('ps.eventwid').alias('eventwid'))

    return staging_df


def edw_isg_salestarget_transform(sparksession, data_df, raw_db, refined_db):
    print("edw_isg_salestarget_transform step")

    sparksession.catalog.setCurrentDatabase(raw_db)

    data_df.createOrReplaceTempView("isg_salestarget_temp")
    ps_table = sparksession.sql("""
        SELECT t.rowwid, t.eventedwid, t.salestargetwid FROM isg_salestarget_attribute t
        INNER JOIN (
          SELECT rowwid, MAX(loadinsertedtimestamp) as loadinsertedtimestamp
          from isg_salestarget_attribute
          group by rowwid
        ) tm on t.rowwid = tm.rowwid and t.loadinsertedtimestamp = tm.loadinsertedtimestamp
    """)
    ps_table.createOrReplaceTempView("isg_salestarget_attribute_temp")

    staging_df = sparksession.sql("""
        SELECT t1.*, t2.eventedwid from isg_salestarget_temp t1
        LEFT JOIN (
            SELECT distinct eventedwid, salestargetwid 
            from isg_salestarget_attribute_temp
        ) t2 on t1.rowwid = t2.salestargetwid
    """)

    return staging_df


def edw_quoteline_transform(sparksession, data_df, raw_db, refined_db):
    print("edw_quoteline_transform step")

    sparksession.catalog.setCurrentDatabase(raw_db)

    ps_table = sparksession.sql("""
        SELECT t.rowwid, t.eventeditionwid FROM quote t
        INNER JOIN (
            SELECT rowwid, MAX(loadinsertedtimestamp) as loadinsertedtimestamp
            from quote
            group by rowwid
        ) tm on t.rowwid = tm.rowwid and t.loadinsertedtimestamp = tm.loadinsertedtimestamp
    """)

    staging_df = data_df.alias('df') \
        .join(ps_table.alias('ps'), [data_df.quotewid == ps_table.rowwid], how='left') \
        .select('df.*', col('ps.eventeditionwid').alias('eventeditionwid'))

    return staging_df


def edw_opportunityteam_transform(sparksession, data_df, raw_db, refined_db):
    print("edw_opportunityteam_transform step")

    sparksession.catalog.setCurrentDatabase(raw_db)

    ps_table = sparksession.sql("""
        SELECT t.rowwid, t.eventedwid, t.eventwid FROM opportunity t
        INNER JOIN (
            SELECT rowwid, MAX(loadinsertedtimestamp) as loadinsertedtimestamp
            from opportunity
            group by rowwid
        ) tm on t.rowwid = tm.rowwid and t.loadinsertedtimestamp = tm.loadinsertedtimestamp
    """)

    staging_df = data_df.alias('df') \
        .join(ps_table.alias('ps'), [data_df.opportunitywid == ps_table.rowwid], how='left') \
        .select('df.*', col('ps.eventedwid').alias('eventedwid'), col('ps.eventwid').alias('eventwid'))

    return staging_df


def edw_event_membership_transform(sparksession, data_df, raw_db, refined_db):
    print("edw_event_membership_transform step")

    sparksession.catalog.setCurrentDatabase(raw_db)

    # ...

    return data_df


def edw_participatingorg_transform(sparksession, data_df, raw_db, refined_db):
    print("edw_participatingorg_transform step")

    sparksession.catalog.setCurrentDatabase(raw_db)

    # ps_table = sparksession.sql("""
    #     SELECT t.rowwid, t.partorgtypeid, t.partorgtype, t.partorgtypedesc FROM participatingorgtype t
    #     INNER JOIN (
    #         SELECT rowwid, MAX(loadinsertedtimestamp) as loadinsertedtimestamp
    #         from participatingorgtype
    #         group by rowwid
    #     ) tm on t.rowwid = tm.rowwid and t.loadinsertedtimestamp = tm.loadinsertedtimestamp
    # """)
    ps_table = sparksession.sql("""
    SELECT rowwid, partorgtypeid, partorgtype, partorgtypedesc FROM participatingorgtype
    """)

    staging_df = data_df.alias('df') \
        .join(ps_table.alias('ps'), [data_df.partorgtypewid == ps_table.rowwid], how='left') \
        .select('df.*', col('ps.partorgtypeid').alias('partorgtypeid'),
                col('ps.partorgtype').alias('partorgtype'),
                col('ps.partorgtypedesc').alias('partorgtypedesc'))

    return staging_df


def edw_opportunity_transform(sparksession, data_df, raw_db, refined_db):
    print("edw_opportunity_transform step")

    sparksession.catalog.setCurrentDatabase(raw_db)

    # ps_table = sparksession.sql("""
    #     SELECT t.rowwid, t.opppipelinestgdesc, t.oppcompletepercentage FROM opportunitypipeline t
    #     INNER JOIN (
    #         SELECT rowwid, MAX(loadinsertedtimestamp) as loadinsertedtimestamp
    #         from opportunitypipeline
    #         group by rowwid
    #     ) tm on t.rowwid = tm.rowwid and t.loadinsertedtimestamp = tm.loadinsertedtimestamp
    # """)
    ps_table = sparksession.sql("""
    SELECT rowwid, opppipelinestgdesc, oppcompletepercentage FROM opportunitypipeline
    """)

    staging_df = data_df.alias('df') \
        .join(ps_table.alias('ps'), [data_df.opppipelinestgwid == ps_table.rowwid], how='left') \
        .select('df.*', col('ps.opppipelinestgdesc').alias('opppipelinestgdesc'),
                col('ps.oppcompletepercentage').alias('oppcompletepercentage'))

    return staging_df


def edw_salescampaign_transform(sparksession, data_df, raw_db, refined_db):
    print("edw_salescampaign_transform step")

    sparksession.catalog.setCurrentDatabase(raw_db)

    data_df.createOrReplaceTempView("salescampaign_temp")
    # ps_table = sparksession.sql("""
    #     SELECT t.rowwid, t.campaignwid, t.eventeditionwid FROM salescampaign_member t
    #     INNER JOIN (
    #       SELECT rowwid, MAX(loadinsertedtimestamp) as loadinsertedtimestamp
    #       from salescampaign_member
    #       group by rowwid
    #     ) tm on t.rowwid = tm.rowwid and t.loadinsertedtimestamp = tm.loadinsertedtimestamp
    # """)
    ps_table = sparksession.sql("SELECT rowwid, campaignwid, eventeditionwid FROM salescampaign_member")
    ps_table.createOrReplaceTempView("salescampaign_member_temp")

    staging_df = sparksession.sql("""
        SELECT t1.*, t2.eventeditionwid from salescampaign_temp t1
        LEFT JOIN (
            SELECT distinct campaignwid, eventeditionwid 
            from salescampaign_member_temp
        ) t2 on t1.rowwid = t2.campaignwid
    """)

    return staging_df


def edw_campaignmember_contact_transform(sparksession, data_df, raw_db, refined_db):
    print("edw_campaignmember_contact_transform step")

    sparksession.catalog.setCurrentDatabase(raw_db)

    # ps_table = sparksession.sql("""
    #     SELECT t.rowwid, t.eventedwid FROM campaign t
    #     INNER JOIN (
    #       SELECT rowwid, MAX(loadinsertedtimestamp) as loadinsertedtimestamp
    #       from campaign
    #       group by rowwid
    #     ) tm on t.rowwid = tm.rowwid and t.loadinsertedtimestamp = tm.loadinsertedtimestamp
    # """)
    ps_table = sparksession.sql("SELECT rowwid, eventedwid FROM campaign")

    staging_df = data_df.alias('df') \
        .join(ps_table.alias('ps'), [data_df.mcawid == ps_table.rowwid], how='left') \
        .select('df.*', col('ps.eventedwid').alias('eventedwid'))

    return staging_df


def edw_registration_transform(sparksession, data_df, raw_db, refined_db):
    print("edw_registration_transform step")

    sparksession.catalog.setCurrentDatabase(raw_db)
    # sparksession.catalog.recoverPartitions("registrationstatus")
    # sparksession.catalog.recoverPartitions("registrationtype")

    data_df.createOrReplaceTempView("registration_temp")
    # status_table = sparksession.sql("""
    #     SELECT t.rowwid, t.rgtstatuscode, t.rgtstatusdesc FROM registrationstatus t
    #     INNER JOIN (
    #       SELECT rowwid, MAX(loadinsertedtimestamp) as loadinsertedtimestamp
    #       from registrationstatus
    #       group by rowwid
    #     ) tm on t.rowwid = tm.rowwid and t.loadinsertedtimestamp = tm.loadinsertedtimestamp
    # """)
    status_table = sparksession.sql("SELECT rowwid, rgtstatuscode, rgtstatusdesc FROM registrationstatus")
    status_table.createOrReplaceTempView("registrationstatus_temp")

    # type_table = sparksession.sql("""
    #     SELECT t.rowwid, t.rgttypecode, t.rgttypedesc FROM registrationtype t
    #     INNER JOIN (
    #       SELECT rowwid, MAX(loadinsertedtimestamp) as loadinsertedtimestamp
    #       from registrationtype
    #       group by rowwid
    #     ) tm on t.rowwid = tm.rowwid and t.loadinsertedtimestamp = tm.loadinsertedtimestamp
    # """)
    type_table = sparksession.sql("SELECT rowwid, rgttypecode, rgttypedesc FROM registrationtype")
    type_table.createOrReplaceTempView("registrationtype_temp")

    staging_df = sparksession.sql("""
        SELECT t1.*, t2.rgtstatuscode, t2.rgtstatusdesc, t3.rgttypecode, t3.rgttypedesc
        from registration_temp t1
        LEFT JOIN registrationstatus_temp t2 on t2.rowwid = t1.rgtstatuswid
        LEFT JOIN registrationtype_temp t3 on t3.rowwid = t1.rgttypewid
    """)

    return staging_df


def edw_campaignmember_lead_transform(sparksession, data_df, raw_db, refined_db):
    print("edw_campaignmember_lead_transform step")

    sparksession.catalog.setCurrentDatabase(raw_db)

    # ps_table = sparksession.sql("""
    #     SELECT t.rowwid, t.eventedwid FROM campaign t
    #     INNER JOIN (
    #       SELECT rowwid, MAX(loadinsertedtimestamp) as loadinsertedtimestamp
    #       from campaign
    #       group by rowwid
    #     ) tm on t.rowwid = tm.rowwid and t.loadinsertedtimestamp = tm.loadinsertedtimestamp
    # """)
    ps_table = sparksession.sql("SELECT rowwid, eventedwid FROM campaign")

    staging_df = data_df.alias('df') \
        .join(ps_table.alias('ps'), [data_df.mcawid == ps_table.rowwid], how='left') \
        .select('df.*', col('ps.eventedwid').alias('eventedwid'))

    return staging_df


def mv_salesorder_line_data_transform(sparksession, data_df, raw_db, refined_db):
    # sparksession.catalog.setCurrentDatabase(refined_db)
    data_df = sparksession.sql(f"""
        SELECT fsales.rowwid,
            fsales.createdbywid,
            int(coalesce(fsales.changedbywid, 0)) AS changedbywid,
            fsales.createdondt,
            fsales.changedondt,
            fsales.deleteflg AS deleteflag,
            fsales.winsertdt,
            fsales.wupdatedt,
            fsales.datasourcenumid,
            fsales.etlprocwid,
            fsales.integrationid,
            int(coalesce(fsales.xquotewid, 0)) AS quotewid,
            int(coalesce(fsales.xeventedwid, 0)) AS xeventedwid,
            int(coalesce(fsales.xstandwid, 0)) AS standwid,
            int(coalesce(dim_doc_curr.rowwid, 0)) AS doccurrencywid,
            int(coalesce(dim_loc_curr.rowwid, 0)) AS loccurrecywid,
            int(coalesce(fsales.xproductwid, 0)) AS productwid,
            int(coalesce(fsales.xpackagewid, 0)) AS productpackagewid,
            int(coalesce(fsales.xacttypewid, 0)) AS ordertypewid,
            int(coalesce(fsales.xoptywid, 0)) AS opportunitywid,
            int(coalesce(fsales.salesrepwid, 0)) AS empwid,
            int(coalesce(fsales.orderstatuswid, 0)) AS orderstatuswid,
            int(coalesce(fsales.ordersourcewid, 0)) AS ordersourcewid,
            fsales.costamt,
                CASE
                    WHEN lower(order_type.xacttypecode) = 'return' THEN int(fsales.netamt) * -1
                    ELSE int(fsales.netamt)
                END AS netamt,
            fsales.listamt,
            fsales.discountamt,
            fsales.cancelledamt,
            fsales.rollupcostamt,
            fsales.rollupnetamt,
            fsales.rolluplistamt,
            fsales.rollupdiscountamt,
            fsales.rollupmarginamt,
                CASE
                    WHEN lower(order_type.xacttypecode) = 'return' THEN int(fsales.salesqty) * '-1'
                    ELSE int(fsales.salesqty)
                END AS salesqty,
                CASE
                    WHEN lower(order_type.xacttypecode) = 'return' THEN int(fsales.orderedqty) * '-1'
                    ELSE int(fsales.orderedqty)
                END AS orderedqty,
            fsales.cancelledqty,
                CASE
                    WHEN lower(order_type.xacttypecode) = 'return' THEN int(fsales.totalshippedqty) * '-1'
                    ELSE int(fsales.totalshippedqty)
                END AS totalshippedqty,
                CASE
                    WHEN lower(order_type.xacttypecode) = 'return' THEN int(fsales.totalinvoicedqty) * '-1'
                    ELSE int(fsales.totalinvoicedqty)
                END AS totalinvoicedqty,
            fsales.customerfirstorderflg,
            fsales.standarduomcode,
            fsales.primaryuomconvrate,
            fsales.primaryuomcode,
            fsales.standarduomconvrate,
            fsales.salesuomcode,
            fsales.ultimatedestcode,
            fsales.salesordernum,
            fsales.salesorderitem,
            fsales.salesorderitemdetailnum,
            fsales.refdocnum,
            fsales.refdocitem,
            fsales.purchordernum,
            fsales.fulfillordernum,
            fsales.fulfillorderlinenum,
            fsales.fulfilllinenum,
            fsales.itemtypecode,
            fsales.serviceflg,
            fsales.earlyshipmentsallowedflg,
            fsales.partialshipmentsallowedflg,
            fsales.bookingflg,
            fsales.shippableflg,
            fsales.fulfilledflg,
            fsales.finbacklogflg,
            fsales.scheduledflg,
            fsales.oprbacklogflg,
            fsales.billingflg,
            fsales.invoiceinterfacedflg,
            fsales.shippinginterfacedflg,
            fsales.rootprodflg,
            fsales.onholdflg,
            fsales.injeopardyflg,
            fsales.backorderflg,
            fsales.salesorderhdid,
            fsales.salesorderlnid,
            fsales.bookingid,
            fsales.fulfillorderid,
            fsales.fulfillorderlineid,
            fsales.doccurrcode,
            fsales.loccurrcode,
            fsales.locexchangerate,
            fsales.global1exchangerate,
            fsales.global2exchangerate,
            fsales.global3exchangerate,
            fsales.tenantid,
            fsales.rootprodlineid,
            fsales.xcustom,
            coalesce(prcpt_org.exhibitorflag, 0) AS exhibitorflag,
            coalesce(prcpt_org.sharerflag, 0) AS sharerflag,
            fsales.xweekstoevent AS weekstogo,
            fsales.xombarter,
            fsales.xeffectivedatefrom,
            fsales.xeffectivedateto,
            fsales.xstandid,
            fsales.xoppidheader,
            fsales.xbellidheader,
            fsales.xsectorheader,
            fsales.xsubsectorheader,
            fsales.xbarterheader,
            fsales.xcorners,
            fsales.xcustomerloyalty,
            fsales.xadhocdiscounttype,
            fsales.xadhocdiscountamount,
            fsales.xadhocreasoncode,
            fsales.xpackageitemnumber,
            fsales.xsuppressinvoice,
            fsales.xwaiverfee,
            fsales.xbeneficiarytocustomer,
            fsales.xbeneficiarytoaccountsite,
            fsales.xlastmoddatasource,
            fsales.xbeneficiarytocontact,
            fsales.xbarterflag,
            fsales.xcancellationreasoncode,
            fsales.xheadernotes,
            fsales.xoriginalsplitqty,
            fsales.xeventid,
            fsales.xeventeditionid,
            fsales.xflowstatusheadrer,
            fsales.xmilestonedue,
            fsales.xflowstatusline,
            fsales.xquoteid,
            fsales.xorderdate,
            fsales.xglaccountwid,
            int(coalesce(inv_cust_acc.rowwid, 0)) AS invoicecustaccntwid,
            int(coalesce(fsales.xinvoicecontactwid, 0)) AS invoicecontactwid,
            int(coalesce(fsales.xinvoicepersonwid, 0)) AS invoicepersonwid,
            int(coalesce(fsales.xinvoiceorgwid, 0)) AS invoiceorgwid,
            int(coalesce(fsales.xinvoiceaddrwid, 0)) AS invoiceaddressid,
            int(coalesce(fsales.customeraccountwid, 0)) AS contractedcustaccntwid,
            int(coalesce(fsales.xcontractcontactwid, 0)) AS contractedconwid,
            int(coalesce(fsales.xcontractpersonwid, 0)) AS contractedpersonwid,
            int(coalesce(fsales.xcontractorgwid, 0)) AS contractedorgwid,
            int(coalesce(fsales.xcontractaddrwid, 0)) AS contractedaddressid,
            int(coalesce(benf_cust_acc.rowwid, 0)) AS benfcustaccntwid,
            int(coalesce(fsales.xbeneficiarycontactwid, 0)) AS benfcontactwid,
            int(coalesce(fsales.xbeneficiarypersonwid, 0)) AS benfpersonwid,
            int(coalesce(fsales.xbeneficiaryaddrwid, 0)) AS benfaddressid,
            int(coalesce(fsales.xbeneficiaryorgwid, 0)) AS benforgwid,
            obu.friendlycd AS xobucode,
            int(coalesce(fsales.closedondtwid, 0)) AS closedondtwid,
            int(coalesce(fsales.cancelledondtwid, 0)) AS cancelledondtwid,
                CASE
                    WHEN lower(order_type.xacttypecode) = 'return' THEN
                    CASE
                        WHEN (prod.majorcategorycode IN ('1000', '1800', '1900')) AND int(fsales.listamt) <> 0 THEN int(fsales.salesqty)
                        ELSE 0
                    END *
                    CASE fsales.salesuomcode
                        WHEN 'm2' THEN 1
                        WHEN 'f2' THEN 0.0929
                        ELSE NULL
                    END * -1
                    ELSE
                    CASE
                        WHEN (prod.majorcategorycode IN ('1000', '1800', '1900')) AND int(fsales.listamt) <> 0 THEN int(fsales.salesqty)
                        ELSE 0
                    END *
                    CASE fsales.salesuomcode
                        WHEN 'm2' THEN 1
                        WHEN 'f2' THEN 0.0929
                        ELSE NULL
                    END
                END AS spacesalesqtysqm,
                CASE
                    WHEN lower(order_type.xacttypecode) = 'return' THEN
                    CASE
                        WHEN (prod.majorcategorycode IN ('1000', '1800', '1900')) AND int(fsales.listamt) <> 0 THEN int(fsales.salesqty)
                        ELSE 0
                    END *
                    CASE trim(fsales.salesuomcode)
                        WHEN 'f2' THEN 1
                        WHEN 'm2' THEN 10.7639
                        ELSE NULL
                    END * -1
                    ELSE
                    CASE
                        WHEN (prod.majorcategorycode IN ('1000', '1800', '1900')) AND int(fsales.listamt) <> 0 THEN int(fsales.salesqty)
                        ELSE 0
                    END *
                    CASE trim(fsales.salesuomcode)
                        WHEN 'f2' THEN 1
                        WHEN 'm2' THEN 10.7639
                        ELSE NULL
                    END
                END AS spacesalesqtysqf,
                CASE
                    WHEN ret.xcontractorgwid IS NOT NULL THEN 'y'
                    ELSE 'n'
                END AS longtermretention,
            int(coalesce(emp.xuserwid, 0)) AS xuserwid,
            fsales.xpackagelineflg AS packagelineflg,
            fsales.xpackageflg AS packageflg,
            fsales.xreturnattribute1 AS returnsalesorderhdid,
            fsales.xreturnattribute2 AS returnsalesorderlnid,
            fsales.xreturncontext AS returncontext,
            fsales.xreturnattribute3 AS returnsalesordernum,
            fsales.xsfdcordertype AS sfdcordertype,
            fsales.xduplicateordflg AS duplicateordflg,
            fsales.xsmartspaceflg,
            int(coalesce(participation_benf.partorgwid, participation_contr.partorgwid, 0)) AS xparticipatingorgwid,
            fsales.xvendorname,
            fsales.xemailoptout,
            int(coalesce(d.rowwid, 3)) AS isgrevncategorywid,
                CASE
                    WHEN fsales.loccurrcode = 'usd' THEN 1
                    ELSE int(coalesce(exc.conversionrate, 0))
                END AS relxconvrate,
            int(coalesce(opp.isgofficeshowwid, 0)) AS isgofficeshowwid,
            int(coalesce(mb.cornmod, 0)) AS cornersmodifier
           FROM {raw_db}.dw_sales_order_line fsales
             LEFT JOIN ( SELECT sum(
                        CASE
                            WHEN xtd.wxacttypecode = 'regular' AND md.cnrmodflg = 'y' THEN int(fsales1.salesqty) * double(mbr.xadjamt) * 1
                            WHEN xtd.wxacttypecode = 'returns' AND md.cnrmodflg = 'y' THEN int(fsales1.salesqty) * double(mbr.xadjamt) * -1
                            ELSE 0
                        END) AS cornmod,
                    mbr.salesorderwid
                   FROM {raw_db}.dw_sales_order_line fsales1,
                    {raw_db}.dw_xact_type xtd,
                    {raw_db}.dw_modifier_bridge mbr,
                    {raw_db}.dw_modifiers md
                  WHERE fsales1.xacttypewid = xtd.rowwid AND fsales1.rowwid = mbr.salesorderwid AND fsales1.deleteflg = 'n' AND mbr.modifierswid = md.rowwid
                  GROUP BY mbr.salesorderwid) mb ON mb.salesorderwid = fsales.rowwid
             LEFT JOIN {raw_db}.dw_event_ed dim_event_ed ON dim_event_ed.rowwid = fsales.xeventedwid
             JOIN {raw_db}.dw_currency dim_doc_curr ON fsales.doccurrcode = dim_doc_curr.currencycd
             JOIN {raw_db}.dw_currency dim_loc_curr ON fsales.loccurrcode = dim_loc_curr.currencycd
             JOIN {raw_db}.dw_product prod ON fsales.xproductwid = prod.rowwid
             LEFT JOIN {raw_db}.dw_exchg_rate exc ON fsales.loccurrcode = exc.fromcurrency AND exc.tocurrency = 'usd' AND fsales.xorderdate = exc.conversiondate
             LEFT JOIN {raw_db}.dw_isg_revn_category_lkp isg ON isg.minorcategorynumber = prod.minorcategorycode AND isg.majorcategorynumber = prod.majorcategorycode
             LEFT JOIN {raw_db}.dw_isg_revn_category d ON d.isgrevenuecategory = isg.isgrevenuecategory
             LEFT JOIN {raw_db}.dw_opty opp ON opp.rowwid = fsales.xoptywid
             LEFT JOIN {raw_db}.dw_xact_type order_type ON order_type.rowwid = fsales.xacttypewid
             LEFT JOIN ( SELECT DISTINCT u.rowwid AS xuserwid,
                    e.rowwid AS xempwid
                   FROM {raw_db}.dw_employee e,
                    {raw_db}.gbsusers u,
                    {raw_db}.dw_user user_d
                  WHERE u.integrationid = concat('msad_obu_000-usr-', user_d.login) AND (user_d.rowwid = e.xuserwid OR e.login = user_d.login) AND u.rowwid <> '0' AND e.login IS NOT NULL AND e.login <> 'iecaouser'
                EXCEPT
                 SELECT b.xuserwid,
                    b.xempwid
                   FROM ( SELECT count(DISTINCT a.xuserwid) AS xuserwid,
                            a.xempwid
                           FROM ( SELECT DISTINCT u.rowwid AS xuserwid,
                                    e.rowwid AS xempwid
                                   FROM {raw_db}.dw_employee e,
                                    {raw_db}.gbsusers u,
                                    {raw_db}.dw_user user_d
                                  WHERE u.integrationid = concat('msad_obu_000-usr-', user_d.login) AND (user_d.rowwid = e.xuserwid OR e.login = user_d.login) AND u.rowwid <> '0' AND e.login IS NOT NULL AND e.login <> 'iecaouser') a
                          GROUP BY a.xempwid
                         HAVING count(DISTINCT a.xuserwid) > 1) b) emp ON emp.xempwid = fsales.salesrepwid
             LEFT JOIN {raw_db}.dw_status status ON status.rowwid = fsales.orderstatuswid AND status.statuscode NOT LIKE 'draft%'
             LEFT JOIN {raw_db}.dw_customerlocation inv_loc ON inv_loc.rowwid = fsales.customerbilltolocwid AND fsales.customerbilltolocwid <> '0'
             LEFT JOIN {raw_db}.dw_customer_account inv_cust_acc ON inv_cust_acc.rowwid = inv_loc.customerwid
             LEFT JOIN {raw_db}.dw_customer_account contracted_cust_acc ON contracted_cust_acc.rowwid = fsales.customeraccountwid
             LEFT JOIN {raw_db}.dw_customer_account benf_cust_acc ON benf_cust_acc.integrationid = fsales.xbeneficiarytocustomer
             LEFT JOIN ( SELECT DISTINCT a.eventnumericcode,
                    b.friendlycd,
                    b.defaultcurrency
                   FROM {raw_db}.dw_event a,
                    {raw_db}.dw_obu b
                  WHERE a.obuwid = b.rowwid) obu ON fsales.xeventid = obu.eventnumericcode
             LEFT JOIN ( SELECT c.rowwid,
                    c.partorgwid,
                    c.orgwid,
                    c.eventedwid
                   FROM ( SELECT DISTINCT a.rowwid,
                            a.partorgwid,
                            a.orgwid,
                            a.eventedwid,
                            rank() OVER (PARTITION BY a.orgwid, a.eventedwid ORDER BY a.partorgwid, a.rowwid) AS preferredpco
                           FROM {raw_db}.dw_participation a
                          WHERE a.partorgtypewid = '1') c
                  WHERE c.preferredpco = '1') participation_benf ON participation_benf.eventedwid = fsales.xeventedwid AND participation_benf.orgwid = fsales.xbeneficiaryorgwid
             LEFT JOIN ( SELECT d_1.rowwid,
                    d_1.partorgwid,
                    d_1.orgwid,
                    d_1.eventedwid
                   FROM ( SELECT DISTINCT a.rowwid,
                            a.partorgwid,
                            a.orgwid,
                            a.eventedwid,
                            rank() OVER (PARTITION BY a.orgwid, a.eventedwid ORDER BY a.partorgwid, a.rowwid) AS preferredpco
                           FROM {raw_db}.dw_participation a
                          WHERE a.partorgtypewid = '1') d_1
                  WHERE d_1.preferredpco = '1') participation_contr ON participation_contr.eventedwid = fsales.xeventedwid AND participation_contr.orgwid = fsales.xcontractorgwid
             LEFT JOIN ( SELECT DISTINCT a.eventedwid,
                    a.orgwid,
                        CASE
                            WHEN c.parentpartintegationid = '0' THEN 1
                            ELSE 0
                        END AS exhibitorflag,
                        CASE
                            WHEN c.parentpartintegationid <> '0' THEN 1
                            ELSE 0
                        END AS sharerflag
                   FROM {raw_db}.dw_participation a,
                    {raw_db}.dw_participatingorgtype b,
                    {raw_db}.dw_participatingorg c
                  WHERE a.partorgtypewid = b.rowwid AND a.partorgwid = c.rowwid AND a.partorgwid IS NOT NULL AND a.partorgwid <> '0' AND lower(b.description) = 'exhibiting organisation'
                EXCEPT
                 SELECT DISTINCT f.eventedwid,
                    f.orgwid,
                    0 AS exhibitorflag,
                    1 AS sharerflag
                   FROM ( SELECT e.eventedwid,
                            e.orgwid
                           FROM ( SELECT DISTINCT a.eventedwid,
                                    a.orgwid,
                                        CASE
                                            WHEN c.parentpartintegationid = '0' THEN 1
                                            ELSE 0
                                        END AS exhibitorflag,
                                        CASE
                                            WHEN c.parentpartintegationid <> '0' THEN 1
                                            ELSE 0
                                        END AS sharerflag
                                   FROM {raw_db}.dw_participation a,
                                    {raw_db}.dw_participatingorgtype b,
                                    {raw_db}.dw_participatingorg c
                                  WHERE a.partorgtypewid = b.rowwid AND a.partorgwid = c.rowwid AND a.partorgwid IS NOT NULL AND a.partorgwid <> '0' AND lower(b.description) = 'exhibiting organisation') e
                          GROUP BY e.eventedwid, e.orgwid
                         HAVING count(1) > 1) f) prcpt_org ON prcpt_org.eventedwid = fsales.xeventedwid AND prcpt_org.orgwid = fsales.xbeneficiaryorgwid
             LEFT JOIN ( SELECT DISTINCT t1.xeventedwid,
                    t1.xcontractorgwid
                   FROM {raw_db}.dw_sales_order_line t1,
                    {raw_db}.dw_event_ed t2,
                    {raw_db}.dw_opty t3,
                    {raw_db}.dw_sales_order_line t4,
                    {raw_db}.dw_opty t5,
                    {raw_db}.dw_sales_order_line t6,
                    {raw_db}.dw_opty t7
                  WHERE t1.xeventedwid = t2.rowwid AND t1.xoptywid = t3.rowwid AND t3.customertype = 'rebooker' AND t1.xcontractorgwid IS NOT NULL AND t4.xeventedwid = t2.previouseventedwid AND t4.xoptywid = t5.rowwid AND t5.customertype = 'rebooker' AND t4.xcontractorgwid = t1.xcontractorgwid AND t6.xeventedwid = t2.prevpreveventedwid AND t6.xoptywid = t7.rowwid AND (t7.customertype IN ('rebooker', 'first time rebooker')) AND t6.xcontractorgwid = t1.xcontractorgwid) ret ON ret.xeventedwid = fsales.xeventedwid AND ret.xcontractorgwid = fsales.xcontractorgwid
             LEFT JOIN {raw_db}.dw_uom_conversion sqf_conv ON fsales.salesuomcode = sqf_conv.wfromuomcode AND sqf_conv.wtouomcode = 'f2'
             LEFT JOIN {raw_db}.dw_uom_conversion sqm_conv ON fsales.salesuomcode = sqm_conv.wfromuomcode AND sqm_conv.wtouomcode = 'm2'
    """)

    return data_df


def mv_profile_response_data_transform(sparksession, data_df, raw_db, refined_db):
    print('GETTING data from gbs_refinedzone ...')
    # sparksession.catalog.setCurrentDatabase(refined_db)
    mv_df = sparksession.sql(f"""
        SELECT t1.rowwid,
            t1.profileanswerwid,
            t1.datasourcecode,
            t1.createdondt,
            t1.changedondt,
            t1.integrationid,
            t1.winsertdt,
            t1.wupdatedt,
            t1.createdby,
            t1.changedby,
            t1.answereddate,
            t1.datasourcenumid,
            t1.etlprocnum,
            t1.deleteflag,
            t1.responsedt,
            t1.latestevprfrespflg,
            t4.eventwid,
            t5.regwid,
            t6.contactwid,
            t7.leadwid,
            t8.organisationwid,
            t9.personwid,
            t10.pcowid,
            t11.pinwid
        FROM {raw_db}.profile_response t1
        JOIN {raw_db}.profile_answer t2 ON t1.profileanswerwid = t2.rowwid
        JOIN {raw_db}.profile_question t3 ON t3.rowwid = t2.profilequestionwid
        JOIN {raw_db}.profile_master t4 ON t3.profilewid = t4.rowwid
        LEFT JOIN {raw_db}.profile_response_registration t5 ON t5.profileresponsewid = t1.rowwid
        LEFT JOIN {raw_db}.profile_response_contact t6 ON t6.profileresponsewid = t1.rowwid
        LEFT JOIN {raw_db}.profile_response_lead t7 ON t7.profileresponsewid = t1.rowwid
        LEFT JOIN {raw_db}.profile_response_organization t8 ON t8.profileresponsewid = t1.rowwid
        LEFT JOIN {raw_db}.profile_response_person t9 ON t9.integrationid = t1.integrationid
        LEFT JOIN {raw_db}.profile_response_pco t10 ON t10.profileresponsewid = t1.rowwid
        LEFT JOIN {raw_db}.profile_response_pin t11 ON t11.profileresponsewid = t1.rowwid
    """)

    return mv_df
