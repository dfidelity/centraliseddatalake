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


def refined_data_transform(sparksession, source, table):

    print("refined_data_transform step")

    option = source + '_' + table

    options = {
        'mv_salesorder_line': mv_salesorder_line_data_transform,
        'mv_profile_response': mv_profile_response_data_transform
    }

    if option in options:
        return options[option](sparksession)

    return -1


def mv_salesorder_line_data_transform(sparksession):
    sparksession.catalog.setCurrentDatabase("gbs_refinedzone")
    # data_df = sparksession.sql("select * from dw_sales_order_line")
    data_df = sparksession.sql("""
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
            int(coalesce(dimdoccurr.rowwid, 0)) AS doccurrencywid,
            int(coalesce(dimloccurr.rowwid, 0)) AS loccurrecywid,
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
           FROM dw_sales_order_line fsales
             LEFT JOIN ( SELECT sum(
                        CASE
                            WHEN xtd.wxacttypecode = 'regular' AND md.cnrmodflg = 'y' THEN int(fsales1.salesqty) * double(mbr.xadjamt) * 1
                            WHEN xtd.wxacttypecode = 'returns' AND md.cnrmodflg = 'y' THEN int(fsales1.salesqty) * double(mbr.xadjamt) * -1
                            ELSE 0
                        END) AS cornmod,
                    mbr.salesorderwid
                   FROM dw_sales_order_line fsales1,
                    dw_xact_type xtd,
                    dw_modifier_bridge mbr,
                    dw_modifiers md
                  WHERE fsales1.xacttypewid = xtd.rowwid AND fsales1.rowwid = mbr.salesorderwid AND fsales1.deleteflg = 'n' AND mbr.modifierswid = md.rowwid
                  GROUP BY mbr.salesorderwid) mb ON mb.salesorderwid = fsales.rowwid
             LEFT JOIN dw_event_ed dim_event_ed ON dim_event_ed.rowwid = fsales.xeventedwid
             JOIN dw_currency dim_doc_curr ON fsales.doccurrcode = dim_doc_curr.currencycd
             JOIN dw_currency dim_loc_curr ON fsales.loccurrcode = dim_loc_curr.currencycd
             JOIN dw_product prod ON fsales.xproductwid = prod.rowwid
             LEFT JOIN dw_exchg_rate exc ON fsales.loccurrcode = exc.fromcurrency AND exc.tocurrency = 'usd' AND fsales.xorderdate = exc.conversiondate
             LEFT JOIN dw_isg_revn_category_lkp isg ON isg.minorcategorynumber = prod.minorcategorycode AND isg.majorcategorynumber = prod.majorcategorycode
             LEFT JOIN dw_isg_revn_category d ON d.isgrevenuecategory = isg.isgrevenuecategory
             LEFT JOIN dw_opty opp ON opp.rowwid = fsales.xoptywid
             LEFT JOIN dw_xact_type order_type ON order_type.rowwid = fsales.xacttypewid
             LEFT JOIN ( SELECT DISTINCT u.rowwid AS xuserwid,
                    e.rowwid AS xempwid
                   FROM dw_employee e,
                    gbsusers u,
                    dw_user user_d
                  WHERE u.integrationid = concat('msad_obu_000-usr-', user_d.login) AND (user_d.rowwid = e.xuserwid OR e.login = user_d.login) AND u.rowwid <> '0' AND e.login IS NOT NULL AND e.login <> 'iecaouser'
                EXCEPT
                 SELECT b.xuserwid,
                    b.xempwid
                   FROM ( SELECT count(DISTINCT a.xuserwid) AS xuserwid,
                            a.xempwid
                           FROM ( SELECT DISTINCT u.rowwid AS xuserwid,
                                    e.rowwid AS xempwid
                                   FROM dw_employee e,
                                    gbsusers u,
                                    dw_user user_d
                                  WHERE u.integrationid = concat('msad_obu_000-usr-', user_d.login) AND (user_d.rowwid = e.xuserwid OR e.login = user_d.login) AND u.rowwid <> '0' AND e.login IS NOT NULL AND e.login <> 'iecaouser') a
                          GROUP BY a.xempwid
                         HAVING count(DISTINCT a.xuserwid) > 1) b) emp ON emp.xempwid = fsales.salesrepwid
             LEFT JOIN dw_status status ON status.rowwid = fsales.orderstatuswid AND status.statuscode NOT LIKE 'draft%'
             LEFT JOIN dw_customerlocation inv_loc ON inv_loc.rowwid = fsales.customerbilltolocwid AND fsales.customerbilltolocwid <> '0'
             LEFT JOIN dw_customer_account inv_cust_acc ON inv_cust_acc.rowwid = inv_loc.customerwid
             LEFT JOIN dw_customer_account contracted_cust_acc ON contracted_cust_acc.rowwid = fsales.customeraccountwid
             LEFT JOIN dw_customer_account benf_cust_acc ON benf_cust_acc.integrationid = fsales.xbeneficiarytocustomer
             LEFT JOIN ( SELECT DISTINCT a.eventnumericcode,
                    b.friendlycd,
                    b.defaultcurrency
                   FROM dw_event a,
                    dw_obu b
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
                           FROM dw_participation a
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
                           FROM dw_participation a
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
                   FROM dw_participation a,
                    dw_participatingorgtype b,
                    dw_participatingorg c
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
                                   FROM dw_participation a,
                                    dw_participatingorgtype b,
                                    dw_participatingorg c
                                  WHERE a.partorgtypewid = b.rowwid AND a.partorgwid = c.rowwid AND a.partorgwid IS NOT NULL AND a.partorgwid <> '0' AND lower(b.description) = 'exhibiting organisation') e
                          GROUP BY e.eventedwid, e.orgwid
                         HAVING count(1) > 1) f) prcpt_org ON prcpt_org.eventedwid = fsales.xeventedwid AND prcpt_org.orgwid = fsales.xbeneficiaryorgwid
             LEFT JOIN ( SELECT DISTINCT t1.xeventedwid,
                    t1.xcontractorgwid
                   FROM dw_sales_order_line t1,
                    dw_event_ed t2,
                    dw_opty t3,
                    dw_sales_order_line t4,
                    dw_opty t5,
                    dw_sales_order_line t6,
                    dw_opty t7
                  WHERE t1.xeventedwid = t2.rowwid AND t1.xoptywid = t3.rowwid AND t3.customertype = 'rebooker' AND t1.xcontractorgwid IS NOT NULL AND t4.xeventedwid = t2.previouseventedwid AND t4.xoptywid = t5.rowwid AND t5.customertype = 'rebooker' AND t4.xcontractorgwid = t1.xcontractorgwid AND t6.xeventedwid = t2.prevpreveventedwid AND t6.xoptywid = t7.rowwid AND (t7.customertype IN ('rebooker', 'first time rebooker')) AND t6.xcontractorgwid = t1.xcontractorgwid) ret ON ret.xeventedwid = fsales.xeventedwid AND ret.xcontractorgwid = fsales.xcontractorgwid
             LEFT JOIN dw_uom_conversion sqf_conv ON fsales.salesuomcode = sqf_conv.wfromuomcode AND sqf_conv.wtouomcode = 'f2'
             LEFT JOIN dw_uom_conversion sqm_conv ON fsales.salesuomcode = sqm_conv.wfromuomcode AND sqm_conv.wtouomcode = 'm2'
    """)

    return data_df


def mv_profile_response_data_transform(sparksession, data_df):
    print('GETTING data from gbs_refinedzone ...')
    sparksession.catalog.setCurrentDatabase("gbs_refinedzone")
    mv_df = sparksession.sql("""
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
        FROM profile_response t1
        JOIN profile_answer t2 ON t1.profileanswerwid = t2.rowwid
        JOIN profile_question t3 ON t3.rowwid = t2.profilequestionwid
        JOIN profile_master t4 ON t3.profilewid = t4.rowwid
        LEFT JOIN profile_response_registration t5 ON t5.profileresponsewid = t1.rowwid
        LEFT JOIN profile_response_contact t6 ON t6.profileresponsewid = t1.rowwid
        LEFT JOIN profile_response_lead t7 ON t7.profileresponsewid = t1.rowwid
        LEFT JOIN profile_response_organization t8 ON t8.profileresponsewid = t1.rowwid
        LEFT JOIN profile_response_person t9 ON t9.integrationid = t1.integrationid
        LEFT JOIN profile_response_pco t10 ON t10.profileresponsewid = t1.rowwid
        LEFT JOIN profile_response_pin t11 ON t11.profileresponsewid = t1.rowwid
    """)

    return mv_df

