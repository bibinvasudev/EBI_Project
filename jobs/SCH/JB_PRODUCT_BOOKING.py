# SCH1010.py  --> jb_product_booking.py

# **************************************************************************************************************
#
# Created by  : Vinay Kumbakonam
# Modified by : bibin
# Version      : 1.2
#
# Description  :
#
#
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-10-23                    Initial creation
# 2018-10-30                    bibin : Getting DB schema, db_prop_key_load from Config file, log DIR
# 2018-11-02                    bibin : Using job_debugger_print() for print any string in Job
#
# **************************************************************************************************************


# Importing required Lib
# To Add all the Import libraries
# Importing required Lib
from dependencies.spark import start_spark
from dependencies.EbiReadWrite import EbiReadWrite
import logging
import sys
from time import gmtime, strftime
import cx_Oracle
import py4j
import pyspark

# Spark logging
logger = logging.getLogger(__name__)

# Date Formats
start_date = "'" + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"
log_date = strftime("%Y%m%d", gmtime())

# Job Naming Details
# Need to have job and app name to represent the actual task/etl being performed Ex: "SCH_PRODUCT_BOOKING_LOAD.PY"
script_name = "SCH1101.SH"
app_name = 'JB_PRODUCT_BOOKING'
log_filename = app_name + '_' + log_date + '.log'


# Query for Extract data
# pass the schema as "T2_SALES_CHANNEL"
# also add the list of columns as per the specified
# Need to find out why query block was used and how parallelism of 6 was arrived at
def query_data(db_schema, Param_fiscal_month, Param_fiscal_year_quarter):

    query = """INSERT INTO """ + db_schema + """.PRODUCT_BOOKING_REPORT (
        USD_EXTENDED_LIST_PRICE,	USD_BOOKING_AMOUNT,	SO_BOOKING_AMOUNT,	NUMBER_OF_DRIVES_QTY,	DISCOUNT_PERC,	CONTROLLER_QTY,	BOOKING_QTY,	BILL_TO_CUSTOMER_COUNTRY_CODE,	BILL_TO_CUSTOMER_NAGP,	BILL_TO_CUST_PARENT_COMPANY,	CALENDAR_MONTH,	CALENDAR_YEAR,	CHANNEL_TYPE,	DISTRIBUTOR_COMPANY_NAME,	DISTRIBUTOR_SITE_COUNTRY_NAME,	EC_NAGP_NETAPP_VERTICAL,	FISCAL_MONTH,	FISCAL_QTR,	FISCAL_YEAR,	OPPORTUNITY_END_CUSTOMER,	SFDC_OPPORTUNITY_NUMBER,	SFDC_VALUE_ADD_PARTNER1_ROLE,	SFDC_VALUE_ADD_PARTNER2_NAME,	SFDC_VALUE_ADD_PARTNER1_NAME,	SFDC_VALUE_ADD_PARTNER2_ROLE,	PART_NUMBER,	PRODUCT_CATEGORY,	PRODUCT_FAMILY,	PRODUCT_LINE,	PRODUCT_TYPE,	HUNDRED_PCT_DISCOUNTING_REASON,	PVR_APPROVED_DATE,	PVR_EXCEPTION_PROMO,	PVR_REASON,	PVR_USED_FLAG,	QUOTE_BOOKED_DATE,	QUOTE_COMMENTS,	SO_CURRENCY_CODE,	BASE_BOOKING_FLAG,	BOOKED_DATE,	SALES_ORDER_CUSTOMER_PO_NUMBER,	SALES_ORDER_NUMBER,	SALES_ORDER_QUOTE_NUMBER,	ORDER_LINE_NUMBER,	PRODUCT_GROUPING_CODE,	PRODUCT_PARENT_CODE,	PROOF_OF_DELIVERY_DATE,	SALES_ORDER_LINE_INVOICE_DATE,	SHIPPED_DATE,	SALES_REP_NAME,	SHIP_TO_CUSTOMER_COUNTRY_CODE,	SHIP_TO_CUSTOMER_NAGP,	SHIP_TO_CUSTOMER_CITY,	SHIP_TO_COUNTRY,	SOLD_TO_CUSTOMER_NAGP,	SOLD_TO_PARTNER_COMPANY_NAME,	SOLD_TO_PARTNER_LEVEL,	DISTRIBUTOR_NAGP_NAME,	SOLD_TO_PARTNER_NAGP_NAME,	VALUE_ADD_PARTNER_NAGP_NAME,	VAL_ADD_PART_SITE_COUNTRY_NAME,	VALUE_ADD_PARTNER2_NAGP_NAME,	VAL_ADD_PART2_SITE_COUN_NAME,	PATHWAY_TYPE,	SOLD_TO_PART_SITE_COUN_NAME,	HUNDRED_PCT_PVR)
        WITH ORG_PART AS (
        SELECT /*+ qb_name(q1block) */
        ORGANIZATION_PARTY_KEY,
                PROTECTED_NAGP_NAME,
                NAGP_NAME,
                PROTECTED_COMPANY_NAME,
                PROTECTED_DOMESTIC_PARENT_NAME,
                DP_COMMON_ID,
                PROTECTION_SUBCLASS_KEY,
                LINE_1_ADDRESS,
                LINE_2_ADDRESS,
                LINE_3_ADDRESS,
                LINE_4_ADDRESS,
                CITY_NAME,
                ISO_COUNTRY_CODE,
                POSTAL_CODE,
                GEOGRAPHY_HIERARCHY_KEY,
                ACTIVE_COMPANY_NAME,
                SITE_COUNTRY_NAME,
                NAGP_NETAPP_VERTICAL_NAME
           FROM DIMS.ORGANIZATION_PARTY A
                )
               select /*+ FULL(@q1block A) PARALLEL(@q1block A,6) */ SUM (T762157.ME_EXT_LIST_PRICE) AS USD_EXTENDED_LIST_PRICE,
         SUM (T762157.ME_EXT_SALE_PRICE) AS USD_BOOKING_AMOUNT,
         SUM (T762157.TC_EXT_SALE_PRICE) AS SO_BOOKING_AMOUNT,
         SUM (T761675.NUMBER_OF_DRIVES * T762157.QUANTITY)
            AS NUMBER_OF_DRIVES_QTY,
         SUM (T762157.ME_EXT_DISCOUNT_AMT) AS DISCOUNT_PERC,
         SUM (T761675.CONTROLLER_COUNT * T762157.QUANTITY) AS CONTROLLER_QTY,
         SUM (T762157.QUANTITY) AS BOOKING_QTY,
         T762641.ISO_COUNTRY_CODE AS BILL_TO_CUSTOMER_COUNTRY_CODE,
         T762641.NAGP_NAME AS BILL_TO_CUSTOMER_NAGP,
         T762641.ACTIVE_COMPANY_NAME AS BILL_TO_CUST_PARENT_COMPANY,
         T763102.CALENDAR_YEAR_MONTH_TEXT AS CALENDAR_MONTH,
         T763102.CALENDAR_YEAR_LONG_TEXT AS CALENDAR_YEAR,
         T760943.SALES_CHANNEL_TYPE_DESCRIPTION AS CHANNEL_TYPE,
         T762683.ACTIVE_COMPANY_NAME AS DISTRIBUTOR_COMPANY_NAME,
         T762683.SITE_COUNTRY_NAME AS DISTRIBUTOR_SITE_COUNTRY_NAME,
         T762895.NAGP_NETAPP_VERTICAL_NAME AS EC_NAGP_NETAPP_VERTICAL,
         T763102.FISCAL_YEAR_MONTH_TEXT AS FISCAL_MONTH,
         T763102.FISCAL_YEAR_QTR_TEXT AS FISCAL_QTR,
         T763102.FISCAL_YEAR_LONG_TEXT AS FISCAL_YEAR,
         T761496.OPTY_END_CUSTOMER_NAME AS OPPORTUNITY_END_CUSTOMER,
         T761496.OPPORTUNITY_NUMBER__C AS SFDC_OPPORTUNITY_NUMBER,
         T761496.VAP1_ROLE AS SFDC_VALUE_ADD_PARTNER1_ROLE,
         T761496.SFDC_VALUE_ADD_PARTNER_2 AS SFDC_VALUE_ADD_PARTNER2_NAME,
         T761496.SFDC_VALUE_ADD_PARTNER_1 AS SFDC_VALUE_ADD_PARTNER1_NAME,
         T761496.VAP2_ROLE AS SFDC_VALUE_ADD_PARTNER2_ROLE,
         T771192.BK_PART_NUMBER AS PART_NUMBER,
         T771192.PRODUCT_CATEGORY_NAME AS PRODUCT_CATEGORY,
         T771192.PRODUCT_FAMILY_NAME AS PRODUCT_FAMILY,
         T771192.PRODUCT_LINE_NAME AS PRODUCT_LINE,
         T771192.PRODUCT_TYPE_NAME AS PRODUCT_TYPE,
         T762071.HUNDRED_PCT_DISC_REASON_NAME AS HUNDRED_PCT_DISCOUNTING_REASON,
         T762071.PVR_APPROVED_DATE AS PVR_APPROVED_DATE,
         T762071.PVR_EXCEPTION_PROMO_CODE AS PVR_EXCEPTION_PROMO,
         T762071.PVR_REASON_TEXT AS PVR_REASON,
         T762071.PVR_USED_FLAG AS PVR_USED_FLAG,
         T762071.QUOTE_BOOKED_DATE AS QUOTE_BOOKED_DATE,
         T762071.COMMENTS AS QUOTE_COMMENTS,
         T763622.BK_ISO_CURRENCY_CODE AS SO_CURRENCY_CODE,
         T705584.ATTRIBUTE_CODE_DESCRIPTION AS BASE_BOOKING_FLAG,
         T762251.BOOKED_DATE AS BOOKED_DATE,
         T762251.PURCHASE_ORDER_NUMBER AS SALES_ORDER_CUSTOMER_PO_NUMBER,
         T762251.BK_SALES_ORDER_NUMBER AS SALES_ORDER_NUMBER,
         T762251.QUOTE_NUMBER AS SALES_ORDER_QUOTE_NUMBER,
         T762300.BK_SALES_ORDER_LINE_NUMBER AS ORDER_LINE_NUMBER,
         T770644.PRODUCT_GROUPING_CODE AS PRODUCT_GROUPING_CODE,
         T770644.PRODUCT_PARENT_CODE AS PRODUCT_PARENT_CODE,
         T762300.PROOF_OF_DELIVERY_DATE AS PROOF_OF_DELIVERY_DATE,
         T762300.INVOICE_DATE AS SALES_ORDER_LINE_INVOICE_DATE,
         T762300.SHIPPED_DATE AS SHIPPED_DATE,
         T762438.SALES_REP_NAME AS SALES_REP_NAME,
         T762725.ISO_COUNTRY_CODE AS SHIP_TO_CUSTOMER_COUNTRY_CODE,
         T762725.NAGP_NAME AS SHIP_TO_CUSTOMER_NAGP,
         T762725.CITY_NAME AS SHIP_TO_CUSTOMER_CITY,
         T763529.COUNTRY_NAME AS SHIP_TO_COUNTRY,
         T762769.NAGP_NAME AS SOLD_TO_CUSTOMER_NAGP,
         T762769.ACTIVE_COMPANY_NAME AS SOLD_TO_PARTNER_COMPANY_NAME,
         T779975.PARTNER_LEVEL_NAME AS SOLD_TO_PARTNER_LEVEL,
         T762683.NAGP_NAME AS DISTRIBUTOR_NAGP_NAME,
         T762811.NAGP_NAME AS SOLD_TO_PARTNER_NAGP_NAME,
         T762853.NAGP_NAME AS VALUE_ADD_PARTNER_NAGP_NAME,
         T762853.SITE_COUNTRY_NAME AS VAL_ADD_PART_SITE_COUNTRY_NAME,
         T780490.NAGP_NAME AS VALUE_ADD_PARTNER2_NAGP_NAME,
         T780490.SITE_COUNTRY_NAME AS VAL_ADD_PART2_SITE_COUN_NAME,
         T765610.BK_PATHWAY_TYPE_CODE AS PATHWAY_TYPE,
         T762811.SITE_COUNTRY_NAME AS SOLD_TO_PART_SITE_COUN_NAME,
         0 AS HUNDRED_PCT_PVR
    FROM DIMS.PARTNER_BUSINESS_MODEL T779975  /* PARTNER_BUSINESS_MODEL_STP */
                                            ,
         ORG_PART T780490                                 /* ORG_PARTY_VAP2 */
                         ,
         DIMS.PRODUCT T771192                             /* PRODUCT_MODULE */
                             ,
         ORG_PART T762725               /* ORG_PART, ORG_PARTY_SHIP_TO_CUST */
                         ,
         DIMS.DEAL_PATHWAY T765610,
         DIMS.QUOTE T762071,
         DIMS.SALES_PARTICIPANT T762438,
         DIMS.SALES_ORDER_LINE T762300,
         DIMS.SALES_ORDER T762251,
         DIMS.ISO_CURRENCY T763622                    /* ISO_CURRENCY_MISC1 */
                                  ,
         DIMS.OPPORTUNITY T761496,
         DIMS.ENTERPRISE_LIST_OF_VALUES T705584            /* LOV_CORP_FLAG */
                                               ,
         ORG_PART T762683                          /* ORG_PARTY_DISTRIBUTOR */
                         ,
         ORG_PART T762853                       /* ORG_PARTY_VALUE_ADD_PART */
                         ,
         ORG_PART T762811                         /* ORG_PARTY_SOLD_TO_PART */
                         ,
         ORG_PART T762769                         /* ORG_PARTY_SOLD_TO_CUST */
                         ,
         ORG_PART T762641                         /* ORG_PARTY_BILL_TO_CUST */
                         ,
         ORG_PART T762895                             /* ORG_PARTY_AOO_CUST */
                         ,
         DIMS.CUSTOMER_GEOGRAPHY_HIERARCHY T763529 /* CUSTOMER_GEO_SHIP_TO_HIERARCHY */
                                                  ,
         DIMS.CALENDAR T763102                           /* CALENDAR_FISCAL */
                              ,
         DIMS.PRODUCT T761675,
         FACTS.SALES_BOOKING T762157,
         DIMS.CHANNEL_TYPE T760943,
         DIMS.SALES_ORDER_LINE_DD T769152,
         DIMS.DERIVED_PRODUCT T770644
   WHERE (    T762157.MODULE_PART_NUMBER_KEY = T771192.PRODUCT_KEY
          AND T762157.DISTRIBUTOR_AS_IS_KEY = T762683.ORGANIZATION_PARTY_KEY
          AND T762157.SOLDTO_PRTNR_MODEL_AS_IS_KEY =
                 T779975.PARTNER_BUSINESS_MODEL_KEY
          AND T762157.SOLDTO_PARTNER_AS_IS_KEY = T762811.ORGANIZATION_PARTY_KEY
          AND T762157.SOLDTO_CUSTOMER_AS_IS_KEY =
                 T762769.ORGANIZATION_PARTY_KEY
          AND T762157.VALUE_ADD_PARTNER2_AS_IS_KEY =
                 T780490.ORGANIZATION_PARTY_KEY
          AND T762157.BILLTO_CUSTOMER_AS_IS_KEY =
                 T762641.ORGANIZATION_PARTY_KEY
          AND T762157.AOO_CUSTOMER_AS_IS_KEY = T762895.ORGANIZATION_PARTY_KEY
          AND T762157.PATHWAY_KEY = T765610.PATHWAY_KEY
          AND T762071.QUOTE_KEY = T762157.QUOTE_KEY
          AND T762157.SALES_REP_AS_IS_KEY = T762438.SALES_PARTICIPANT_KEY
          AND T762157.SALES_ORDER_LINE_KEY = T762300.SALES_ORDER_LINE_KEY
          AND T762300.SALES_ORDER_LINE_KEY = T769152.SALES_ORDER_LINE_KEY
          AND T762157.SALES_ORDER_KEY = T762251.SALES_ORDER_KEY
          AND T762157.TC_TRX_CURR_CODE_KEY = T763622.ISO_CURRENCY_KEY
          AND T761496.OPPORTUNITY_KEY = T762157.OPPORTUNITY_KEY
          AND T705584.ATTRIBUTE_CODE = 'CORP FLAG'
          AND T705584.ATTRIBUTE_CODE_VALUE = T762157.CORP_FLAG
          AND T762157.SHIPTO_CUSTOMER_AS_IS_KEY =
                 T762725.ORGANIZATION_PARTY_KEY
          AND T762725.GEOGRAPHY_HIERARCHY_KEY = T763529.GEOGRAPHY_HIERARCHY_KEY
          AND T762157.SOURCE_TRANSACTION_DATE_KEY = T763102.DATE_KEY
          AND T761675.PRODUCT_KEY = T762157.PRODUCT_AS_IS_KEY
          AND T760943.SALES_CHANNEL_TYPE_KEY = T762251.SALES_CHANNEL_TYPE_KEY
          AND T762157.SALES_ORDER_LINE_KEY = T769152.SALES_ORDER_LINE_KEY
          AND T762157.VALUE_ADD_PARTNER_AS_IS_KEY =
                 T762853.ORGANIZATION_PARTY_KEY
          AND T705584.ATTRIBUTE_CODE_DESCRIPTION = 'Y'
          AND T769152.DERIVED_PRODUCT_KEY = T770644.DERIVED_PRODUCT_KEY
          AND T763102.FISCAL_YEAR_QTR_TEXT in (""" + Param_fiscal_year_quarter + """)
GROUP BY T762641.ISO_COUNTRY_CODE,
         T762641.NAGP_NAME,
         T762641.ACTIVE_COMPANY_NAME,
         T763102.CALENDAR_YEAR_MONTH_TEXT,
         T763102.CALENDAR_YEAR_LONG_TEXT,
         T760943.SALES_CHANNEL_TYPE_DESCRIPTION,
         T762683.ACTIVE_COMPANY_NAME,
         T762683.SITE_COUNTRY_NAME,
         T762895.NAGP_NETAPP_VERTICAL_NAME,
         T763102.FISCAL_YEAR_MONTH_TEXT,
         T763102.FISCAL_YEAR_QTR_TEXT,
         T763102.FISCAL_YEAR_LONG_TEXT,
         T761496.OPTY_END_CUSTOMER_NAME,
         T761496.OPPORTUNITY_NUMBER__C,
         T761496.VAP1_ROLE,
         T761496.SFDC_VALUE_ADD_PARTNER_2,
         T761496.SFDC_VALUE_ADD_PARTNER_1,
         T761496.VAP2_ROLE,
         T771192.BK_PART_NUMBER,
         T771192.PRODUCT_CATEGORY_NAME,
         T771192.PRODUCT_FAMILY_NAME,
         T771192.PRODUCT_LINE_NAME,
         T771192.PRODUCT_TYPE_NAME,
         T762071.HUNDRED_PCT_DISC_REASON_NAME,
         T762071.PVR_APPROVED_DATE,
         T762071.PVR_EXCEPTION_PROMO_CODE,
         T762071.PVR_REASON_TEXT,
         T762071.PVR_USED_FLAG,
         T762071.QUOTE_BOOKED_DATE,
         T762071.COMMENTS,
         T763622.BK_ISO_CURRENCY_CODE,
         T705584.ATTRIBUTE_CODE_DESCRIPTION,
         T762251.BOOKED_DATE,
         T762251.PURCHASE_ORDER_NUMBER,
         T762251.BK_SALES_ORDER_NUMBER,
         T762251.QUOTE_NUMBER,
         T762300.BK_SALES_ORDER_LINE_NUMBER,
         T770644.PRODUCT_GROUPING_CODE,
         T770644.PRODUCT_PARENT_CODE,
         T762300.PROOF_OF_DELIVERY_DATE,
         T762300.INVOICE_DATE,
         T762300.SHIPPED_DATE,
         T762438.SALES_REP_NAME,
         T762725.ISO_COUNTRY_CODE,
         T762725.NAGP_NAME,
         T762725.CITY_NAME,
         T763529.COUNTRY_NAME,
         T762769.NAGP_NAME,
         T762769.ACTIVE_COMPANY_NAME,
         T779975.PARTNER_LEVEL_NAME,
         T762683.NAGP_NAME,
         T762811.NAGP_NAME,
         T762853.NAGP_NAME,
         T762853.SITE_COUNTRY_NAME,
         T780490.NAGP_NAME,
         T780490.SITE_COUNTRY_NAME,
         T765610.BK_PATHWAY_TYPE_CODE,
         T762811.SITE_COUNTRY_NAME """

    return query


#  Main method
def main():
    try:
        src_count = '0'
        dest_count = '0'
        """Main ETL script definition.
        :return: None
        """

        # start Spark application and get Spark session, logger and config
        spark, config = start_spark(app_name=app_name)

        # Create class Object
        Ebi_read_write_obj = EbiReadWrite(app_name, spark, config, logger)


        log_file = config['LOG_DIR_NAME'] + "/" + log_filename
        # DB prop Key of Source DB
        db_prop_key_extract = config['DB_PROP_KEY_EXTRACT']
        db_prop_key_load = config['DB_PROP_KEY_LOAD']
        db_schema = config['DB_SCHEMA']

        param_fiscal_year_quarter = Ebi_read_write_obj.get_fiscal_year_quarter(db_schema, db_prop_key_extract)
        param_fiscal_month = Ebi_read_write_obj.get_fiscal_month(db_schema, db_prop_key_extract)
        # SQL Query
        query = query_data(db_schema, param_fiscal_month, param_fiscal_year_quarter)
        # print(query)
        # log = logging.getLogger(app_name)

        # Calling Job Class method --> get_target_data_update()
        Ebi_read_write_obj.get_target_data_update(query, db_prop_key_load)

        end_date = "'" + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"

        data_format = "JOB START DT : " + start_date + " | SCRIPT NAME : " + script_name + " | JOB : " + app_name + " | SRC COUNT : " + src_count + " | TGT COUNT : " + dest_count + " | JOB END DT : " + end_date + " | STATUS : %(message)s"

        Ebi_read_write_obj.create_log(data_format, log_file, logger)

        logger.info("Success")
        Ebi_read_write_obj.job_debugger_print("	\n  __main__ " + app_name + " --> Job " + app_name + " Succeed \n")

    except Exception as err:
        # Write expeption in spark log or console
        end_date = "'" + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"
        data_format = "JOB START DT : " + start_date + " | SCRIPT NAME : " + script_name + " | JOB : " + app_name + " | SRC COUNT : " + src_count + " | TGT COUNT : " + dest_count + " | JOB END DT : " + end_date + " | STATUS : %(message)s"
        Ebi_read_write_obj.create_log(data_format, log_file, logger)
        logger.info("[Error] Failed")
        Ebi_read_write_obj.job_debugger_print("     \n Job " + app_name + " Failed\n")
        logger.error("\n __main__ " + app_name + " --> Exception-Traceback :: " + str(err))
        raise

# Entry point for script
if __name__ == "__main__":
    # Calling main() method
    main()

