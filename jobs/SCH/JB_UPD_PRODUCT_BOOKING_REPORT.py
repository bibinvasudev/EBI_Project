# SCH1160.sh  --> JB_UPD_PRODUCT_BOOKING_REPORT.py

# **************************************************************************************************************
#
# Programmer   : Vijay Nagappa
# Version      : 1.1
#
# Description  :
#        1. Updates the table 'Product_Booking_Report' table based on Oracle procedure
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-10-31                    Initial creation
# 2018-11-02                    bibin : Using job_debugger_print() for print any string in Job
#
# **************************************************************************************************************


# Importing required Lib
from dependencies.spark import start_spark
from dependencies.EbiReadWrite import EbiReadWrite
import sys
from time import gmtime, strftime
import cx_Oracle
import py4j
import logging

# Spark logging
logger = logging.getLogger(__name__)

# Date Formats
start_date = "'" + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"
log_date = strftime("%Y%m%d", gmtime())

# Job Naming Details
# Need to have job and app name to represent the actual task/etl being performed Ex: "SCH_PRODUCT_BOOKING_LOAD.PY"
script_name = "SCH1160.SH"
app_name = 'JB_UPD_PRODUCT_BOOKING_REPORT'
log_filename = app_name + '_' + log_date + '.log'

# Target Table Details
write_table = 'PRODUCT_BOOKING_REPORT'

# Oracle write Details
save_mode = 'append'

#Oracle Properties
use_header = 'true'
infer_schema = 'true'

query = """(WITH ORG_PART AS (
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
           FROM DIMS.ORGANIZATION_PARTY A)
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
   WHERE T762157.MODULE_PART_NUMBER_KEY = T771192.PRODUCT_KEY
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
          AND T763102.FISCAL_YEAR_QTR_TEXT = '2019Q2'
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
         T762811.SITE_COUNTRY_NAME) """

lkp_query = """(SELECT T762683.SITE_COUNTRY_NAME AS DISTRIBUTOR_SITE_COUNTRY_NAME,
         COUNT (DISTINCT BK_SALES_ORDER_NUMBER) AS HUNDRED_PCT_PVR
    FROM DIMS.ORGANIZATION_PARTY T762683,
         DIMS.QUOTE T762071,
         DIMS.ENTERPRISE_LIST_OF_VALUES T705584,
         FACTS.SALES_BOOKING T762157,
         DIMS.CALENDAR T763102,
         DIMS.SALES_ORDER T762251
   WHERE (    T762157.DISTRIBUTOR_AS_IS_KEY = T762683.ORGANIZATION_PARTY_KEY
          AND T705584.ATTRIBUTE_CODE = 'CORP FLAG'
          AND T705584.ATTRIBUTE_CODE_VALUE = T762157.CORP_FLAG
          AND T762157.SOURCE_TRANSACTION_DATE_KEY = T763102.DATE_KEY
          AND T762071.QUOTE_KEY = T762157.QUOTE_KEY
          AND T705584.ATTRIBUTE_CODE_DESCRIPTION = 'Y'
          AND T762157.SALES_ORDER_KEY = T762251.SALES_ORDER_KEY
          AND (T763102.FISCAL_YEAR_QTR_TEXT = '2019Q2')
          AND T762071.HUNDRED_PCT_DISC_REASON_NAME IS NOT NULL
          AND T762157.ME_EXT_LIST_PRICE <> '0')
GROUP BY T762683.SITE_COUNTRY_NAME) """


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
        target_table_name = db_schema+"."+write_table
        # Calling Job Class method --> extract_data_oracle() for source dataset
        dataFrame = Ebi_read_write_obj.extract_data_oracle(query, db_prop_key_load)
        # Calling Job Class method --> extract_data_oracle() for lookup dataset
        lkp_table = Ebi_read_write_obj.extract_data_oracle(lkp_query, db_prop_key_load)

        # Creating Temp Table
        dataFrame.createOrReplaceTempView("product_bkng_report_temp")
        lkp_table.createOrReplaceTempView("product_bkng_report_lkp")
        # Joining Source table data to Lookup table
        target_query = spark.sql("select a.USD_EXTENDED_LIST_PRICE \
                       ,a.USD_BOOKING_AMOUNT \
                       ,a.SO_BOOKING_AMOUNT \
                       ,a.NUMBER_OF_DRIVES_QTY \
                       ,a.DISCOUNT_PERC \
                       ,a.CONTROLLER_QTY \
                       ,a.BOOKING_QTY \
                       ,a.BILL_TO_CUSTOMER_COUNTRY_CODE \
                       ,a.BILL_TO_CUSTOMER_NAGP \
                       ,a.BILL_TO_CUST_PARENT_COMPANY \
                       ,a.CALENDAR_MONTH \
                       ,a.CALENDAR_YEAR \
                       ,a.CHANNEL_TYPE \
                       ,a.DISTRIBUTOR_COMPANY_NAME \
                       ,a.DISTRIBUTOR_SITE_COUNTRY_NAME \
                       ,a.EC_NAGP_NETAPP_VERTICAL \
                       ,a.FISCAL_MONTH \
                       ,a.FISCAL_QTR \
                       ,a.FISCAL_YEAR \
                       ,a.OPPORTUNITY_END_CUSTOMER \
                       ,a.SFDC_OPPORTUNITY_NUMBER \
                       ,a.SFDC_VALUE_ADD_PARTNER1_ROLE \
                       ,a.SFDC_VALUE_ADD_PARTNER2_NAME \
                       ,a.SFDC_VALUE_ADD_PARTNER1_NAME \
                       ,a.SFDC_VALUE_ADD_PARTNER2_ROLE \
                       ,a.PART_NUMBER \
                       ,a.PRODUCT_CATEGORY \
                       ,a.PRODUCT_FAMILY \
                       ,a.PRODUCT_LINE \
                       ,a.PRODUCT_TYPE \
                       ,a.HUNDRED_PCT_DISCOUNTING_REASON \
                       ,a.PVR_APPROVED_DATE \
                       ,a.PVR_EXCEPTION_PROMO \
                       ,a.PVR_REASON \
                       ,a.PVR_USED_FLAG \
                       ,a.QUOTE_BOOKED_DATE \
                       ,a.QUOTE_COMMENTS \
                       ,a.SO_CURRENCY_CODE \
                       ,a.BASE_BOOKING_FLAG \
                       ,a.BOOKED_DATE \
                       ,a.SALES_ORDER_CUSTOMER_PO_NUMBER \
                       ,a.SALES_ORDER_NUMBER \
                       ,a.SALES_ORDER_QUOTE_NUMBER \
                       ,a.ORDER_LINE_NUMBER \
                       ,a.PRODUCT_GROUPING_CODE \
                       ,a.PRODUCT_PARENT_CODE \
                       ,a.PROOF_OF_DELIVERY_DATE \
                       ,a.SALES_ORDER_LINE_INVOICE_DATE \
                       ,a.SHIPPED_DATE \
                       ,a.SALES_REP_NAME \
                       ,a.SHIP_TO_CUSTOMER_COUNTRY_CODE \
                       ,a.SHIP_TO_CUSTOMER_NAGP \
                       ,a.SHIP_TO_CUSTOMER_CITY \
                       ,a.SHIP_TO_COUNTRY \
                       ,a.SOLD_TO_CUSTOMER_NAGP \
                       ,a.SOLD_TO_PARTNER_COMPANY_NAME \
                       ,a.SOLD_TO_PARTNER_LEVEL \
                       ,a.DISTRIBUTOR_NAGP_NAME \
                       ,a.SOLD_TO_PARTNER_NAGP_NAME \
                       ,a.VALUE_ADD_PARTNER_NAGP_NAME \
                       ,a.VAL_ADD_PART_SITE_COUNTRY_NAME \
                       ,a.VALUE_ADD_PARTNER2_NAGP_NAME \
                       ,a.VAL_ADD_PART2_SITE_COUN_NAME \
                       ,a.PATHWAY_TYPE \
                       ,a.SOLD_TO_PART_SITE_COUN_NAME \
                       ,b.HUNDRED_PCT_PVR \
                       from product_bkng_report_temp a \
                       left outer join product_bkng_report_lkp b on (a.DISTRIBUTOR_SITE_COUNTRY_NAME = b.DISTRIBUTOR_SITE_COUNTRY_NAME)")
        
        target_query.show(5)

        # Calling Job Class method --> load_data_oracle()
        Ebi_read_write_obj.load_data_oracle(target_query,target_table_name,save_mode,db_prop_key_load)

        end_date = "'" + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"

        data_format = "JOB START DT : " + start_date + " | SCRIPT NAME : " + script_name + " | JOB : " + app_name + " | SRC COUNT : " + src_count + " | TGT COUNT : " + dest_count + " | JOB END DT : " + end_date + " | STATUS : %(message)s"

        Ebi_read_write_obj.create_log(data_format, log_file, logger)

        logger.info("Success")
        Ebi_read_write_obj.job_debugger_print("	\n  __main__ " + app_name + " --> Job " + app_name + " Succeed \n")

    except Exception as err:  # Write expeption in spark log or console
        Ebi_read_write_obj.job_debugger_print("\n Table PRODUCT_BOOKING_REPORT Load Failed\n")
        Ebi_read_write_obj.job_debugger_print(err)
        raise  # Entry point for script
if __name__ == "__main__":
    # Calling main() method
    main()

