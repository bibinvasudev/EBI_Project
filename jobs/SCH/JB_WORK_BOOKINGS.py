# SCH1101.sh  --> JB_WORK_BOOKINGS.py

#**************************************************************************************************************
#
# Created by  : Vinay Kumbakonam
# Modified by : bibin
# Version      : 1.2
#
# Description  :
#		1. This script will load the data into 'WORK_BOOKINGS' table based on stream lookups.
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
#**************************************************************************************************************


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
start_date = "'"+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"'"
log_date =strftime("%Y%m%d", gmtime())

# Job Naming Details
script_name = "SCH1101.SH"
app_name = "JB_WORK_BOOKINGS"
log_filename = app_name + '_' + log_date + '.log'

# Query for loading invoice table
def query_data(db_schema):

	query = """
INSERT INTO """+ db_schema +""".WORK_BOOKINGS
(
SOURCE_TRANSACTION_DATE_KEY, AOO_CUSTOMER_AS_IS_KEY, DATE_KEY, PRODUCT_AS_IS_KEY, SOLDTO_PARTNER_AS_IS_KEY,
BILLTO_CUSTOMER_AS_IS_KEY, DISTRIBUTOR_AS_IS_KEY, SHIPTO_CUSTOMER_AS_IS_KEY, TIER_FLG_2, TIER_FLG_1,
MISMATCH_INSTALLED_AT_SHIP_TO, INSTALLED_AT_CUSTOMER, PO_NUMBER, INSTALLED_AT_COUNTRY, SERIAL_NUMBER, DEAL_DESK_REASON,
ORDER_LINE_STATUS, ORDER_LINE, BOOKING_FISCAL_WEEK_ACTUAL, BOOKING_FISCAL_QTR_ACTUAL, DEAL_DESK_FLAG, ACCOUNT_NAGP,
ELA_ESA_CODE, PVR_STATUS, PVR_APPROVAL_ROLE, SO_PRIMARY_RESERVE_CODE, PVR_APPROVED_DATE, QUOTE_CREATED_DATE,
QUOTE_NUMBER, SOLD_TO_PARTNER_DP_CMAT_ID, SOLD_TO_PARTNER_DP, DISTRIBUTOR_DP_CMAT_ID, DISTRIBUTOR_DP,
SFDC_RESELLING_NAGP_ID, SFDC_RESELLING_PARTNER, SFDC_DISTRIBUTOR_NAGP_ID, SFDC_DISTRIBUTOR, SALES_GEOGRAPHY,
SALES_REP_NUMBER, ENTRY_DATE, DISTRIBUTOR_PARTNER_ID, PARTNER_COMPANY_CMAT_ID, DISTRIBUTOR_COMPANY_CMAT_ID,
PARTNER_ADD_MATCH, VALUE_ADD_PARTNER_NAGP_CMAT_ID, VALUE_ADD_PARTNER_NAGP, USD_STANDARD_MARGIN, USD_EXTENDED_LIST_PRICE,
USD_EXTENDED_DISCOUNT, USD_EXTENDED_COST, USD_EXTENDED_BURDENED_COST, USD_BURDENED_MARGIN, USD_BOOKING_AMOUNT,
SOLD_TO_PARTNER_NAGP_CMAT_ID, SOLD_TO_PARTNER_NAGP, SOLD_TO_PARTNER_LEVEL, SOLD_TO_CUSTOMER_NAGP,
SOLD_TO_COUNTRY, SO_NUMBER, SO_CURRENCY_CODE, SO_BOOKING_AMOUNT, SHIPPED_DATE, SHIP_TO_CUSTOMER_POSTAL_CODE,
SHIP_TO_CUSTOMER_NAGP, SHIP_TO_CUSTOMER_COUNTRY_CODE, SHIP_TO_CUSTOMER_CITY, SHIP_TO_CUSTOMER_ADDRESS4,
SHIP_TO_CUSTOMER_ADDRESS3, SHIP_TO_CUSTOMER_ADDRESS2, SHIP_TO_CUSTOMER_ADDRESS1, SHIP_TO_COUNTRY, SALES_REP_NAME,
SALES_REGION, SALES_MULTI_AREA, SALES_DISTRICT, SALES_CHANNEL_CODE, SALES_AREA, PRODUCT_TYPE, PRODUCT_PARENT_CODE,
PRODUCT_LINE, PRODUCT_GROUPING_CODE, PRODUCT_FAMILY, PRODUCT_CATEGORY, PATHWAY_TYPE, OPPORTUNITY_END_CUSTOMER,
FISCAL_DATE, DISTRIBUTOR_NAGP_CMAT_ID, DISTRIBUTOR_NAGP, CHANNEL_TYPE, CE_PART_CATEGORY_TYPE_CODE, CANCELLED_FLAG,
BOOKING_FISCAL_QTR, BOOKING_FISCAL_WEEK, BOOKED_DATE, BILL_TO_CUSTOMER_NAGP, BILL_TO_COUNTRY, BE_USD_BOOKING_AMOUNT,
PARTNER_ID, GEOGRAPHY_SOLD_HIERARCHY_KEY, GEOGRAPHY_BILL_HIERARCHY_KEY, GEOGRAPHY_SHIP_HIERARCHY_KEY, SOLD_TO_PARTNER_ID
)
WITH LKP1 as (
SELECT SOLD_TO_PARTNER_ID, PARTNER_NAME
FROM (
SELECT A.partner_id AS SOLD_TO_PARTNER_ID, UPPER(A.PARTNER_NAME) AS PARTNER_NAME, 
row_number() OVER (PARTITION BY UPPER(PARTNER_NAME) ORDER BY BUSINESS_MODEL_STATUS )  AS RN1
FROM T2_SALES_CHANNEL.PARTNER_DETAILS A
ORDER BY BUSINESS_MODEL_STATUS
)
WHERE RN1 = 1),
LKP2 AS (
SELECT SOLD_TO_PARTNER_ID, CMAT_ID
FROM (
SELECT A.partner_id AS SOLD_TO_PARTNER_ID, cast(A.CMAT_ID as integer) CMAT_ID, 
row_number() OVER (PARTITION BY CMAT_ID ORDER BY BUSINESS_MODEL_STATUS )  AS RN1
FROM T2_SALES_CHANNEL.PARTNER_DETAILS A
WHERE CMAT_ID IS NOT NULL
ORDER BY BUSINESS_MODEL_STATUS
)
WHERE RN1 = 1),
LKP3 AS (
SELECT SOLD_TO_PARTNER_ID, COO
FROM (
SELECT A.partner_id AS SOLD_TO_PARTNER_ID, UPPER(A.COO) AS COO, 
row_number() OVER (PARTITION BY UPPER(COO) ORDER BY BUSINESS_MODEL_STATUS )  AS RN1
FROM T2_SALES_CHANNEL.PARTNER_DETAILS A
WHERE COO IS NOT NULL
ORDER BY BUSINESS_MODEL_STATUS
)
WHERE RN1 = 1),
LKP4 AS(
SELECT SOLD_TO_PARTNER_ID, COO
FROM (
SELECT A.partner_id AS SOLD_TO_PARTNER_ID, UPPER(A.COO) AS COO,
row_number() OVER (PARTITION BY UPPER(COO) ORDER BY BUSINESS_MODEL_STATUS )  AS RN1
FROM T2_SALES_CHANNEL.PARTNER_DETAILS A
WHERE COO IS NOT NULL
ORDER BY BUSINESS_MODEL_STATUS
)
WHERE RN1 = 1),
LKP5 AS(
SELECT SOLD_TO_PARTNER_ID, COO
FROM (
SELECT A.partner_id AS SOLD_TO_PARTNER_ID, UPPER(A.COO) COO, 
row_number() OVER (PARTITION BY UPPER(COO) ORDER BY BUSINESS_MODEL_STATUS ) AS RN1 
FROM T2_SALES_CHANNEL.PARTNER_DETAILS A
WHERE COO IS NOT NULL
ORDER BY BUSINESS_MODEL_STATUS
)
WHERE RN1 = 1),
LKP6 AS(
SELECT FISCAL_YEAR_QTR_TEXT  AS Booking_Fiscal_Qtr,
  FISCAL_YEAR_WEEK_TEXT AS Booking_Fiscal_Week, TRUNC(BK_CALENDAR_DATE) BK_CALENDAR_DATE
from T2_SALES_CHANNEL.CALENDAR
)
SELECT
    A.SOURCE_TRANSACTION_DATE_KEY,
                A.AOO_CUSTOMER_AS_IS_KEY,
                A.DATE_KEY,
                A.PRODUCT_AS_IS_KEY,
                A.SOLDTO_PARTNER_AS_IS_KEY,
                A.BILLTO_CUSTOMER_AS_IS_KEY,
                A.DISTRIBUTOR_AS_IS_KEY,
                A.SHIPTO_CUSTOMER_AS_IS_KEY,
                A.TIER_FLG_2,
                A.TIER_FLG_1,
                A.MISMATCH_INSTALLED_AT_SHIP_TO,
                A.INSTALLED_AT_CUSTOMER,
                A.PO_NUMBER,
                A.INSTALLED_AT_COUNTRY,
                A.SERIAL_NUMBER,
                A.DEAL_DESK_REASON,
                A.ORDER_LINE_STATUS,
                A.ORDER_LINE,
                A.BOOKING_FISCAL_WEEK_ACTUAL,
                A.BOOKING_FISCAL_QTR_ACTUAL,
                A.DEAL_DESK_FLAG,
                A.ACCOUNT_NAGP,
                A.ELA_ESA_CODE,
                A.PVR_STATUS,
                A.PVR_APPROVAL_ROLE,
                A.SO_PRIMARY_RESERVE_CODE,
                A.PVR_APPROVED_DATE,
                A.QUOTE_CREATED_DATE,
                A.QUOTE_NUMBER,
                A.SOLD_TO_PARTNER_DP_CMAT_ID,
                trim(upper(A.SOLD_TO_PARTNER_DP)) as SOLD_TO_PARTNER_DP,
                A.DISTRIBUTOR_DP_CMAT_ID,
                A.DISTRIBUTOR_DP,
                A.SFDC_RESELLING_NAGP_ID,
                trim(upper(A.SFDC_RESELLING_PARTNER)) as SFDC_RESELLING_PARTNER,
                A.SFDC_DISTRIBUTOR_NAGP_ID,
                A.SFDC_DISTRIBUTOR,
                A.SALES_GEOGRAPHY,
                A.SALES_REP_NUMBER,
                A.ENTRY_DATE,
                A.DISTRIBUTOR_PARTNER_ID,
                CAST(A.PARTNER_COMPANY_CMAT_ID AS INTEGER) PARTNER_COMPANY_CMAT_ID,
                A.DISTRIBUTOR_COMPANY_CMAT_ID,
                A.PARTNER_ADD_MATCH,
                A.VALUE_ADD_PARTNER_NAGP_CMAT_ID,
                A.VALUE_ADD_PARTNER_NAGP,
                A.USD_STANDARD_MARGIN,
                A.USD_EXTENDED_LIST_PRICE,
                A.USD_EXTENDED_DISCOUNT,
                A.USD_EXTENDED_COST,
                A.USD_EXTENDED_BURDENED_COST,
                A.USD_BURDENED_MARGIN,
                A.USD_BOOKING_AMOUNT,
                A.SOLD_TO_PARTNER_NAGP_CMAT_ID,
                trim(upper(A.SOLD_TO_PARTNER_NAGP)) as SOLD_TO_PARTNER_NAGP,
                A.SOLD_TO_PARTNER_LEVEL,
                A.SOLD_TO_CUSTOMER_NAGP,
                A.SOLD_TO_COUNTRY,
                A.SO_NUMBER,
                A.SO_CURRENCY_CODE,
                A.SO_BOOKING_AMOUNT,
                A.SHIPPED_DATE,
                A.SHIP_TO_CUSTOMER_POSTAL_CODE,
                A.SHIP_TO_CUSTOMER_NAGP,
                A.SHIP_TO_CUSTOMER_COUNTRY_CODE,
                A.SHIP_TO_CUSTOMER_CITY,
                A.SHIP_TO_CUSTOMER_ADDRESS4,
                A.SHIP_TO_CUSTOMER_ADDRESS3,
                A.SHIP_TO_CUSTOMER_ADDRESS2,
                A.SHIP_TO_CUSTOMER_ADDRESS1,
                A.SHIP_TO_COUNTRY,
                A.SALES_REP_NAME,
                A.SALES_REGION,
                A.SALES_MULTI_AREA,
                A.SALES_DISTRICT,
                A.SALES_CHANNEL_CODE,
                A.SALES_AREA,
                A.PRODUCT_TYPE,
                A.PRODUCT_PARENT_CODE,
                A.PRODUCT_LINE,
                A.PRODUCT_GROUPING_CODE,
                A.PRODUCT_FAMILY,
                A.PRODUCT_CATEGORY,
                A.PATHWAY_TYPE,
                A.OPPORTUNITY_END_CUSTOMER,
                A.FISCAL_DATE,
                A.DISTRIBUTOR_NAGP_CMAT_ID,
                A.DISTRIBUTOR_NAGP,
                A.CHANNEL_TYPE,
                A.CE_PART_CATEGORY_TYPE_CODE,
                A.CANCELLED_FLAG,
                G.BOOKING_FISCAL_QTR,
                G.BOOKING_FISCAL_WEEK,
                A.BOOKED_DATE,
                A.BILL_TO_CUSTOMER_NAGP,
                A.BILL_TO_COUNTRY,
                A.BE_USD_BOOKING_AMOUNT,
                A.PARTNER_ID,
                A.GEOGRAPHY_SOLD_HIERARCHY_KEY,
                A.GEOGRAPHY_BILL_HIERARCHY_KEY,
                A.GEOGRAPHY_SHIP_HIERARCHY_KEY,
(CASE
    WHEN B.PARTNER_NAME iS NOT NULL THEN B.SOLD_TO_PARTNER_ID
    WHEN C.CMAT_ID IS NOT NULL THEN C.SOLD_TO_PARTNER_ID
    WHEN D.COO IS NOT NULL THEN D.SOLD_TO_PARTNER_ID
    WHEN E.COO IS NOT NULL THEN E.SOLD_TO_PARTNER_ID
    WHEN F.COO IS NOT NULL THEN F.SOLD_TO_PARTNER_ID
    ELSE NULL
    END) AS SOLD_TO_PARTNER_ID
                --G.BOOKING_FISCAL_QTR AS BOOKING_FISCAL_QTR,
                --G.BOOKING_FISCAL_WEEK AS BOOKING_FISCAL_WEEK
FROM T2_SALES_CHANNEL.STG_BOOKINGS A
LEFT OUTER JOIN
LKP1 B ON (A.SOLD_TO_PARTNER_DP=B.PARTNER_NAME)
LEFT OUTER JOIN
LKP2 C ON (A.PARTNER_COMPANY_CMAT_ID=C.CMAT_ID)
LEFT OUTER JOIN
LKP3 D ON (A.SOLD_TO_PARTNER_DP=D.COO)
LEFT OUTER JOIN
LKP4 E ON (A.SFDC_RESELLING_PARTNER=E.COO)
LEFT OUTER JOIN
LKP5 F ON (A.SOLD_TO_PARTNER_NAGP=F.COO)
LEFT OUTER JOIN
LKP6 G ON (A.FISCAL_DATE=G.BK_CALENDAR_DATE)

		"""

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
            spark, config = start_spark(
                app_name=app_name)

            # Create class Object
            Ebi_read_write_obj = EbiReadWrite(app_name,spark,config,logger)

            # DB prop Key of Source DB
            #db_prop_key_load = config['DB_PROP_KEY_LOAD']
            db_prop_key_load = 'EBI_EDW07'
            db_schema = config['DB_SCHEMA']
            log_file = config['LOG_DIR_NAME'] + "/" + log_filename
            #SQL Query
            query = query_data(db_schema)

            #log = logging.getLogger(app_name)

            # Calling Job Class method --> get_target_data_update()
            Ebi_read_write_obj.get_target_data_update(query,db_prop_key_load)

            end_date="'"+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"'"

            data_format = "JOB START DT : "+start_date+" | SCRIPT NAME : "+script_name+" | JOB : "+app_name+" | SRC COUNT : "+src_count+" | TGT COUNT : "+dest_count+" | JOB END DT : "+end_date+" | STATUS : %(message)s"

            Ebi_read_write_obj.create_log(data_format,log_file,logger)

            logger.info("Success")
            Ebi_read_write_obj.job_debugger_print("     \n  __main__ " + app_name +" --> Job "+app_name+" Succeed \n")

        except Exception as err:
            # Write expeption in spark log or console
            end_date="'"+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"'"
            data_format = "JOB START DT : "+start_date+" | SCRIPT NAME : "+script_name+" | JOB : "+app_name+" | SRC COUNT : "+src_count+" | TGT COUNT : "+dest_count+" | JOB END DT : "+end_date+" | STATUS : %(message)s"
            Ebi_read_write_obj.create_log(data_format,log_file,logger)
            logger.info("[Error] Failed")
            Ebi_read_write_obj.job_debugger_print("     \n Job "+app_name+" Failed\n")
            logger.error("\n __main__ "+ app_name +" --> Exception-Traceback :: " + str(err))
            raise

# Entry point for script
if __name__ == "__main__":
    # Calling main() method
    main()

