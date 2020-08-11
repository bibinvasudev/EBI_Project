# SCH1101.sh  --> JB_WORK_TO_BOOKINGS.py

#**************************************************************************************************************
#
# Created by  : Vinay Kumbakonam
# Modified by : bibin
# Version      : 1.2
#
# Description  :
#		1. This script will load the data into 'BOOKINGS' table based on stream lookups.
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
app_name = "JB_WORK_TO_BOOKINGS"
log_filename = app_name + '_' + log_date + '.log'


# Query for loading invoice table
def query_data(db_schema):

	query = """
INSERT INTO """+ db_schema +""".BOOKINGS 
(ACCOUNT_NAGP, 
AOO_CUSTOMER_AS_IS_KEY, 
BE_USD_BOOKING_AMOUNT, 
BILL_TO_COUNTRY, 
BILL_TO_CUSTOMER_NAGP, 
BILLTO_CUSTOMER_AS_IS_KEY, 
BOOKED_DATE, 
BOOKING_FISCAL_QTR, 
BOOKING_FISCAL_WEEK, 
CANCELLED_FLAG, 
CE_PART_CATEGORY_TYPE_CODE, 
CHANNEL_TYPE, 
DISTRIBUTOR_AS_IS_KEY, 
DISTRIBUTOR_COMPANY_CMAT_ID, 
DISTRIBUTOR_DP, 
DISTRIBUTOR_DP_CMAT_ID, 
DISTRIBUTOR_NAGP, 
DISTRIBUTOR_NAGP_CMAT_ID, 
DISTRIBUTOR_PARTNER_ID, 
ELA_ESA_CODE, 
ENTRY_DATE, 
FISCAL_DATE, 
INSTALLED_AT_COUNTRY, 
INSTALLED_AT_CUSTOMER, 
MISMATCH_INSTALLED_AT_SHIP_TO, 
OPPORTUNITY_END_CUSTOMER, 
ORDER_LINE, ORDER_LINE_STATUS, 
PARTNER_ADD_MATCH, 
PARTNER_COMPANY_CMAT_ID, 
PARTNER_ID, 
PATHWAY_TYPE, 
PO_NUMBER, 
PRODUCT_AS_IS_KEY, 
PRODUCT_CATEGORY, 
PRODUCT_FAMILY, 
PRODUCT_GROUPING_CODE, 
PRODUCT_LINE, 
PRODUCT_PARENT_CODE, 
PRODUCT_TYPE, 
PVR_APPROVAL_ROLE, 
PVR_APPROVED_DATE, 
PVR_STATUS, 
QUOTE_CREATED_DATE, 
QUOTE_NUMBER, 
SALES_AREA, 
SALES_CHANNEL_CODE, 
SALES_DISTRICT, 
SALES_GEOGRAPHY, 
SALES_MULTI_AREA, 
SALES_REGION, 
SALES_REP_NAME, 
SALES_REP_NUMBER, 
SERIAL_NUMBER, 
SFDC_DISTRIBUTOR, 
SFDC_DISTRIBUTOR_NAGP_ID, 
SFDC_RESELLING_NAGP_ID, 
SFDC_RESELLING_PARTNER, 
SHIP_TO_COUNTRY, 
SHIP_TO_CUSTOMER_ADDRESS1, 
SHIP_TO_CUSTOMER_ADDRESS2, 
SHIP_TO_CUSTOMER_ADDRESS3, 
SHIP_TO_CUSTOMER_ADDRESS4, 
SHIP_TO_CUSTOMER_CITY, 
SHIP_TO_CUSTOMER_COUNTRY_CODE, 
SHIP_TO_CUSTOMER_NAGP, 
SHIP_TO_CUSTOMER_POSTAL_CODE, 
SHIPPED_DATE, 
SHIPTO_CUSTOMER_AS_IS_KEY, 
SO_BOOKING_AMOUNT, 
SO_CURRENCY_CODE, 
SO_NUMBER, 
SO_PRIMARY_RESERVE_CODE, 
SOLD_TO_COUNTRY, 
SOLD_TO_CUSTOMER_NAGP, 
SOLD_TO_PARTNER_DP, 
SOLD_TO_PARTNER_DP_CMAT_ID, 
SOLD_TO_PARTNER_ID, 
SOLD_TO_PARTNER_LEVEL, 
SOLD_TO_PARTNER_NAGP, 
SOLD_TO_PARTNER_NAGP_CMAT_ID, 
SOLDTO_PARTNER_AS_IS_KEY, 
SOURCE_TRANSACTION_DATE_KEY, 
TIER_FLG_2, 
TIER_FLG_1, 
USD_BOOKING_AMOUNT, 
USD_BURDENED_MARGIN, 
USD_EXTENDED_BURDENED_COST, 
USD_EXTENDED_COST, 
USD_EXTENDED_DISCOUNT, 
USD_EXTENDED_LIST_PRICE, 
USD_STANDARD_MARGIN, 
VALUE_ADD_PARTNER_NAGP, 
VALUE_ADD_PARTNER_NAGP_CMAT_ID, 
DEBOOKED_FLAG, 
GEOGRAPHY_SOLD_HIERARCHY_KEY, 
GEOGRAPHY_BILL_HIERARCHY_KEY, 
GEOGRAPHY_SHIP_HIERARCHY_KEY, 
PARTNER_TERRITORY, 
BOOKING_FISCAL_QTR_ACTUAL, 
BOOKING_FISCAL_WEEK_ACTUAL, 
DATE_KEY, 
QUOTE_FISCAL_QTR, 
DEAL_DESK_REASON, 
DEAL_DESK_FLAG)

WITH LKP1 AS (
SELECT FISCAL_YEAR_QTR_TEXT AS Booking_Fiscal_Qtr_Actual,
FISCAL_YEAR_WEEK_TEXT  AS Booking_Fiscal_Week_Actual , TRUNC(BK_CALENDAR_DATE) BK_CALENDAR_DATE, DATE_KEY
from T2_SALES_CHANNEL.CALENDAR
),
LKP2 AS (
SELECT FISCAL_YEAR_QTR_TEXT AS QUOTE_FISCAL_QTR, TRUNC(BK_CALENDAR_DATE) BK_CALENDAR_DATE
from T2_SALES_CHANNEL.CALENDAR 
WHERE BK_CALENDAR_DATE IS NOT NULL
),
LKP3 AS (
select distinct QUOTE_NUMBER,BOOKING_FISCAL_QTR,ELA_ESA_CODE as DEAL_DESK_REASON,'Y' as DEAL_DESK_FLAG 
from T2_SALES_CHANNEL.WORK_BOOKINGS 
Where ELA_ESA_Code in ('ELA - Initial','ESA - Initial') and QUOTE_NUMBER IS NOT NULL
union
select distinct QUOTE_NUMBER,BOOKING_FISCAL_QTR, SALES_GEOGRAPHY ||'- Booking Amount > $1M' AS DEAL_DESK_REASON,'Y' AS DEAL_DESK_FLAG 
FROM T2_SALES_CHANNEL.WORK_BOOKINGS a 
where  (QUOTE_NUMBER,BOOKING_FISCAL_QTR) in (select QUOTE_NUMBER,FISCAL_YEAR_QTR_TEXT 
from T2_SALES_CHANNEL.WORK_BOOKINGS B,T2_SALES_CHANNEL.CALENDAR C 
WHERE Trunc(fiscal_date)= trunc(bk_calendar_date)
group by QUOTE_NUMBER,FISCAL_YEAR_QTR_TEXT having SUM(USD_BOOKING_AMOUNT) > 1000000)
And Sales_Geography in('Americas','EMEA') And ELA_ESA_Code not in ('ELA - Initial','ESA - Initial') and QUOTE_NUMBER IS NOT NULL
union
select distinct QUOTE_NUMBER,BOOKING_FISCAL_QTR,SALES_GEOGRAPHY ||'- Booking Amount > $10M' as DEAL_DESK_REASON,'Y' as DEAL_DESK_FLAG 
from T2_SALES_CHANNEL.WORK_BOOKINGS a 
where (QUOTE_NUMBER,BOOKING_FISCAL_QTR) in (select QUOTE_NUMBER,FISCAL_YEAR_QTR_TEXT 
from T2_SALES_CHANNEL.WORK_BOOKINGS B,T2_SALES_CHANNEL.CALENDAR C 
WHERE Trunc(fiscal_date)= trunc(bk_calendar_date)
group by QUOTE_NUMBER,FISCAL_YEAR_QTR_TEXT having SUM(USD_BOOKING_AMOUNT) > 10000000)
And Sales_Geography = 'US Public Sector' And ELA_ESA_Code not in ('ELA - Initial','ESA - Initial') and QUOTE_NUMBER IS NOT NULL
union
select distinct QUOTE_NUMBER,BOOKING_FISCAL_QTR,SALES_GEOGRAPHY ||'- Booking Amount > $500K' AS DEAL_DESK_REASON,'Y' AS DEAL_DESK_FLAG 
from T2_SALES_CHANNEL.WORK_BOOKINGS a 
where (QUOTE_NUMBER,BOOKING_FISCAL_QTR) in (select QUOTE_NUMBER,FISCAL_YEAR_QTR_TEXT 
from T2_SALES_CHANNEL.WORK_BOOKINGS B,T2_SALES_CHANNEL.CALENDAR C 
WHERE Trunc(fiscal_date)= trunc(bk_calendar_date)
group by QUOTE_NUMBER,FISCAL_YEAR_QTR_TEXT having SUM(USD_BOOKING_AMOUNT) > 500000)
and SALES_GEOGRAPHY = 'Asia Pacific' and ELA_ESA_CODE not in ('ELA - Initial','ESA - Initial') and QUOTE_NUMBER IS NOT NULL
)
SELECT
Z.ACCOUNT_NAGP, Z.AOO_CUSTOMER_AS_IS_KEY, Z.BE_USD_BOOKING_AMOUNT, Z.BILL_TO_COUNTRY, Z.BILL_TO_CUSTOMER_NAGP, Z.BILLTO_CUSTOMER_AS_IS_KEY, Z.BOOKED_DATE, Z.BOOKING_FISCAL_QTR, Z.BOOKING_FISCAL_WEEK, Z.CANCELLED_FLAG, Z.CE_PART_CATEGORY_TYPE_CODE, Z.CHANNEL_TYPE, Z.DISTRIBUTOR_AS_IS_KEY, Z.DISTRIBUTOR_COMPANY_CMAT_ID, Z.DISTRIBUTOR_DP, Z.DISTRIBUTOR_DP_CMAT_ID, Z.DISTRIBUTOR_NAGP, Z.DISTRIBUTOR_NAGP_CMAT_ID, Z.DISTRIBUTOR_PARTNER_ID, Z.ELA_ESA_CODE, Z.ENTRY_DATE, Z.FISCAL_DATE, Z.INSTALLED_AT_COUNTRY, Z.INSTALLED_AT_CUSTOMER, Z.MISMATCH_INSTALLED_AT_SHIP_TO, Z.OPPORTUNITY_END_CUSTOMER, Z.ORDER_LINE, Z.ORDER_LINE_STATUS, Z.PARTNER_ADD_MATCH, Z.PARTNER_COMPANY_CMAT_ID, Z.PARTNER_ID, Z.PATHWAY_TYPE, Z.PO_NUMBER, Z.PRODUCT_AS_IS_KEY, Z.PRODUCT_CATEGORY, Z.PRODUCT_FAMILY, Z.PRODUCT_GROUPING_CODE, Z.PRODUCT_LINE, Z.PRODUCT_PARENT_CODE, Z.PRODUCT_TYPE, Z.PVR_APPROVAL_ROLE, Z.PVR_APPROVED_DATE, Z.PVR_STATUS, Z.QUOTE_CREATED_DATE, Z.QUOTE_NUMBER, Z.SALES_AREA, Z.SALES_CHANNEL_CODE, Z.SALES_DISTRICT, Z.SALES_GEOGRAPHY, Z.SALES_MULTI_AREA, Z.SALES_REGION, Z.SALES_REP_NAME, Z.SALES_REP_NUMBER, Z.SERIAL_NUMBER, Z.SFDC_DISTRIBUTOR, Z.SFDC_DISTRIBUTOR_NAGP_ID, Z.SFDC_RESELLING_NAGP_ID, Z.SFDC_RESELLING_PARTNER, Z.SHIP_TO_COUNTRY, Z.SHIP_TO_CUSTOMER_ADDRESS1, Z.SHIP_TO_CUSTOMER_ADDRESS2, Z.SHIP_TO_CUSTOMER_ADDRESS3, Z.SHIP_TO_CUSTOMER_ADDRESS4, Z.SHIP_TO_CUSTOMER_CITY, Z.SHIP_TO_CUSTOMER_COUNTRY_CODE, Z.SHIP_TO_CUSTOMER_NAGP, Z.SHIP_TO_CUSTOMER_POSTAL_CODE, Z.SHIPPED_DATE, Z.SHIPTO_CUSTOMER_AS_IS_KEY, Z.SO_BOOKING_AMOUNT, Z.SO_CURRENCY_CODE, Z.SO_NUMBER, Z.SO_PRIMARY_RESERVE_CODE, Z.SOLD_TO_COUNTRY, Z.SOLD_TO_CUSTOMER_NAGP, Z.SOLD_TO_PARTNER_DP, Z.SOLD_TO_PARTNER_DP_CMAT_ID, Z.SOLD_TO_PARTNER_ID, Z.SOLD_TO_PARTNER_LEVEL, Z.SOLD_TO_PARTNER_NAGP, Z.SOLD_TO_PARTNER_NAGP_CMAT_ID, Z.SOLDTO_PARTNER_AS_IS_KEY, Z.SOURCE_TRANSACTION_DATE_KEY, Z.TIER_FLG_2, Z.TIER_FLG_1, Z.USD_BOOKING_AMOUNT, Z.USD_BURDENED_MARGIN, Z.USD_EXTENDED_BURDENED_COST, Z.USD_EXTENDED_COST, Z.USD_EXTENDED_DISCOUNT, Z.USD_EXTENDED_LIST_PRICE, Z.USD_STANDARD_MARGIN, Z.VALUE_ADD_PARTNER_NAGP, Z.VALUE_ADD_PARTNER_NAGP_CMAT_ID, Z.DEBOOKED_FLAG, Z.GEOGRAPHY_SOLD_HIERARCHY_KEY, Z.GEOGRAPHY_BILL_HIERARCHY_KEY, Z.GEOGRAPHY_SHIP_HIERARCHY_KEY, Z.PARTNER_TERRITORY, B.BOOKING_FISCAL_QTR_ACTUAL, B.BOOKING_FISCAL_WEEK_ACTUAL, B.DATE_KEY, 
C.QUOTE_FISCAL_QTR, D.DEAL_DESK_REASON, D.DEAL_DESK_FLAG
FROM T2_SALES_CHANNEL.BOOKINGS_DATAPREP Z
LEFT OUTER JOIN
LKP1 B ON (Z.BOOKED_DATE = B.BK_CALENDAR_DATE)
LEFT OUTER JOIN
LKP2 C ON (Z.QUOTE_CREATED_DATE = C.BK_CALENDAR_DATE)
LEFT OUTER JOIN
LKP3 D ON (Z.QUOTE_NUMBER = D.QUOTE_NUMBER AND Z.BOOKING_FISCAL_QTR = D.BOOKING_FISCAL_QTR)
WHERE Z.PARTNER_ID IS NOT NULL
or (Z.DISTRIBUTOR_DP = 'UNKNOWN' AND Z.SOLD_TO_PARTNER_DP = 'UNKNOWN')

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
            db_prop_key = config['DB_PROP_KEY_LOAD']
            db_schema = config['DB_SCHEMA']
            log_file = config['LOG_DIR_NAME'] + "/" + log_filename

            #SQL Query
            query = query_data(db_schema)

            # Calling Job Class method --> get_target_data_update()
            Ebi_read_write_obj.get_target_data_update(query,db_prop_key)
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

