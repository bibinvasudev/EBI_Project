# SCH1100.sh  --> JB_WORK_TO_EDW_INVOICE.py

#**************************************************************************************************************
#
# Created by  : Vinay Kumbakonam
# Modified by : bibin
# Version      : 1.2
#
# Description  :
#		1. This script will load the data into INVOICE tables based on stream lookups.
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
script_name = "SCH1100.sh"
app_name = "JB_WORK_TO_EDW_INVOICE"
log_filename = app_name + '_' + log_date + '.log'


# Query for loading invoice table
def query_data(db_schema):

	query = """
INSERT INTO """+db_schema+""".INVOICE
(VALUE_ADD_PARTNER_NAGP, USD_STANDARD_MARGIN
, USD_RECOGNIZABLE_REVENUE, USD_EXTENDED_LIST_PRICE
, USD_EXTENDED_COST, USD_EXTENDED_BURDENED_COST
, USD_BURDENED_MARGIN, USD_BILLING_AMOUNT
, SOLD_TO_PARTNER_NAGP, SOLD_TO_PARTNER_LEVEL
, SOLD_TO_CUSTOMER_NAGP, SOLD_TO_COUNTRY
, SO_NUMBER, SO_CURRENCY_CODE
, SO_BILLING_AMOUNT, SHIP_TO_CUSTOMER_POSTAL_CODE
, SHIP_TO_CUSTOMER_NAGP, SHIP_TO_CUSTOMER_COUNTRY_CODE
, SHIP_TO_CUSTOMER_CITY, SHIP_TO_CUSTOMER_ADDR_LINE_4
, SHIP_TO_CUSTOMER_ADDR_LINE_3, SHIP_TO_CUSTOMER_ADDR_LINE_2
, SHIP_TO_CUSTOMER_ADDR_LINE_1, SHIP_TO_COUNTRY
, SALES_REP_NAME, SALES_REGION
, SALES_MULTI_AREA, SALES_DISTRICT
, SALES_CHANNEL_CODE, SALES_AREA
, REVENUE_RECOGNIZED_DATE, PRODUCT_TYPE
, PRODUCT_LINE, PRODUCT_FAMILY
, PRODUCT_CATEGORY, MANUAL_INVOICE_FLAG
, FISCAL_DATE, DISTRIBUTOR_NAGP
, CE_PART_CATEGORY_TYPE_CODE, BOOKED_DATE
, BILLING_QTY, BILL_TO_CUSTOMER_NAGP
, BILL_TO_COUNTRY, AR_INVOICE_NUMBER
, AR_INVOICE_DATE, PARTNER_ID
, GEOGRAPHY_SHIP_HIERARCHY_KEY, GEOGRAPHY_BILL_HIERARCHY_KEY
, GEOGRAPHY_SOLD_HIERARCHY_KEY, SOURCE_TRANSACTION_DATE_KEY
, AOO_CUSTOMER_AS_IS_KEY, PRODUCT_AS_IS_KEY
, SOLDTO_PARTNER_AS_IS_KEY, BILLTO_CUSTOMER_AS_IS_KEY
, DISTRIBUTOR_AS_IS_KEY, SHIPTO_CUSTOMER_AS_IS_KEY
, TIER_FLG_2, TIER_FLG_1, SALES_REP_NUMBER
, SALES_GEO, SOLD_TO_PARTNER_DP_CMAT_ID
, SOLD_TO_PARTNER_DP, DISTRIBUTOR_DP_CMAT_ID
, DISTRIBUTOR_DP, ENTRY_DATE
, SOLD_TO_PARTNER_ID, DISTRIBUTOR_PARTNER_ID
, INVOICE_FISCAL_WEEK, INVOICE_FISCAL_QTR
, MONTH_KEY, QUARTER_KEY, DATE_KEY)
SELECT  
A.VALUE_ADD_PARTNER_NAGP
,A.USD_STANDARD_MARGIN
,A.USD_RECOGNIZABLE_REVENUE
,A.USD_EXTENDED_LIST_PRICE
,A.USD_EXTENDED_COST
,A.USD_EXTENDED_BURDENED_COST
,A.USD_BURDENED_MARGIN
,A.USD_BILLING_AMOUNT
,A.SOLD_TO_PARTNER_NAGP
,A.SOLD_TO_PARTNER_LEVEL
,A.SOLD_TO_CUSTOMER_NAGP
,A.SOLD_TO_COUNTRY
,A.SO_NUMBER
,A.SO_CURRENCY_CODE
,A.SO_BILLING_AMOUNT
,A.SHIP_TO_CUSTOMER_POSTAL_CODE
,A.SHIP_TO_CUSTOMER_NAGP
,A.SHIP_TO_CUSTOMER_COUNTRY_CODE
,A.SHIP_TO_CUSTOMER_CITY
,A.SHIP_TO_CUSTOMER_ADDR_LINE_4
,A.SHIP_TO_CUSTOMER_ADDR_LINE_3
,A.SHIP_TO_CUSTOMER_ADDR_LINE_2
,A.SHIP_TO_CUSTOMER_ADDR_LINE_1
,A.SHIP_TO_COUNTRY
,NVL(A.SALES_REP_NAME,'UNKNOWN') AS SALES_REP_NAME
,A.SALES_REGION
,A.SALES_MULTI_AREA
,A.SALES_DISTRICT
,A.SALES_CHANNEL_CODE
,A.SALES_AREA
,A.REVENUE_RECOGNIZED_DATE
,A.PRODUCT_TYPE
,A.PRODUCT_LINE
,A.PRODUCT_FAMILY
,A.PRODUCT_CATEGORY
,A.MANUAL_INVOICE_FLAG
,A.REVENUE_RECOGNIZED_DATE as FISCAL_DATE
,A.DISTRIBUTOR_NAGP
,A.CE_PART_CATEGORY_TYPE_CODE
,A.BOOKED_DATE
,A.BILLING_QTY
,A.BILL_TO_CUSTOMER_NAGP
,A.BILL_TO_COUNTRY
,A.AR_INVOICE_NUMBER
,trunc(A.AR_INVOICE_DATE) As AR_INVOICE_DATE
,A.PARTNER_ID
,A.GEOGRAPHY_SHIP_HIERARCHY_KEY
,A.GEOGRAPHY_BILL_HIERARCHY_KEY
,A.GEOGRAPHY_SOLD_HIERARCHY_KEY
,A.SOURCE_TRANSACTION_DATE_KEY
,A.AOO_CUSTOMER_AS_IS_KEY
,A.PRODUCT_AS_IS_KEY
,A.SOLDTO_PARTNER_AS_IS_KEY
,A.BILLTO_CUSTOMER_AS_IS_KEY
,A.DISTRIBUTOR_AS_IS_KEY
,A.SHIPTO_CUSTOMER_AS_IS_KEY
,CASE WHEN A.PARTNER_ID IS NOT NULL AND SOLD_TO_PARTNER_ID IS NOT NULL AND PARTNER_ID <> SOLD_TO_PARTNER_ID THEN 'Y' ELSE 'N' END AS TIER_FLG_2
,CASE WHEN A.PARTNER_ID IS NOT NULL THEN 'Y' 
   WHEN PARTNER_ID IS NULL AND SOLD_TO_PARTNER_ID IS NOT NULL THEN 'Y' ELSE 'N' END AS TIER_FLG_1
,A.SALES_REP_NUMBER
,A.SALES_GEO
,A.SOLD_TO_PARTNER_DP_CMAT_ID
,A.SOLD_TO_PARTNER_DP
,A.DISTRIBUTOR_DP_CMAT_ID
,A.DISTRIBUTOR_DP
,A.ENTRY_DATE
,A.SOLD_TO_PARTNER_ID
,A.DISTRIBUTOR_PARTNER_ID
,B.FISCAL_YEAR_WEEK_TEXT as INVOICE_FISCAL_WEEK
,B.FISCAL_YEAR_QTR_TEXT as INVOICE_FISCAL_QTR
,B.FISCAL_MONTH_KEY as MONTH_KEY
,C.FISCAL_QUARTER_KEY as QUARTER_KEY
,D.DATE_KEY
FROM T2_SALES_CHANNEL.WORK_INVOICE A
LEFT OUTER JOIN
                (SELECT CAL.FISCAL_YEAR_WEEK_TEXT,CAL.FISCAL_YEAR_QTR_TEXT,CAL.DATE_KEY,FIS.FISCAL_MONTH_KEY FROM 
                T2_SALES_CHANNEL.CALENDAR CAL, T2_SALES_CHANNEL.FISCAL_MONTH FIS
                WHERE 
                CAL.FISCAL_MONTH_START_DATE = FIS.BK_FISCAL_MONTH_START_DATE
                and CAL.FISCAL_MONTH_END_DATE = FIS.FISCAL_MONTH_END_DATE
                ORDER BY CAL.DATE_KEY) B
ON A.SOURCE_TRANSACTION_DATE_KEY = B.DATE_KEY
LEFT OUTER JOIN
                (SELECT CAL.DATE_KEY,FIS.FISCAL_QUARTER_KEY FROM 
                T2_SALES_CHANNEL.CALENDAR CAL, T2_SALES_CHANNEL.FISCAL_QUARTER FIS
                WHERE 
                CAL.FISCAL_QUARTER_START_DATE = FIS.FISCAL_QUARTER_START_DATE
                and CAL.FISCAL_QUARTER_END_DATE = FIS.FISCAL_QUARTER_END_DATE
                ORDER BY CAL.DATE_KEY)C
ON A.SOURCE_TRANSACTION_DATE_KEY = C.DATE_KEY
LEFT OUTER JOIN
                (SELECT DATE_KEY,TRUNC(BK_CALENDAR_DATE) BK_CALENDAR_DATE 
                FROM T2_SALES_CHANNEL.CALENDAR)D
ON A.AR_INVOICE_DATE = D.BK_CALENDAR_DATE
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
            db_schema = config['DB_SCHEMA']
            db_prop_key_load = config['DB_PROP_KEY_LOAD']

            log_file = config['LOG_DIR_NAME'] + "/" + log_filename

            #SQL Query
            query = query_data(db_schema)

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

