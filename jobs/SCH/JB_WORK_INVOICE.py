#SCH1006.py  --> JB_WORK_INVOICE.py

#**************************************************************************************************************
#
# Programmer   : bibin
# Version      : 1.0
#
# Description  :
#
#
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-10-24                    Initial creation
#
#
#**************************************************************************************************************


# Importing required Lib

import logging
import sys
from time import gmtime, strftime
import cx_Oracle
import py4j
import pyspark

from dependencies.spark import start_spark
from dependencies.EbiReadWrite import EbiReadWrite


# Spark logging
logger = logging.getLogger(__name__)

# Date Formats
start_date = "'"+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"'"
log_date =strftime("%Y%m%d", gmtime())

# Job Naming Details
script_name = "SCH1100.sh"
app_name = 'JB_WORK_INVOICE'
log_filename = app_name + '_' + log_date + '.log'

# Query for Extract data
def query_data(db_schema):

        query = """insert into """+ db_schema +""".WORK_INVOICE(
            VALUE_ADD_PARTNER_NAGP,
            USD_STANDARD_MARGIN,
            USD_RECOGNIZABLE_REVENUE,
            USD_EXTENDED_LIST_PRICE,
            USD_EXTENDED_COST,
            USD_EXTENDED_BURDENED_COST,
            USD_BURDENED_MARGIN,
            USD_BILLING_AMOUNT,
            SOLD_TO_PARTNER_NAGP,
            SOLD_TO_PARTNER_LEVEL,
            SOLD_TO_CUSTOMER_NAGP,
            SOLD_TO_COUNTRY,
            SO_NUMBER,
            SO_CURRENCY_CODE,
            SO_BILLING_AMOUNT,
            SHIP_TO_CUSTOMER_POSTAL_CODE,
            SHIP_TO_CUSTOMER_NAGP,
            SHIP_TO_CUSTOMER_COUNTRY_CODE,
            SHIP_TO_CUSTOMER_CITY,
            SHIP_TO_CUSTOMER_ADDR_LINE_4,
            SHIP_TO_CUSTOMER_ADDR_LINE_3,
            SHIP_TO_CUSTOMER_ADDR_LINE_2,
            SHIP_TO_CUSTOMER_ADDR_LINE_1,
            SHIP_TO_COUNTRY,
            SALES_REP_NAME,
            SALES_REGION,
            SALES_MULTI_AREA,
            SALES_DISTRICT,
            SALES_CHANNEL_CODE,
            SALES_AREA,
            REVENUE_RECOGNIZED_DATE,
            PRODUCT_TYPE,
            PRODUCT_LINE,
            PRODUCT_FAMILY,
            PRODUCT_CATEGORY,
            MANUAL_INVOICE_FLAG,
            INVOICE_FISCAL_WEEK,
            INVOICE_FISCAL_QTR,
            FISCAL_DATE,
            DISTRIBUTOR_NAGP,
            CE_PART_CATEGORY_TYPE_CODE,
            BOOKED_DATE,
            BILLING_QTY,
            BILL_TO_CUSTOMER_NAGP,
            BILL_TO_COUNTRY,
            AR_INVOICE_NUMBER,
            AR_INVOICE_DATE,
            PARTNER_ID,
            GEOGRAPHY_SHIP_HIERARCHY_KEY,
            GEOGRAPHY_BILL_HIERARCHY_KEY,
            GEOGRAPHY_SOLD_HIERARCHY_KEY,
            SOURCE_TRANSACTION_DATE_KEY,
            AOO_CUSTOMER_AS_IS_KEY,
            DATE_KEY,
            PRODUCT_AS_IS_KEY,
            SOLDTO_PARTNER_AS_IS_KEY,
            BILLTO_CUSTOMER_AS_IS_KEY,
            DISTRIBUTOR_AS_IS_KEY,
            SHIPTO_CUSTOMER_AS_IS_KEY,
            TIER_FLG_2,
            TIER_FLG_1,
            SALES_REP_NUMBER,
            SALES_GEO,
            SOLD_TO_PARTNER_DP_CMAT_ID,
            SOLD_TO_PARTNER_DP,
            DISTRIBUTOR_DP_CMAT_ID,
            DISTRIBUTOR_DP,
            ENTRY_DATE,
            DISTRIBUTOR_PARTNER_ID,
            SOLD_TO_PARTNER_ID
            )


            WITH LKP1 as (
            SELECT Partner_ID as Sold_To_Partner_ID, TRIM(UPPER(Partner_Name)) Partner_Name
            FROM (
            SELECT A.*, ROW_NUMBER() OVER (PARTITION BY UPPER(PARTNER_NAME) ORDER BY BUSINESS_MODEL_STATUS )  AS RANK_1 FROM T2_SALES_CHANNEL.PARTNER_DETAILS A
            ORDER BY BUSINESS_MODEL_STATUS
            )
            WHERE RANK_1 = 1),
            LKP2 AS (
            SELECT Partner_ID as Sold_To_Partner_ID, TRIM(UPPER(COO)) AS COO
            FROM (
            SELECT A.*, ROW_NUMBER() OVER (PARTITION BY UPPER(COO) ORDER BY BUSINESS_MODEL_STATUS )  AS RANK_1 FROM T2_SALES_CHANNEL.PARTNER_DETAILS A
            WHERE COO IS NOT NULL
            ORDER BY BUSINESS_MODEL_STATUS
            )
            WHERE RANK_1 = 1),
            LKP3 AS (
            SELECT Partner_ID as Sold_To_Partner_ID, TRIM(UPPER(Partner_Name)) Partner_Name
            FROM (
            SELECT A.*, ROW_NUMBER() OVER (PARTITION BY UPPER(PARTNER_NAME) ORDER BY BUSINESS_MODEL_STATUS )  AS RANK_1 FROM T2_SALES_CHANNEL.PARTNER_DETAILS A
            ORDER BY BUSINESS_MODEL_STATUS
            )
            WHERE RANK_1 = 1),
            LKP4 AS (
            SELECT Partner_ID as Sold_To_Partner_ID, TRIM(UPPER(COO)) AS COO
            FROM (
            SELECT A.*, ROW_NUMBER() OVER (PARTITION BY UPPER(COO) ORDER BY BUSINESS_MODEL_STATUS) AS RANK_1 FROM T2_SALES_CHANNEL.PARTNER_DETAILS A
            WHERE COO IS NOT NULL
            ORDER BY BUSINESS_MODEL_STATUS
            )
            WHERE RANK_1 = 1
            )

            SELECT
            VALUE_ADD_PARTNER_NAGP,
            USD_STANDARD_MARGIN,
            USD_RECOGNIZABLE_REVENUE,
            USD_EXTENDED_LIST_PRICE,
            USD_EXTENDED_COST,
            USD_EXTENDED_BURDENED_COST,
            USD_BURDENED_MARGIN,
            USD_BILLING_AMOUNT,
            TRIM(UPPER(SOLD_TO_PARTNER_NAGP)) AS SOLD_TO_PARTNER_NAGP,
            SOLD_TO_PARTNER_LEVEL,
            SOLD_TO_CUSTOMER_NAGP,
            SOLD_TO_COUNTRY,
            SO_NUMBER,
            SO_CURRENCY_CODE,
            SO_BILLING_AMOUNT,
            SHIP_TO_CUSTOMER_POSTAL_CODE,
            SHIP_TO_CUSTOMER_NAGP,
            SHIP_TO_CUSTOMER_COUNTRY_CODE,
            SHIP_TO_CUSTOMER_CITY,
            SHIP_TO_CUSTOMER_ADDR_LINE_4,
            SHIP_TO_CUSTOMER_ADDR_LINE_3,
            SHIP_TO_CUSTOMER_ADDR_LINE_2,
            SHIP_TO_CUSTOMER_ADDR_LINE_1,
            SHIP_TO_COUNTRY,
            SALES_REP_NAME,
            SALES_REGION,
            SALES_MULTI_AREA,
            SALES_DISTRICT,
            SALES_CHANNEL_CODE,
            SALES_AREA,
            REVENUE_RECOGNIZED_DATE,
            PRODUCT_TYPE,
            PRODUCT_LINE,
            PRODUCT_FAMILY,
            PRODUCT_CATEGORY,
            MANUAL_INVOICE_FLAG,
            INVOICE_FISCAL_WEEK,
            INVOICE_FISCAL_QTR,
            FISCAL_DATE,
            DISTRIBUTOR_NAGP,
            CE_PART_CATEGORY_TYPE_CODE,
            BOOKED_DATE,
            BILLING_QTY,
            BILL_TO_CUSTOMER_NAGP,
            BILL_TO_COUNTRY,
            AR_INVOICE_NUMBER,
            AR_INVOICE_DATE,
            PARTNER_ID,
            GEOGRAPHY_SHIP_HIERARCHY_KEY,
            GEOGRAPHY_BILL_HIERARCHY_KEY,
            GEOGRAPHY_SOLD_HIERARCHY_KEY,
            SOURCE_TRANSACTION_DATE_KEY,
            AOO_CUSTOMER_AS_IS_KEY,
            DATE_KEY,
            PRODUCT_AS_IS_KEY,
            SOLDTO_PARTNER_AS_IS_KEY,
            BILLTO_CUSTOMER_AS_IS_KEY,
            DISTRIBUTOR_AS_IS_KEY,
            SHIPTO_CUSTOMER_AS_IS_KEY,
            TIER_FLG_2,
            TIER_FLG_1,
            SALES_REP_NUMBER,
            SALES_GEO,
            SOLD_TO_PARTNER_DP_CMAT_ID,
            TRIM(UPPER(SOLD_TO_PARTNER_DP)) AS SOLD_TO_PARTNER_DP,
            DISTRIBUTOR_DP_CMAT_ID,
            DISTRIBUTOR_DP,
            ENTRY_DATE,
            DISTRIBUTOR_PARTNER_ID,
            (CASE WHEN B.PARTNER_NAME iS NOT NULL THEN B.SOLD_TO_PARTNER_ID
                WHEN C.COO IS NOT NULL THEN C.SOLD_TO_PARTNER_ID
                WHEN D.PARTNER_NAME IS NOT NULL THEN D.SOLD_TO_PARTNER_ID
                WHEN E.COO IS NOT NULL THEN E.SOLD_TO_PARTNER_ID
                ELSE NULL
                END) as SOLD_TO_PARTNER_ID
            FROM t2_SALES_CHANNEL.STG_INVOICE A
            LEFT OUTER JOIN
            LKP1 B ON (A.SOLD_TO_PARTNER_DP=B.PARTNER_NAME)
            LEFT OUTER JOIN
            LKP2 C ON (A.SOLD_TO_PARTNER_DP=C.COO)
            LEFT OUTER JOIN
            LKP3 D ON (A.SOLD_TO_PARTNER_NAGP=D.PARTNER_NAME)
            LEFT OUTER JOIN
            LKP4 E ON (A.SOLD_TO_PARTNER_NAGP=E.COO) """

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

            #
            log_file = config['LOG_DIR_NAME'] + "/" + log_filename
            # DB prop Key of Source DB
            db_prop_key_load = config['DB_PROP_KEY_LOAD']
            db_schema = config['DB_SCHEMA']

            #SQL Query
            query = query_data(db_schema)

            # Calling Job Class method --> get_target_data_update()
            Ebi_read_write_obj.get_target_data_update(query,db_prop_key_load)

            end_date="'"+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"'"

            data_format = "JOB START DT : "+start_date+" | SCRIPT NAME : "+script_name+" | JOB : "+app_name+" | SRC COUNT : "+src_count+" | TGT COUNT : "+dest_count+" | JOB END DT : "+end_date+" | STATUS : %(message)s"

            Ebi_read_write_obj.create_log(data_format,log_file,logger)

            logger.info("Success")
            Ebi_read_write_obj.job_debugger_print("	\n  __main__ " + app_name +" --> Job "+app_name+" Succeed \n")

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
