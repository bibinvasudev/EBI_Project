# SCH1101.sh  --> JB_STG_BOOKINGS.py

#**************************************************************************************************************
#
# Created by  : Vinay Kumbakonam
# Modified by : bibin
# Version      : 1.2
#
# Description  :
#		1. This script will load the data into 'STG_BOOKINGS' table based on stream lookups.
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
app_name = "JB_STG_BOOKINGS"
log_filename = app_name + '_' + log_date + '.log'

# Query for loading invoice table
def query_data(db_schema):

	query = """
        INSERT INTO """+ db_schema +""".STG_BOOKINGS
        (
        USD_STANDARD_MARGIN
        , USD_EXTENDED_LIST_PRICE
        , USD_EXTENDED_DISCOUNT
        , USD_EXTENDED_COST
        , USD_EXTENDED_BURDENED_COST
        , USD_BURDENED_MARGIN
        , USD_BOOKING_AMOUNT
        , SO_BOOKING_AMOUNT
        , BE_USD_BOOKING_AMOUNT
        , BILL_TO_CUSTOMER_NAGP
        , BILL_TO_COUNTRY
        , CHANNEL_TYPE
        , DISTRIBUTOR_COMPANY_CMAT_ID
        , DISTRIBUTOR_NAGP_CMAT_ID
        , DISTRIBUTOR_NAGP
        , ACCOUNT_NAGP
        , FISCAL_DATE
        , OPPORTUNITY_END_CUSTOMER
        , SFDC_DISTRIBUTOR_NAGP_ID
        , SFDC_DISTRIBUTOR
        , SFDC_RESELLING_NAGP_ID
        , SFDC_RESELLING_PARTNER
        , PATHWAY_TYPE
        , CE_PART_CATEGORY_TYPE_CODE
        , PRODUCT_CATEGORY
        , PRODUCT_FAMILY
        , PRODUCT_LINE
        , PRODUCT_TYPE
        , PVR_APPROVED_DATE
        , PVR_APPROVAL_ROLE
        , PVR_STATUS
        , QUOTE_CREATED_DATE
        , QUOTE_NUMBER
        , SO_CURRENCY_CODE
        , CANCELLED_FLAG
        , BOOKED_DATE
        , PO_NUMBER
        , SO_NUMBER
        , SO_PRIMARY_RESERVE_CODE
        , ELA_ESA_CODE
        , ORDER_LINE
        , ORDER_LINE_STATUS
        , PRODUCT_GROUPING_CODE
        , PRODUCT_PARENT_CODE
        , SHIPPED_DATE
        , SALES_REP_NAME
        , SALES_REP_NUMBER
        , SALES_AREA
        , SALES_DISTRICT
        , SALES_GEOGRAPHY
        , SALES_MULTI_AREA
        , SALES_REGION
        , SHIP_TO_CUSTOMER_ADDRESS1
        , SHIP_TO_CUSTOMER_ADDRESS2, SHIP_TO_CUSTOMER_ADDRESS3, SHIP_TO_CUSTOMER_ADDRESS4, SHIP_TO_CUSTOMER_CITY, SHIP_TO_CUSTOMER_NAGP, SHIP_TO_CUSTOMER_POSTAL_CODE, SHIP_TO_COUNTRY, SOLD_TO_CUSTOMER_NAGP, SOLD_TO_COUNTRY, PARTNER_COMPANY_CMAT_ID, SOLD_TO_PARTNER_LEVEL, SOLD_TO_PARTNER_NAGP_CMAT_ID, SOLD_TO_PARTNER_NAGP, VALUE_ADD_PARTNER_NAGP_CMAT_ID, VALUE_ADD_PARTNER_NAGP, SHIPTO_CUSTOMER_AS_IS_KEY, DISTRIBUTOR_AS_IS_KEY, BILLTO_CUSTOMER_AS_IS_KEY, SOLDTO_PARTNER_AS_IS_KEY, PRODUCT_AS_IS_KEY, SOURCE_TRANSACTION_DATE_KEY, AOO_CUSTOMER_AS_IS_KEY, SOLD_TO_PARTNER_DP, SOLD_TO_PARTNER_DP_CMAT_ID, DISTRIBUTOR_DP, DISTRIBUTOR_DP_CMAT_ID, SALES_CHANNEL_CODE, GEOGRAPHY_SOLD_HIERARCHY_KEY, GEOGRAPHY_BILL_HIERARCHY_KEY,GEOGRAPHY_SHIP_HIERARCHY_KEY, ENTRY_DATE, PARTNER_ID, DISTRIBUTOR_PARTNER_ID
        )WITH LKP1 as (
        SELECT Partner_ID as Partner_ID,Partner_ID as Distributor_Partner_ID , TRIM(UPPER(Partner_Name)) Partner_Name
        FROM (
        SELECT A.*, ROW_NUMBER() OVER (PARTITION BY UPPER(PARTNER_NAME) ORDER BY BUSINESS_MODEL_STATUS )  AS RN1
        FROM T2_SALES_CHANNEL.PARTNER_DETAILS A
        ORDER BY BUSINESS_MODEL_STATUS )
        WHERE RN1 = 1),
        LKP2 AS (
        SELECT Partner_ID as Partner_ID,Partner_ID as Distributor_Partner_ID , cast(CMAT_ID as integer) CMAT_ID
        FROM (
        SELECT A.*, ROW_NUMBER() OVER (PARTITION BY CMAT_ID ORDER BY BUSINESS_MODEL_STATUS )  AS RN1
        FROM T2_SALES_CHANNEL.PARTNER_DETAILS A WHERE CMAT_ID is not null
        ORDER BY BUSINESS_MODEL_STATUS
        )
        WHERE RN1 = 1),
        LKP3 AS (
        SELECT Partner_ID as Partner_ID,Partner_ID as Distributor_Partner_ID , TRIM(UPPER(COO)) AS COO
        FROM (
        SELECT A.*, ROW_NUMBER() OVER (PARTITION BY UPPER(COO) ORDER BY BUSINESS_MODEL_STATUS ) AS RN1
        FROM T2_SALES_CHANNEL.PARTNER_DETAILS A WHERE COO is not null
        ORDER BY BUSINESS_MODEL_STATUS
        )
        WHERE RN1 = 1),
        LKP4 AS(
        SELECT Partner_ID as Partner_ID,Partner_ID as Distributor_Partner_ID , TRIM(UPPER(COO)) AS COO
        FROM (
        SELECT A.*, ROW_NUMBER() OVER (PARTITION BY UPPER(COO) ORDER BY BUSINESS_MODEL_STATUS )  AS RN1
        FROM T2_SALES_CHANNEL.PARTNER_DETAILS A WHERE COO is not null
        ORDER BY BUSINESS_MODEL_STATUS
        )
        WHERE RN1 = 1),
        LKP5 AS(
        SELECT Partner_ID as Partner_ID,Partner_ID as Distributor_Partner_ID , TRIM(UPPER(COO)) AS COO
        FROM (
        SELECT A.*, ROW_NUMBER() OVER (PARTITION BY UPPER(COO) ORDER BY BUSINESS_MODEL_STATUS )  AS RN1
        FROM T2_SALES_CHANNEL.PARTNER_DETAILS A WHERE COO is not null
        ORDER BY BUSINESS_MODEL_STATUS
        )
        WHERE RN1 = 1)
        SELECT A.USD_STANDARD_MARGIN, A.USD_EXTENDED_LIST_PRICE, A.USD_EXTENDED_DISCOUNT, A.USD_EXTENDED_COST, A.USD_EXTENDED_BURDENED_COST, A.USD_BURDENED_MARGIN, A.USD_BOOKING_AMOUNT, A.SO_BOOKING_AMOUNT, A.BE_USD_BOOKING_AMOUNT, A.BILL_TO_CUSTOMER_NAGP, A.BILL_TO_COUNTRY, A.CHANNEL_TYPE, A.DISTRIBUTOR_COMPANY_CMAT_ID, A.DISTRIBUTOR_NAGP_CMAT_ID, A.DISTRIBUTOR_NAGP, A.ACCOUNT_NAGP, A.FISCAL_DATE, A.OPPORTUNITY_END_CUSTOMER, A.SFDC_DISTRIBUTOR_NAGP_ID, A.SFDC_DISTRIBUTOR, A.SFDC_RESELLING_NAGP_ID, A.SFDC_RESELLING_PARTNER, A.PATHWAY_TYPE, A.CE_PART_CATEGORY_TYPE_CODE, A.PRODUCT_CATEGORY, A.PRODUCT_FAMILY, A.PRODUCT_LINE, A.PRODUCT_TYPE, A.PVR_APPROVED_DATE, A.PVR_APPROVAL_ROLE, A.PVR_STATUS, A.QUOTE_CREATED_DATE, A.QUOTE_NUMBER, A.SO_CURRENCY_CODE, A.CANCELLED_FLAG, A.BOOKED_DATE, A.PO_NUMBER, A.SO_NUMBER, A.SO_PRIMARY_RESERVE_CODE, A.ELA_ESA_CODE, A.ORDER_LINE, A.ORDER_LINE_STATUS, A.PRODUCT_GROUPING_CODE, A.PRODUCT_PARENT_CODE, A.SHIPPED_DATE, A.SALES_REP_NAME, A.SALES_REP_NUMBER, A.SALES_AREA, A.SALES_DISTRICT, A.SALES_GEOGRAPHY, A.SALES_MULTI_AREA, A.SALES_REGION, A.SHIP_TO_CUSTOMER_ADDRESS1, A.SHIP_TO_CUSTOMER_ADDRESS2, A.SHIP_TO_CUSTOMER_ADDRESS3, A.SHIP_TO_CUSTOMER_ADDRESS4, A.SHIP_TO_CUSTOMER_CITY, A.SHIP_TO_CUSTOMER_NAGP, A.SHIP_TO_CUSTOMER_POSTAL_CODE, A.SHIP_TO_COUNTRY, A.SOLD_TO_CUSTOMER_NAGP, A.SOLD_TO_COUNTRY, A.PARTNER_COMPANY_CMAT_ID, A.SOLD_TO_PARTNER_LEVEL, A.SOLD_TO_PARTNER_NAGP_CMAT_ID, A.SOLD_TO_PARTNER_NAGP, A.VALUE_ADD_PARTNER_NAGP_CMAT_ID, A.VALUE_ADD_PARTNER_NAGP, A.SHIPTO_CUSTOMER_AS_IS_KEY, A.DISTRIBUTOR_AS_IS_KEY, A.BILLTO_CUSTOMER_AS_IS_KEY, A.SOLDTO_PARTNER_AS_IS_KEY, A.PRODUCT_AS_IS_KEY, A.SOURCE_TRANSACTION_DATE_KEY, A.AOO_CUSTOMER_AS_IS_KEY, A.SOLD_TO_PARTNER_DP, A.SOLD_TO_PARTNER_DP_CMAT_ID, A.DISTRIBUTOR_DP, A.DISTRIBUTOR_DP_CMAT_ID, A.SALES_CHANNEL_CODE, A.GEOGRAPHY_SOLD_HIERARCHY_KEY, A.GEOGRAPHY_BILL_HIERARCHY_KEY, A.GEOGRAPHY_SHIP_HIERARCHY_KEY, A.ENTRY_DATE,
        (CASE
            WHEN B.PARTNER_NAME iS NOT NULL THEN B.PARTNER_ID
            WHEN C.CMAT_ID IS NOT NULL THEN C.PARTNER_ID
            WHEN D.COO IS NOT NULL THEN D.PARTNER_ID
            WHEN E.COO IS NOT NULL THEN E.PARTNER_ID
                WHEN F.COO IS NOT NULL THEN F.PARTNER_ID
            ELSE NULL
            END) as PARTNER_ID,
        (CASE
            WHEN B.PARTNER_NAME iS NOT NULL THEN B.DISTRIBUTOR_PARTNER_ID
            WHEN C.CMAT_ID IS NOT NULL THEN C.DISTRIBUTOR_PARTNER_ID
            WHEN D.COO IS NOT NULL THEN D.DISTRIBUTOR_PARTNER_ID
            WHEN E.COO IS NOT NULL THEN E.DISTRIBUTOR_PARTNER_ID
                WHEN F.COO IS NOT NULL THEN F.DISTRIBUTOR_PARTNER_ID
            ELSE NULL
            END) AS DISTRIBUTOR_PARTNER_ID
         FROM T2_SALES_CHANNEL.WORK_BOOKINGS_DATAPREP A
        LEFT OUTER JOIN
        LKP1 B ON (A.Distributor_DP=B.PARTNER_NAME)
        LEFT OUTER JOIN
        LKP2 C ON (A.Distributor_Company_CMAT_ID=C.CMAT_ID)
        LEFT OUTER JOIN
        LKP3 D ON (A.DISTRIBUTOR_DP=D.COO)
        LEFT OUTER JOIN
        LKP4 E ON (A.SFDC_Distributor=E.COO)
        LEFT OUTER JOIN
        LKP5 F ON (A.DISTRIBUTOR_NAGP=F.COO)"""

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

            log_file = config['LOG_DIR_NAME'] + "/" + log_filename
            # DB prop Key of Source DB
            db_prop_key_load = config['DB_PROP_KEY_LOAD']
            #db_prop_key_load = 'EBI_EDW07'
            db_schema = config['DB_SCHEMA']
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

