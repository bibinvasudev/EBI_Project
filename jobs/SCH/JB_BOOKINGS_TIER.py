# SCH1101.sh  --> JB_BOOKINGS_TIER.py

#**************************************************************************************************************
#
# Created by  : Vinay Kumbakonam
# Modified by : bibin
# Version      : 1.2
#
# Description  :
#               1. This script will load the data into 'BOOKINGS_TIER' table based on stream lookups.
#
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-10-23                    Initial creation
# 2018-10-30                    bibin : Getting DB schema, db_prop_key_load from Config file
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
app_name = "JB_BOOKINGS_TIER"
log_filename = app_name + '_' + log_date + '.log'

# Query for loading invoice table
def query_data(schema,param_fiscal_year_quarter):

        query = """
INSERT INTO T2_SALES_CHANNEL.BOOKINGS_TIER
(
SOURCE_TRANSACTION_DATE_KEY, AOO_CUSTOMER_AS_IS_KEY, DATE_KEY, PRODUCT_AS_IS_KEY, SOLDTO_PARTNER_AS_IS_KEY, BILLTO_CUSTOMER_AS_IS_KEY, DISTRIBUTOR_AS_IS_KEY, SHIPTO_CUSTOMER_AS_IS_KEY, MISMATCH_INSTALLED_AT_SHIP_TO, INSTALLED_AT_CUSTOMER, PO_NUMBER, INSTALLED_AT_COUNTRY, SERIAL_NUMBER, DEAL_DESK_REASON, ORDER_LINE_STATUS, ORDER_LINE, BOOKING_FISCAL_WEEK_ACTUAL, BOOKING_FISCAL_QTR_ACTUAL, DEAL_DESK_FLAG, ACCOUNT_NAGP, ELA_ESA_CODE, PVR_STATUS, PVR_APPROVAL_ROLE, SO_PRIMARY_RESERVE_CODE, PVR_APPROVED_DATE, QUOTE_CREATED_DATE, QUOTE_NUMBER,
SOLD_TO_PARTNER_DP_CMAT_ID, SOLD_TO_PARTNER_DP, DISTRIBUTOR_DP_CMAT_ID, DISTRIBUTOR_DP, SFDC_RESELLING_NAGP_ID, SFDC_RESELLING_PARTNER, SFDC_DISTRIBUTOR_NAGP_ID, SFDC_DISTRIBUTOR, SALES_GEOGRAPHY, SALES_REP_NUMBER, ENTRY_DATE, SOLD_TO_PARTNER_ID, DISTRIBUTOR_PARTNER_ID, PARTNER_COMPANY_CMAT_ID, DISTRIBUTOR_COMPANY_CMAT_ID, PARTNER_ADD_MATCH, VALUE_ADD_PARTNER_NAGP_CMAT_ID, VALUE_ADD_PARTNER_NAGP, USD_STANDARD_MARGIN, USD_EXTENDED_LIST_PRICE, USD_EXTENDED_DISCOUNT, USD_EXTENDED_COST, USD_EXTENDED_BURDENED_COST, USD_BURDENED_MARGIN,
USD_BOOKING_AMOUNT, SOLD_TO_PARTNER_NAGP_CMAT_ID, SOLD_TO_PARTNER_NAGP, SOLD_TO_PARTNER_LEVEL, SOLD_TO_CUSTOMER_NAGP, SOLD_TO_COUNTRY, SO_NUMBER, SO_CURRENCY_CODE, SO_BOOKING_AMOUNT, SHIPPED_DATE, SHIP_TO_CUSTOMER_POSTAL_CODE, SHIP_TO_CUSTOMER_NAGP, SHIP_TO_CUSTOMER_COUNTRY_CODE, SHIP_TO_CUSTOMER_CITY, SHIP_TO_CUSTOMER_ADDRESS4, SHIP_TO_CUSTOMER_ADDRESS3, SHIP_TO_CUSTOMER_ADDRESS2, SHIP_TO_CUSTOMER_ADDRESS1, SHIP_TO_COUNTRY, SALES_REP_NAME, SALES_REGION, SALES_MULTI_AREA, SALES_DISTRICT, SALES_CHANNEL_CODE, SALES_AREA, PRODUCT_TYPE,
PRODUCT_PARENT_CODE, PRODUCT_LINE, PRODUCT_GROUPING_CODE, PRODUCT_FAMILY, PRODUCT_CATEGORY, PATHWAY_TYPE, OPPORTUNITY_END_CUSTOMER, FISCAL_DATE, DISTRIBUTOR_NAGP_CMAT_ID, DISTRIBUTOR_NAGP, CHANNEL_TYPE, CE_PART_CATEGORY_TYPE_CODE, CANCELLED_FLAG, BOOKING_FISCAL_QTR, BOOKING_FISCAL_WEEK, BOOKED_DATE, BILL_TO_CUSTOMER_NAGP, BILL_TO_COUNTRY, BE_USD_BOOKING_AMOUNT, PARTNER_ID, TIER_FLAG, GEOGRAPHY_SOLD_HIERARCHY_KEY, GEOGRAPHY_BILL_HIERARCHY_KEY, GEOGRAPHY_SHIP_HIERARCHY_KEY, COMMON_PARTNER_ID
)
SELECT
                SOURCE_TRANSACTION_DATE_KEY,
                AOO_CUSTOMER_AS_IS_KEY,
                DATE_KEY,
                PRODUCT_AS_IS_KEY,
                SOLDTO_PARTNER_AS_IS_KEY,
                BILLTO_CUSTOMER_AS_IS_KEY,
                DISTRIBUTOR_AS_IS_KEY,
                SHIPTO_CUSTOMER_AS_IS_KEY,
                MISMATCH_INSTALLED_AT_SHIP_TO,
                INSTALLED_AT_CUSTOMER,
                PO_NUMBER,
                INSTALLED_AT_COUNTRY,
                SERIAL_NUMBER,
                DEAL_DESK_REASON,
                ORDER_LINE_STATUS,
                ORDER_LINE,
                BOOKING_FISCAL_WEEK_ACTUAL,
                BOOKING_FISCAL_QTR_ACTUAL,
                DEAL_DESK_FLAG,
                ACCOUNT_NAGP,
                ELA_ESA_CODE,
                PVR_STATUS,
                PVR_APPROVAL_ROLE,
                SO_PRIMARY_RESERVE_CODE,
                PVR_APPROVED_DATE,
                QUOTE_CREATED_DATE,
                QUOTE_NUMBER,
                SOLD_TO_PARTNER_DP_CMAT_ID,
                SOLD_TO_PARTNER_DP,
                DISTRIBUTOR_DP_CMAT_ID,
                DISTRIBUTOR_DP,
                SFDC_RESELLING_NAGP_ID,
                SFDC_RESELLING_PARTNER,
                SFDC_DISTRIBUTOR_NAGP_ID,
                SFDC_DISTRIBUTOR,
                SALES_GEOGRAPHY,
                SALES_REP_NUMBER,
                ENTRY_DATE,
                SOLD_TO_PARTNER_ID,
                DISTRIBUTOR_PARTNER_ID,
                PARTNER_COMPANY_CMAT_ID,
                DISTRIBUTOR_COMPANY_CMAT_ID,
                PARTNER_ADD_MATCH,
                VALUE_ADD_PARTNER_NAGP_CMAT_ID,
                VALUE_ADD_PARTNER_NAGP,
                USD_STANDARD_MARGIN,
                USD_EXTENDED_LIST_PRICE,
                USD_EXTENDED_DISCOUNT,
                USD_EXTENDED_COST,
                USD_EXTENDED_BURDENED_COST,
                USD_BURDENED_MARGIN,
                USD_BOOKING_AMOUNT,
                SOLD_TO_PARTNER_NAGP_CMAT_ID,
                SOLD_TO_PARTNER_NAGP,
                SOLD_TO_PARTNER_LEVEL,
                SOLD_TO_CUSTOMER_NAGP,
                SOLD_TO_COUNTRY,
                SO_NUMBER,
                SO_CURRENCY_CODE,
                SO_BOOKING_AMOUNT,
                SHIPPED_DATE,
                SHIP_TO_CUSTOMER_POSTAL_CODE,
                SHIP_TO_CUSTOMER_NAGP,
                SHIP_TO_CUSTOMER_COUNTRY_CODE,
                SHIP_TO_CUSTOMER_CITY,
                SHIP_TO_CUSTOMER_ADDRESS4,
                SHIP_TO_CUSTOMER_ADDRESS3,
                SHIP_TO_CUSTOMER_ADDRESS2,
                SHIP_TO_CUSTOMER_ADDRESS1,
                SHIP_TO_COUNTRY,
                SALES_REP_NAME,
                SALES_REGION,
                SALES_MULTI_AREA,
                SALES_DISTRICT,
                SALES_CHANNEL_CODE,
                SALES_AREA,
                PRODUCT_TYPE,
                PRODUCT_PARENT_CODE,
                PRODUCT_LINE,
                PRODUCT_GROUPING_CODE,
                PRODUCT_FAMILY,
                PRODUCT_CATEGORY,
                PATHWAY_TYPE,
                OPPORTUNITY_END_CUSTOMER,
                FISCAL_DATE,
                DISTRIBUTOR_NAGP_CMAT_ID,
                DISTRIBUTOR_NAGP,
                CHANNEL_TYPE,
                CE_PART_CATEGORY_TYPE_CODE,
                CANCELLED_FLAG,
                BOOKING_FISCAL_QTR,
                BOOKING_FISCAL_WEEK,
                BOOKED_DATE,
                BILL_TO_CUSTOMER_NAGP,
                BILL_TO_COUNTRY,
                BE_USD_BOOKING_AMOUNT,
                PARTNER_ID,
                1 AS TIER_FLAG,
                GEOGRAPHY_SOLD_HIERARCHY_KEY,
                GEOGRAPHY_BILL_HIERARCHY_KEY,
                GEOGRAPHY_SHIP_HIERARCHY_KEY,
                PARTNER_ID AS COMMON_PARTNER_ID
FROM T2_SALES_CHANNEL.BOOKINGS
WHERE BOOKING_FISCAL_QTR IN ("""+ param_fiscal_year_quarter +""")
AND TIER_FLG_1='Y'
UNION ALL
SELECT
                SOURCE_TRANSACTION_DATE_KEY,
                AOO_CUSTOMER_AS_IS_KEY,
                DATE_KEY,
                PRODUCT_AS_IS_KEY,
                SOLDTO_PARTNER_AS_IS_KEY,
                BILLTO_CUSTOMER_AS_IS_KEY,
                DISTRIBUTOR_AS_IS_KEY,
                SHIPTO_CUSTOMER_AS_IS_KEY,
                MISMATCH_INSTALLED_AT_SHIP_TO,
                INSTALLED_AT_CUSTOMER,
                PO_NUMBER,
                INSTALLED_AT_COUNTRY,
                SERIAL_NUMBER,
                DEAL_DESK_REASON,
                ORDER_LINE_STATUS,
                ORDER_LINE,
                BOOKING_FISCAL_WEEK_ACTUAL,
                BOOKING_FISCAL_QTR_ACTUAL,
                DEAL_DESK_FLAG,
                ACCOUNT_NAGP,
                ELA_ESA_CODE,
                PVR_STATUS,
                PVR_APPROVAL_ROLE,
                SO_PRIMARY_RESERVE_CODE,
                PVR_APPROVED_DATE,
                QUOTE_CREATED_DATE,
                QUOTE_NUMBER,
                SOLD_TO_PARTNER_DP_CMAT_ID,
                SOLD_TO_PARTNER_DP,
                DISTRIBUTOR_DP_CMAT_ID,
                DISTRIBUTOR_DP,
                SFDC_RESELLING_NAGP_ID,
                SFDC_RESELLING_PARTNER,
                SFDC_DISTRIBUTOR_NAGP_ID,
                SFDC_DISTRIBUTOR,
                SALES_GEOGRAPHY,
                SALES_REP_NUMBER,
                ENTRY_DATE,
                SOLD_TO_PARTNER_ID,
                DISTRIBUTOR_PARTNER_ID,
                PARTNER_COMPANY_CMAT_ID,
                DISTRIBUTOR_COMPANY_CMAT_ID,
                PARTNER_ADD_MATCH,
                VALUE_ADD_PARTNER_NAGP_CMAT_ID,
                VALUE_ADD_PARTNER_NAGP,
                USD_STANDARD_MARGIN,
                USD_EXTENDED_LIST_PRICE,
                USD_EXTENDED_DISCOUNT,
                USD_EXTENDED_COST,
                USD_EXTENDED_BURDENED_COST,
                USD_BURDENED_MARGIN,
                USD_BOOKING_AMOUNT,
                SOLD_TO_PARTNER_NAGP_CMAT_ID,
                SOLD_TO_PARTNER_NAGP,
                SOLD_TO_PARTNER_LEVEL,
                SOLD_TO_CUSTOMER_NAGP,
                SOLD_TO_COUNTRY,
                SO_NUMBER,
                SO_CURRENCY_CODE,
                SO_BOOKING_AMOUNT,
                SHIPPED_DATE,
                SHIP_TO_CUSTOMER_POSTAL_CODE,
                SHIP_TO_CUSTOMER_NAGP,
                SHIP_TO_CUSTOMER_COUNTRY_CODE,
                SHIP_TO_CUSTOMER_CITY,
                SHIP_TO_CUSTOMER_ADDRESS4,
                SHIP_TO_CUSTOMER_ADDRESS3,
                SHIP_TO_CUSTOMER_ADDRESS2,
                SHIP_TO_CUSTOMER_ADDRESS1,
                SHIP_TO_COUNTRY,
                SALES_REP_NAME,
                SALES_REGION,
                SALES_MULTI_AREA,
                SALES_DISTRICT,
                SALES_CHANNEL_CODE,
                SALES_AREA,
                PRODUCT_TYPE,
                PRODUCT_PARENT_CODE,
                PRODUCT_LINE,
                PRODUCT_GROUPING_CODE,
                PRODUCT_FAMILY,
                PRODUCT_CATEGORY,
                PATHWAY_TYPE,
                OPPORTUNITY_END_CUSTOMER,
                FISCAL_DATE,
                DISTRIBUTOR_NAGP_CMAT_ID,
                DISTRIBUTOR_NAGP,
                CHANNEL_TYPE,
                CE_PART_CATEGORY_TYPE_CODE,
                CANCELLED_FLAG,
                BOOKING_FISCAL_QTR,
                BOOKING_FISCAL_WEEK,
                BOOKED_DATE,
                BILL_TO_CUSTOMER_NAGP,
                BILL_TO_COUNTRY,
                BE_USD_BOOKING_AMOUNT,
                PARTNER_ID,
                2 AS TIER_FLAG,
                GEOGRAPHY_SOLD_HIERARCHY_KEY,
                GEOGRAPHY_BILL_HIERARCHY_KEY,
                GEOGRAPHY_SHIP_HIERARCHY_KEY,
                SOLD_TO_PARTNER_ID AS COMMON_PARTNER_ID
FROM T2_SALES_CHANNEL.BOOKINGS
WHERE BOOKING_FISCAL_QTR IN ("""+ param_fiscal_year_quarter +""")
AND TIER_FLG_2='Y'
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
            db_prop_key_load = config['DB_PROP_KEY_LOAD']
            db_prop_key_extract =  config['DB_PROP_KEY_EXTRACT']			
            db_schema = config['DB_SCHEMA']
            log_file = config['LOG_DIR_NAME'] + "/" + log_filename

            param_fiscal_year_quarter = Ebi_read_write_obj.get_fiscal_year_quarter(db_schema,db_prop_key_extract)

            #SQL Query
            query = query_data(db_schema,param_fiscal_year_quarter)

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

