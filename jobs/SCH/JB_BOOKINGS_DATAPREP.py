# SCH1101.sh  --> JB_BOOKINGS_DATAPREP.py


#**************************************************************************************************************
#
# Created by  : Vinay Kumbakonam
# Modified by : bibin
# Version      : 1.1
#
# Description  :
#               1. This script will load the data into 'BOOKINGS_DATAPREP' table based on stream lookups.
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
app_name = "JB_BOOKINGS_DATAPREP"
log_filename = app_name + '_' + log_date + '.log'


# Query for loading invoice table
def query_data(db_schema):

        query = """
INSERT INTO """+ db_schema +""".BOOKINGS_DATAPREP
(ACCOUNT_NAGP, AOO_CUSTOMER_AS_IS_KEY, BE_USD_BOOKING_AMOUNT, BILL_TO_COUNTRY, BILL_TO_CUSTOMER_NAGP, BILLTO_CUSTOMER_AS_IS_KEY, BOOKED_DATE, CANCELLED_FLAG, CE_PART_CATEGORY_TYPE_CODE, CHANNEL_TYPE, DISTRIBUTOR_AS_IS_KEY, DISTRIBUTOR_COMPANY_CMAT_ID, DISTRIBUTOR_DP, DISTRIBUTOR_DP_CMAT_ID, DISTRIBUTOR_NAGP, DISTRIBUTOR_NAGP_CMAT_ID, DISTRIBUTOR_PARTNER_ID, ELA_ESA_CODE, ENTRY_DATE, FISCAL_DATE, INSTALLED_AT_COUNTRY, INSTALLED_AT_CUSTOMER, MISMATCH_INSTALLED_AT_SHIP_TO, OPPORTUNITY_END_CUSTOMER, ORDER_LINE, ORDER_LINE_STATUS, PARTNER_COMPANY_CMAT_ID, PARTNER_ID, PATHWAY_TYPE, PO_NUMBER, PRODUCT_AS_IS_KEY, PRODUCT_CATEGORY, PRODUCT_FAMILY, PRODUCT_GROUPING_CODE, PRODUCT_LINE, PRODUCT_PARENT_CODE, PRODUCT_TYPE, PVR_APPROVAL_ROLE, PVR_APPROVED_DATE, PVR_STATUS, QUOTE_CREATED_DATE, QUOTE_NUMBER, SALES_AREA, SALES_CHANNEL_CODE, SALES_DISTRICT, SALES_GEOGRAPHY, SALES_MULTI_AREA, SALES_REGION, SALES_REP_NAME, SALES_REP_NUMBER, SERIAL_NUMBER, SFDC_DISTRIBUTOR, SFDC_DISTRIBUTOR_NAGP_ID, SFDC_RESELLING_NAGP_ID, SFDC_RESELLING_PARTNER, SHIP_TO_COUNTRY, SHIP_TO_CUSTOMER_ADDRESS1, SHIP_TO_CUSTOMER_ADDRESS2, SHIP_TO_CUSTOMER_ADDRESS3, SHIP_TO_CUSTOMER_ADDRESS4, SHIP_TO_CUSTOMER_CITY, SHIP_TO_CUSTOMER_COUNTRY_CODE, SHIP_TO_CUSTOMER_NAGP, SHIP_TO_CUSTOMER_POSTAL_CODE, SHIPPED_DATE, SHIPTO_CUSTOMER_AS_IS_KEY, SO_BOOKING_AMOUNT, SO_CURRENCY_CODE, SO_NUMBER, SO_PRIMARY_RESERVE_CODE, SOLD_TO_COUNTRY, SOLD_TO_CUSTOMER_NAGP, SOLD_TO_PARTNER_DP, SOLD_TO_PARTNER_DP_CMAT_ID, SOLD_TO_PARTNER_ID, SOLD_TO_PARTNER_LEVEL, SOLD_TO_PARTNER_NAGP, SOLD_TO_PARTNER_NAGP_CMAT_ID, SOLDTO_PARTNER_AS_IS_KEY, SOURCE_TRANSACTION_DATE_KEY, TIER_FLG_2, TIER_FLG_1, USD_BOOKING_AMOUNT, USD_BURDENED_MARGIN, USD_EXTENDED_BURDENED_COST, USD_EXTENDED_COST, USD_EXTENDED_DISCOUNT, USD_EXTENDED_LIST_PRICE, USD_STANDARD_MARGIN, VALUE_ADD_PARTNER_NAGP, VALUE_ADD_PARTNER_NAGP_CMAT_ID, PARTNER_ADD_MATCH, DEBOOKED_FLAG, GEOGRAPHY_SOLD_HIERARCHY_KEY, GEOGRAPHY_BILL_HIERARCHY_KEY, GEOGRAPHY_SHIP_HIERARCHY_KEY, PARTNER_TERRITORY, BOOKING_FISCAL_WEEK, BOOKING_FISCAL_QTR)
SELECT 
                ACCOUNT_NAGP
,               AOO_CUSTOMER_AS_IS_KEY
,               BE_USD_BOOKING_AMOUNT
,               BILL_TO_COUNTRY
,               BILL_TO_CUSTOMER_NAGP
,               BILLTO_CUSTOMER_AS_IS_KEY
,               TRUNC(BOOKED_DATE) AS BOOKED_DATE
,               CANCELLED_FLAG
,               CE_PART_CATEGORY_TYPE_CODE
,               CHANNEL_TYPE
,               DISTRIBUTOR_AS_IS_KEY
,               DISTRIBUTOR_COMPANY_CMAT_ID
,               DISTRIBUTOR_DP
,               DISTRIBUTOR_DP_CMAT_ID
,               DISTRIBUTOR_NAGP
,               DISTRIBUTOR_NAGP_CMAT_ID
,               DISTRIBUTOR_PARTNER_ID
,               ELA_ESA_CODE
,               A.ENTRY_DATE
,               trunc(FISCAL_DATE) as FISCAL_DATE
,               INSTALLED_AT_COUNTRY
,               INSTALLED_AT_CUSTOMER
,               MISMATCH_INSTALLED_AT_SHIP_TO
,               OPPORTUNITY_END_CUSTOMER
,               ORDER_LINE
,               ORDER_LINE_STATUS
,               PARTNER_COMPANY_CMAT_ID
,               A.PARTNER_ID
,               PATHWAY_TYPE
,               PO_NUMBER
,               PRODUCT_AS_IS_KEY
,               PRODUCT_CATEGORY
,               PRODUCT_FAMILY
,               PRODUCT_GROUPING_CODE
,               PRODUCT_LINE
,               PRODUCT_PARENT_CODE
,               PRODUCT_TYPE
,               PVR_APPROVAL_ROLE
,               PVR_APPROVED_DATE
,               PVR_STATUS
,               TRUNC(QUOTE_CREATED_DATE) AS QUOTE_CREATED_DATE
,               QUOTE_NUMBER
,               SALES_AREA
,               SALES_CHANNEL_CODE
,               SALES_DISTRICT
,               SALES_GEOGRAPHY
,               SALES_MULTI_AREA
,               SALES_REGION
,               SALES_REP_NAME
,               SALES_REP_NUMBER
,               SERIAL_NUMBER
,               SFDC_DISTRIBUTOR
,               SFDC_DISTRIBUTOR_NAGP_ID
,               SFDC_RESELLING_NAGP_ID
,               SFDC_RESELLING_PARTNER
,               SHIP_TO_COUNTRY
,               SHIP_TO_CUSTOMER_ADDRESS1
,               SHIP_TO_CUSTOMER_ADDRESS2
,               SHIP_TO_CUSTOMER_ADDRESS3
,               SHIP_TO_CUSTOMER_ADDRESS4
,               SHIP_TO_CUSTOMER_CITY
,               SHIP_TO_CUSTOMER_COUNTRY_CODE
,               SHIP_TO_CUSTOMER_NAGP
,               SHIP_TO_CUSTOMER_POSTAL_CODE
,               SHIPPED_DATE
,               SHIPTO_CUSTOMER_AS_IS_KEY
,               SO_BOOKING_AMOUNT
,               SO_CURRENCY_CODE
,               SO_NUMBER
,               SO_PRIMARY_RESERVE_CODE
,               SOLD_TO_COUNTRY
,               SOLD_TO_CUSTOMER_NAGP
,               SOLD_TO_PARTNER_DP
,               SOLD_TO_PARTNER_DP_CMAT_ID
,               SOLD_TO_PARTNER_ID
,               SOLD_TO_PARTNER_LEVEL
,               SOLD_TO_PARTNER_NAGP
,               SOLD_TO_PARTNER_NAGP_CMAT_ID
,               SOLDTO_PARTNER_AS_IS_KEY
,               SOURCE_TRANSACTION_DATE_KEY
,               case when a.PARTNER_ID is not null and SOLD_TO_PARTNER_ID is not null and A.PARTNER_ID <> SOLD_TO_PARTNER_ID then 'Y' else 'N' end as TIER_FLG_2
,               case when a.PARTNER_ID is not null then 'Y' 
                                WHEN A.PARTNER_ID IS NULL AND SOLD_TO_PARTNER_ID IS NOT NULL THEN 'Y' ELSE 'N' END AS TIER_FLG_1
,               USD_BOOKING_AMOUNT
,               USD_BURDENED_MARGIN
,               USD_EXTENDED_BURDENED_COST
,               USD_EXTENDED_COST
,               USD_EXTENDED_DISCOUNT
,               USD_EXTENDED_LIST_PRICE
,               USD_STANDARD_MARGIN
,               VALUE_ADD_PARTNER_NAGP
,               VALUE_ADD_PARTNER_NAGP_CMAT_ID
,UTL_MATCH.JARO_WINKLER_SIMILARITY(Address_Line1 || ' ' ||   
  Address_Line2 || ' ' ||   
  Address_Line3 || ' ' ||   
  City || ' ' ||   
  Country || ' ' ||   
  Zip,Case When Ship_To_Customer_Address1 = 'UNKNOWN' Then '' Else Ship_To_Customer_Address1 End || ' ' ||   
 Case When Ship_To_Customer_Address2 = 'UNKNOWN' Then '' Else Ship_To_Customer_Address2 End || ' ' ||   
 Case When Ship_To_Customer_Address3 = 'UNKNOWN' Then '' Else Ship_To_Customer_Address3 End || ' ' ||   
 Case When Ship_To_Customer_Address4 = 'UNKNOWN' Then '' Else Ship_To_Customer_Address4 End || ' ' ||   
 Case When Ship_To_Customer_City = 'UNKNOWN' Then '' Else Ship_To_Customer_City End || ' ' ||  
 Case When Ship_To_Country = 'UNKNOWN' Then '' Else Ship_To_Country End || ' ' ||  
 case when SHIP_TO_CUSTOMER_POSTAL_CODE = 'UNKNOWN' then '' else SHIP_TO_CUSTOMER_POSTAL_CODE end) as            PARTNER_ADD_MATCH,
'N' as DEBOOKED_FLAG,
GEOGRAPHY_SOLD_HIERARCHY_KEY,
GEOGRAPHY_BILL_HIERARCHY_KEY,
GEOGRAPHY_SHIP_HIERARCHY_KEY,
CASE WHEN C.PARTNER_SELLING_TERRITORY IS NULL THEN A.SHIP_TO_COUNTRY ELSE 'Null' END AS PARTNER_TERRITORY,
BOOKING_FISCAL_WEEK,
BOOKING_FISCAL_QTR
from T2_SALES_CHANNEL.WORK_BOOKINGS A
LEFT JOIN T2_SALES_CHANNEL.partner_details B
ON  A.PARTNER_ID = B.PARTNER_ID
LEFT JOIN T2_SALES_CHANNEL.PARTNER_SELLING_TERRITORY C
ON A.PARTNER_ID = C.PARTNER_ID
AND TRIM(UPPER(A.SHIP_TO_COUNTRY)) = TRIM(UPPER(C.PARTNER_SELLING_TERRITORY))

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
            db_schema = config['DB_SCHEMA']
            log_file = config['LOG_DIR_NAME'] + "/" + log_filename

            #SQL Query
            #param_fiscal_year_quarter = """'2019Q1'"""
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


