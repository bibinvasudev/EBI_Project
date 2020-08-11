# SCH1150.SH --> JB_STG_INSTALLEDBASE.py

#**************************************************************************************************************
#
# Created by   : Vinay Kumbakonam
# Modified by  : bibin
# Version      : 1.1
#
# Description  :
#               1. This script will load the data into 'STG_INSTALLEDBASE' table based on stream lookups.
#
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-10-30                    Initial creation
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
job_name = "JB_STG_INSTALL_BASE"
script_name = "SCH1150.SH"
app_name = "JB_STG_INSTALL_BASE"
log_file = '/home/spark/EBI_Project/log/' + app_name + '_' + log_date + '.log'

# Query for loading invoice table
def query_data(schema,param_fiscal_year_quarter):

        query = """

INSERT INTO """+schema+""".STG_INSTALLEDBASE 
(INSTALLED_AT_COUNTRY, INSTALLED_AT_CUSTOMER_NAGP, SERIAL_NUMBER_OWNER_NAME, SALES_ORDER_NUMBER, SYSTEM_OS_VERSION, SERIAL_NUMBER, SHIPPED_DATE, SERIAL_NUMBER_OWNER_NAGP, ENTRY_DATE, DATE_KEY, MONTH_KEY, FISCAL_SHIP_QTR, ACCOUNT_NAGP, INSTALLED_AT_CUSTOMER_SITE, INSTALL_DATE, INSTALLBASE_FISCAL_QTR)
SELECT 
INSTALLED_AT_COUNTRY, INSTALLED_AT_CUSTOMER_NAGP, SERIAL_NUMBER_OWNER_NAME, SALES_ORDER_NUMBER, SYSTEM_OS_VERSION, SERIAL_NUMBER, SHIPPED_DATE, SERIAL_NUMBER_OWNER_NAGP, ENTRY_DATE, DATE_KEY, FISCAL_MONTH_KEY AS MONTH_KEY, FISCAL_YEAR_QTR_TEXT AS FISCAL_SHIP_QTR, ACCOUNT_NAGP, INSTALLED_AT_CUSTOMER_SITE, INSTALL_DATE, INSTALLBASE_FISCAL_QTR
FROM (
WITH DRIVER_TBL_VW AS (
SELECT distinct T800951.COUNTRY_NAME as Installed_At_Country,
              T813247.PROTECTED_NAGP_NAME as Installed_At_Customer_NAGP,
                T799774.PARTY_NAME as serial_Number_Owner_Name,
              T799594.PARTY_NAME as Installed_At_Customer_SITE,
              T801163.ATTRIBUTE_CODE_DESCRIPTION as c4,
              TRUNC(T800383.INSTALL_DATE) as INSTALL_DATE,
              T801142.ATTRIBUTE_CODE_DESCRIPTION as c6,
              T800383.ETL_SALES_ORDER_NUMBER as Sales_Order_Number,
              T800383.OS_VERSION_NUMBER as System_OS_Version,
              T800383.SERIAL_NUMBER as Serial_Number,
              TRUNC(T800383.SHIPPED_DATE) as Shipped_Date,
              T813308.PROTECTED_NAGP_NAME as Serial_Number_Owner_NAGP,
                SYSDATE AS ENTRY_DATE               
         from 
              DIMS.PARTY T799774 /* PARTY_SERIAL_NUMBER_OWNER */ ,
              DIMS.ORGANIZATION_PARTY T813308 /* ORG_PARTY_SERIAL_NUMBER_OWNER */ ,
              DIMS.PARTY T799594 /* PARTY_INSTALLED_AT_CUSTOMER_SITE */ ,
              DIMS.ORGANIZATION_PARTY T813247 /* ORG_PARTY_INSTALLED_AT_CUSTOMER */ ,
              DIMS.INSTALLED_PRODUCT T800383 /* INSTALLED_PRODUCT_IB */ ,
              DIMS.ENTERPRISE_LIST_OF_VALUES T801142 /* LOV_IOBJECT_STATUS_CODE */ ,
              DIMS.ENTERPRISE_LIST_OF_VALUES T801163 /* LOV_ASUP_STATUS_CODE */ ,
              FACTS.ALERT_ITEM_DETAILS T837690,
              FACTS.INSTALL_BASE T799332,
              DIMS.PARTY T800951 /* PARTY_INSTALLED_AT_CUSTOMER */ 
         where ( T799774.ORGANIZATION_PARTY_KEY = T813308.ORGANIZATION_PARTY_KEY and 
                                 T799594.ORGANIZATION_PARTY_KEY = T813247.ORGANIZATION_PARTY_KEY and T799594.PARTY_KEY = T837690.INSTALLED_AT_SITE_KEY and 
                                 T799774.PARTY_KEY = T837690.OWNER_KEY and T800383.ASUP_STATUS_CODE = T801163.ATTRIBUTE_CODE_VALUE and 
                                 T800383.IOBJECT_KEY = T837690.IOBJECT_KEY and T799332.INSTALL_AT_CUSTOMER_KEY = T800951.PARTY_KEY and 
                                 T799332.IOBJECT_KEY = T800383.IOBJECT_KEY and T799332.INSTALL_AT_SITE_KEY = T799594.PARTY_KEY and 
                                 T799332.OWNER_KEY = T799774.PARTY_KEY and T800383.IOBJECT_STATUS_CODE = T801142.ATTRIBUTE_CODE_VALUE and 
                                 (T801163.ATTRIBUTE_ID in (19, 20, 21, 36, 91)) and (T801142.ATTRIBUTE_ID in (8, 9, 10, 11, 33)) 
                                 and T800383.INSTALL_DATE >= (SELECT MIN(FISCAL_QUARTER_START_DATE) FROM T2_SALES_CHANNEL.CALENDAR 
                                 WHERE FISCAL_YEAR_QTR_TEXT IN ("""+ param_fiscal_year_quarter +""")))
                                )
SELECT Z.*, BB.FISCAL_MONTH_KEY, BB.DATE_KEY, BB.FISCAL_YEAR_QTR_TEXT, C.ACCOUNT_NAGP, D.FISCAL_YEAR_QTR_TEXT AS INSTALLBASE_FISCAL_QTR 
FROM DRIVER_TBL_VW Z
LEFT OUTER JOIN 
(
SELECT DATE_KEY, FISCAL_MONTH_KEY,TRUNC(BK_CALENDAR_DATE) BK_CALENDAR_DATE,A.FISCAL_YEAR_QTR_TEXT  
FROM T2_SALES_CHANNEL.CALENDAR a,T2_SALES_CHANNEL.FISCAL_MONTH b
WHERE 
b.BK_FISCAL_MONTH_START_DATE = a.FISCAL_MONTH_START_DATE
AND b.FISCAL_MONTH_END_DATE =  a.FISCAL_MONTH_END_DATE
ORDER BY DATE_KEY
) BB ON (Z.SHIPPED_DATE = BB.BK_CALENDAR_DATE)
LEFT OUTER JOIN 
(
SELECT ACCOUNT_NAGP,BOOKING_FISCAL_QTR,SO_NUMBER,SALES_REP_NUMBER FROM T2_SALES_CHANNEL.BOOKINGS
) C ON (Z.Sales_Order_Number = C.SO_NUMBER AND BB.FISCAL_YEAR_QTR_TEXT = C.BOOKING_FISCAL_QTR)
LEFT OUTER JOIN
(
SELECT TRUNC(BK_CALENDAR_DATE) BK_CALENDAR_DATE,FISCAL_YEAR_QTR_TEXT  FROM T2_SALES_CHANNEL.CALENDAR
) D ON (Z.INSTALL_DATE = D.BK_CALENDAR_DATE)
)

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
            #db_prop_key = 'EBI_EDW04'

            db_prop_key_extract =  config['DB_PROP_KEY_EXTRACT']
            #db_prop_key_extract = 'EBI_EDW04'
            db_schema = config['DB_SCHEMA']
            #db_schema = 'T2_SALES_CHANNEL'
            param_fiscal_year_quarter = Ebi_read_write_obj.get_fiscal_year_quarter(db_schema,db_prop_key_extract)
            Ebi_read_write_obj.job_debugger_print(str(param_fiscal_year_quarter))
            #SQL Query

            #param_fiscal_year_quarter = """'2019Q1'"""
            query = query_data(db_schema, param_fiscal_year_quarter)
            #log = logging.getLogger(app_name)

            # Calling Job Class method --> get_target_data_update()
            Ebi_read_write_obj.get_target_data_update(query,db_prop_key_load)

            end_date="'"+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"'"

            data_format = "JOB START DT : "+start_date+" | SCRIPT NAME : "+script_name+" | JOB : "+job_name+" | SRC COUNT : "+src_count+" | TGT COUNT : "+dest_count+" | JOB END DT : "+end_date+" | STATUS : %(message)s"

            Ebi_read_write_obj.create_log(data_format,log_file,logger)

            logger.info("Success")
            Ebi_read_write_obj.job_debugger_print("     \n  __main__ " + app_name +" --> Job "+job_name+" Succeed \n")

        except Exception as err:
            # Write expeption in spark log or console
            end_date="'"+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"'"
            data_format = "JOB START DT : "+start_date+" | SCRIPT NAME : "+script_name+" | JOB : "+job_name+" | SRC COUNT : "+src_count+" | TGT COUNT : "+dest_count+" | JOB END DT : "+end_date+" | STATUS : %(message)s"
            Ebi_read_write_obj.create_log(data_format,log_file,logger)
            logger.info("[Error] Failed")
            Ebi_read_write_obj.job_debugger_print("     \n Job "+job_name+" Failed\n")
            logger.error("\n __main__ "+ job_name +" --> Exception-Traceback :: " + str(err))
            raise

# Entry point for script
if __name__ == "__main__":
    # Calling main() method
    main()

