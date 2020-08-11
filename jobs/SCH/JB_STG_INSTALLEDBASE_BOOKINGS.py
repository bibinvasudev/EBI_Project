# SCH1150.SH -->  JB_STG_INSTALLEDBASE_BOOKINGS.py


#*******************************************************************************************************************
#
# Created by   : Vinay Kumbakonam
# Modified by  : bibin
# Version      : 1.1
#
# Description  :
#               1. This script will load the data into 'STG_INSTALLEDBASE_BOOKINGS' table based on stream lookups.
#
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-10-28                    Initial creation
# 2018-11-02                    bibin : Using job_debugger_print() for print any string in Job
#
#*******************************************************************************************************************


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
job_name = "JB_STG_INSTALLEDBASE_BOOKINGS"
script_name = "SCH1150.SH"
app_name = "JB_STG_INSTALLEDBASE_BOOKINGS"
log_file = '/home/spark/EBI_Project/log/' + app_name + '_' + log_date + '.log'


# Query for loading invoice table
def query_data(db_schema):

        query = """
INSERT INTO """+db_schema+""".STG_INSTALLEDBASE_BOOKINGS
(SALES_ORDER_NUMBER
, SERIAL_NUMBER
, SYSTEM_OS_VERSION
, SERIAL_NUMBER_OWNER_NAGP
, SERIAL_NUMBER_OWNER_NAME
, INSTALLED_AT_CUSTOMER_NAGP
, INSTALLED_AT_CUSTOMER_SITE
, SO_NUMBER
, ACCOUNT_NAGP
, SALES_REP_NUMBER
, OPPORTUNITY_END_CUSTOMER
, INSTALL_CUST_NAGP_MATCH_PCT
, SERIAL_OWNER_NAGP_MATCH_PCT
, SERIAL_OWNER_NAME_MATCH_PCT)
SELECT 
a.SALES_ORDER_NUMBER,
a.SERIAL_NUMBER,
a.SYSTEM_OS_VERSION,
a.INSTALLED_AT_CUSTOMER_NAGP,
a.SERIAL_NUMBER_OWNER_NAGP,
a.SERIAL_NUMBER_OWNER_NAME,
a.INSTALLED_AT_CUSTOMER_SITE,
b.SO_NUMBER,
b.ACCOUNT_NAGP,
b.SALES_REP_NUMBER,
b.OPPORTUNITY_END_CUSTOMER,
UTL_MATCH.JARO_WINKLER_SIMILARITY(b.ACCOUNT_NAGP,a.INSTALLED_AT_CUSTOMER_NAGP) INSTALL_CUST_NAGP_MATCH_PCT,
UTL_MATCH.JARO_WINKLER_SIMILARITY(b.ACCOUNT_NAGP,a.SERIAL_NUMBER_OWNER_NAGP) SERIAL_OWNER_NAGP_MATCH_PCT,
UTL_MATCH.JARO_WINKLER_SIMILARITY(b.ACCOUNT_NAGP,a.SERIAL_NUMBER_OWNER_NAME) SERIAL_OWNER_NAME_MATCH_PCT
FROM
"""+db_schema+""".INSTALLEDBASE a,
"""+db_schema+""".INSTALL_BOOKING_LKUP b
WHERE b.SO_NUMBER = a.SALES_ORDER_NUMBER
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
            #db_prop_key_extract =  config['DB_PROP_KEY_EXTRACT']	
            db_schema = config['DB_SCHEMA']

            #SQL Query
            #param_fiscal_year_quarter = """'2019Q1'"""
            query = query_data(db_schema)
						
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


