# SCH1150.SH --> JB_INSTALLEDBASE.py


#**************************************************************************************************************
#
# Created by  : Vinay Kumbakonam
# Modified by : bibin
# Version      : 1.2
#
# Description  :
#               1. This script will load the data into 'INSTALLEDBASE' table based on stream lookups.
#
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-10-28                    Initial creation
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
script_name = "SCH1150.SH"
app_name = "JB_INSTALLEDBASE"
log_filename = app_name + '_' + log_date + '.log'


# Query for loading invoice table
def query_data(db_schema):

        query = """
INSERT INTO """+db_schema+""".INSTALLEDBASE
( FISCAL_SHIP_QTR
, DATE_KEY
, MONTH_KEY
, ENTRY_DATE
, LAST_ASUP_DATE
, SERIAL_NUMBER_OWNER_NAME
, SERIAL_NUMBER_OWNER_NAGP
, INSTALLED_AT_COUNTRY
, INSTALLED_AT_STATE
, INSTALLED_AT_CITY
, INSTALLED_AT_CUSTOMER_SITE
, INSTALLED_AT_CUSTOMER_NAGP
, SHIPPED_DATE
, SALES_ORDER_NUMBER
, SYSTEM_OS_VERSION
, HOST_NAME
, PRODUCT_FAMILY
, CONTROLLER_FLAG
, MODEL_NAME
, SERIAL_NUMBER
, INSTALL_DATE
, INSTALLBASE_FISCAL_QTR
, CUSTOMER_NAGP_MATCH_PCT
, OWNER_NAGP_MATCH_PCT
, OWNER_NAME_MATCH_PCT)
SELECT 
       FISCAL_SHIP_QTR
,      DATE_KEY
,      MONTH_KEY
,   ENTRY_DATE
,      LAST_ASUP_DATE
,      SERIAL_NUMBER_OWNER_NAME
,      SERIAL_NUMBER_OWNER_NAGP
,      INSTALLED_AT_COUNTRY
,      INSTALLED_AT_STATE
,      INSTALLED_AT_CITY
,      INSTALLED_AT_CUSTOMER_SITE
,      INSTALLED_AT_CUSTOMER_NAGP
,      SHIPPED_DATE
,      SALES_ORDER_NUMBER
,      SYSTEM_OS_VERSION
,      HOST_NAME
,      PRODUCT_FAMILY
,      CONTROLLER_FLAG
,      MODEL_NAME
,      SERIAL_NUMBER
,   INSTALL_DATE
,      INSTALLBASE_FISCAL_QTR
,UTL_MATCH.JARO_WINKLER_SIMILARITY(ACCOUNT_NAGP,INSTALLED_AT_CUSTOMER_NAGP) AS CUSTOMER_NAGP_MATCH_PCT
,UTL_MATCH.JARO_WINKLER_SIMILARITY(ACCOUNT_NAGP,SERIAL_NUMBER_OWNER_NAGP) AS OWNER_NAGP_MATCH_PCT
,UTL_MATCH.JARO_WINKLER_SIMILARITY(ACCOUNT_NAGP,SERIAL_NUMBER_OWNER_NAME) AS OWNER_NAME_MATCH_PCT
FROM """+db_schema+""".STG_INSTALLEDBASE
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


