# SCH1101.sh  --> JB_PARTNER_PRODUCT.py


#**************************************************************************************************************
#
# Created by  : bibin
# Version      : 1.0
#
# Description  :
#               1. This script will load the data into 'PARTNER_PRODUCT' table based on stream lookups.
#
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-10-31                    Initial creation
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
app_name = "JB_PARTNER_PRODUCT"
log_filename = app_name + '_' + log_date + '.log'


# Query for loading invoice table
def query_data(db_schema):

        query = """
        INSERT INTO """+ db_schema +""".PARTNER_PRODUCT
        (FISCAL_YEAR_QTR_TEXT, HOME_GEO, COUNTRY, PARTNER_NAME, PRODUCT_FAMILY_NAME, SO_NUMBER, FLAG, USD_BOOKING_AMOUNT, USD_EXTENDED_LIST_PRICE, USD_EXTENDED_DISCOUNT, DISCOUNT_PERCENTAGE, DISC_LS_FIFTY, DISC_FIFTY_TO_SEVENTY, DISC_SEVENTY_TO_EIGHTY, DISC_GT_EIGHTY)
        SELECT FISCAL_YEAR_QTR_TEXT, HOME_GEO, COUNTRY, PARTNER_NAME, PRODUCT_FAMILY_NAME, SO_NUMBER, FLAG, USD_BOOKING_AMOUNT, USD_EXTENDED_LIST_PRICE, USD_EXTENDED_DISCOUNT, DISCOUNT_PERCENTAGE, DISC_LS_FIFTY, DISC_FIFTY_TO_SEVENTY, DISC_SEVENTY_TO_EIGHTY, DISC_GT_EIGHTY
        FROM
        (
        SELECT a.*,
        ROUND((USD_EXTENDED_DISCOUNT/USD_EXTENDED_LIST_PRICE) * 100,0) DISCOUNT_PERCENTAGE,
        CASE WHEN ROUND((USD_EXTENDED_DISCOUNT/USD_EXTENDED_LIST_PRICE) * 100,0) < 50 THEN 1 ELSE 0 END DISC_LS_FIFTY,
        CASE WHEN ( ROUND((USD_EXTENDED_DISCOUNT/USD_EXTENDED_LIST_PRICE) * 100,0) >= 50 AND
             ROUND((USD_EXTENDED_DISCOUNT/USD_EXTENDED_LIST_PRICE) * 100,0) < 70 ) THEN 1
        ELSE 0 END DISC_FIFTY_TO_SEVENTY,
        CASE WHEN ( ROUND((USD_EXTENDED_DISCOUNT/USD_EXTENDED_LIST_PRICE) * 100,0) >= 70 AND
             ROUND((USD_EXTENDED_DISCOUNT/USD_EXTENDED_LIST_PRICE) * 100,0) < 80 ) THEN 1
        ELSE 0 END DISC_SEVENTY_TO_EIGHTY,
        CASE WHEN ROUND((USD_EXTENDED_DISCOUNT/USD_EXTENDED_LIST_PRICE) * 100,0) >= 80 THEN 1 ELSE 0 END DISC_GT_EIGHTY
        FROM T2_SALES_CHANNEL.STG_PARTNER_PRODUCT a
        WHERE
        (USD_EXTENDED_LIST_PRICE <> 0 AND USD_EXTENDED_DISCOUNT <> 0)) """

        return query


#  Main method
def main():
        try:
            src_count = '0'
            dest_count = '0'

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
            query = query_data(db_schema)
            Ebi_read_write_obj.job_debugger_print(query)

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


