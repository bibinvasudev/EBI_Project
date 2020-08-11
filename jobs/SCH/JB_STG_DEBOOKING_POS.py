# SCH1101.sh  --> JB_STG_DEBOOKING_POS.py


#**************************************************************************************************************
#
# Created by  : bibin
# Version      : 1.0
#
# Description  :
#               1. This script will load the data into 'STG_DEBOOKING_POS' table based on stream lookups.
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
app_name = "JB_STG_DEBOOKING_POS"
log_filename = app_name + '_' + log_date + '.log'


# Query for loading invoice table
def query_data(db_schema,debooked_fiscal_quarter):

        query = """
        INSERT INTO """+ db_schema +""".STG_DEBOOKING_POS
        (BOOKED_QTR, DEBOOKED_QTR, SO_NUMBER, USD_BOOKING_AMOUNT)
        SELECT BOOKED_QTR, DEBOOKED_QTR, SO_NUMBER, USD_BOOKING_AMOUNT
        FROM
        (
        SELECT b.FISCAL_YEAR_QTR_TEXT BOOKED_QTR,NEXT_QTR_TEXT DEBOOKED_QTR,a.SO_NUMBER,SUM(a.USD_BOOKING_AMOUNT) AS USD_BOOKING_AMOUNT FROM
        t2_sales_channel.BOOKINGS a,
        t2_sales_channel.CALENDAR b, t2_sales_channel.QUARTER_LOOK_UP c
        WHERE
        a.USD_BOOKING_AMOUNT <> 0 AND
         b.DATE_KEY = a.SOURCE_TRANSACTION_DATE_KEY AND c.CURR_QTR_TEXT = b.FISCAL_YEAR_QTR_TEXT
        and c.NEXT_QTR_TEXT = '2019Q2'
        GROUP BY b.FISCAL_YEAR_QTR_TEXT,NEXT_QTR_TEXT,a.SO_NUMBER
        union
        SELECT b.FISCAL_YEAR_QTR_TEXT BOOKED_QTR,NEXT_QTR_TEXT DEBOOKED_QTR,a.SO_NUMBER,SUM(a.USD_BOOKING_AMOUNT) AS USD_BOOKING_AMOUNT FROM
        t2_sales_channel.BOOKINGS a,
        t2_sales_channel.CALENDAR b, t2_sales_channel.QUARTER_LOOK_UP c
        WHERE
        a.USD_BOOKING_AMOUNT <> 0 AND
        b. FISCAL_MONTH_OF_QTR_NUMBER = 1
        AND b.DATE_KEY = a.SOURCE_TRANSACTION_DATE_KEY and c.CURR_QTR_TEXT = b.FISCAL_YEAR_QTR_TEXT
        AND c.CURR_QTR_TEXT = """+ debooked_fiscal_quarter +"""
        GROUP BY b.FISCAL_YEAR_QTR_TEXT,NEXT_QTR_TEXT,a.SO_NUMBER)"""

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
            db_prop_key_extract =  config['DB_PROP_KEY_EXTRACT']	
            db_schema = config['DB_SCHEMA']
            log_file = config['LOG_DIR_NAME'] + "/" + log_filename

            debooked_fiscal_quarter = Ebi_read_write_obj.get_debooked_fiscal_quarter(db_schema,db_prop_key_extract)

            #SQL Query
            query = query_data(db_schema,debooked_fiscal_quarter)
            print(query)

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


