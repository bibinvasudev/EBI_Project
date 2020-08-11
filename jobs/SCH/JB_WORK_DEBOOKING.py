# SCH1101.sh  --> JB_WORK_DEBOOKING.py


#**************************************************************************************************************
#
# Created by  : bibin
# Version      : 1.0
#
# Description  :
#               1. This script will load the data into 'WORK_DEBOOKING' table based on stream lookups.
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
app_name = "JB_WORK_DEBOOKING"
log_filename = app_name + '_' + log_date + '.log'


# Query for loading invoice table
def query_data(db_schema):

        query = """
        INSERT INTO """+ db_schema +""".WORK_DEBOOKING
        (BOOKED_QTR, DEBOOKED_QTR, SO_NUMBER, BOOKED_AMOUNT, DEBOOKED_AMOUNT)
        SELECT BOOKED_QTR, DEBOOKED_QTR, SO_NUMBER, BOOKED_AMOUNT, DEBOOKED_AMOUNT
        FROM
        (
        SELECT
          a.BOOKED_QTR,a.DEBOOKED_QTR,
          a.SO_NUMBER,a.USD_BOOKING_AMOUNT BOOKED_AMOUNT,b.USD_BOOKING_AMOUNT DEBOOKED_AMOUNT
        FROM
          T2_SALES_CHANNEL.STG_DEBOOKING_POS a,
          T2_SALES_CHANNEL.STG_DEBOOKING_POS             b
        WHERE a.USD_BOOKING_AMOUNT = ABS(b.USD_BOOKING_AMOUNT) AND a.SO_NUMBER = b.SO_NUMBER
        AND b.BOOKED_QTR = a.DEBOOKED_QTR
        UNION
        SELECT
          a.BOOKED_QTR,a.DEBOOKED_QTR,
          a.SO_NUMBER,a.USD_BOOKING_AMOUNT BOOKED_AMOUNT,b.USD_BOOKING_AMOUNT DEBOOKED_AMOUNT
        FROM
          T2_SALES_CHANNEL.STG_DEBOOKING_POS a,
          T2_SALES_CHANNEL.STG_DEBOOKING_POS             b
        WHERE ABS(a.USD_BOOKING_AMOUNT) = (b.USD_BOOKING_AMOUNT) AND a.SO_NUMBER = b.SO_NUMBER
        AND b.BOOKED_QTR = a.DEBOOKED_QTR)"""

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

            #SQL Query
            query = query_data(db_schema)
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


