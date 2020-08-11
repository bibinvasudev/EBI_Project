# SCH1101.sh  --> JB_BOOKINGS_DAY_FLAG_TEMP.py

#**************************************************************************************************************
#
# Created by  : Vinay Kumbakonam
# Modified by : bibin
# Version      : 1.2
#
# Description  :
#		1. This script will load the data into 'BOOKINGS_DAY_FLAG_TEMP' tables based on stream lookups.
#
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-10-23                    Initial creation
# 2018-10-30                    Getting DB schema, db_prop_key_load from Config file, log DIR
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
app_name = "JB_BOOKINGS_DAY_FLAG_TEMP"
log_filename = app_name + '_' + log_date + '.log'

# Oracle details
write_table = 'BOOKINGS_DAY_FLAG_TEMP'

# Query for loading invoice table
def query_data(schema):

	query = """
    INSERT INTO T2_SALES_CHANNEL.BOOKINGS_DAY_FLAG_TEMP
    (PARTNER_ID
    , OPPORTUNITY_END_CUSTOMER
    , BOOKED_DATE
    , DAY
    , VALUE)
    SELECT
    B.PARTNER_ID,
    B.OPPORTUNITY_END_CUSTOMER,
    A.BOOKED_DATE,
    (CASE
       WHEN (A.BOOKED_DATE = B.DATE_B1 OR A.BOOKED_DATE = B.DATE_F1 ) THEN 1
                    WHEN (A.BOOKED_DATE = B.DATE_B2 OR A.BOOKED_DATE = B.DATE_F2) THEN 2
                    WHEN (A.BOOKED_DATE = B.DATE_B3 OR A.BOOKED_DATE = B.DATE_F3) THEN 3
                    WHEN (A.BOOKED_DATE = B.DATE_B4 OR A.BOOKED_DATE = B.DATE_F4) THEN 4
                    WHEN (A.BOOKED_DATE = B.DATE_B5 OR A.BOOKED_DATE = B.DATE_F5) THEN 5
                    WHEN (A.BOOKED_DATE = B.DATE_B6 OR A.BOOKED_DATE = B.DATE_F6) THEN 6
                    WHEN (A.BOOKED_DATE = B.DATE_B7 OR A.BOOKED_DATE = B.DATE_F7) THEN 7
                    END) AS DAY,
    (CASE
       WHEN (A.BOOKED_DATE = B.DATE_B1 OR A.BOOKED_DATE = B.DATE_F1) THEN 'Y'
                    WHEN (A.BOOKED_DATE = B.DATE_B2 OR A.BOOKED_DATE = B.DATE_F2) THEN 'Y'
                    WHEN (A.BOOKED_DATE = B.DATE_B3 OR A.BOOKED_DATE = B.DATE_F3) THEN 'Y'
                    WHEN (A.BOOKED_DATE = B.DATE_B4 OR A.BOOKED_DATE = B.DATE_F4) THEN 'Y'
                    WHEN (A.BOOKED_DATE = B.DATE_B5 OR A.BOOKED_DATE = B.DATE_F5) THEN 'Y'
                    WHEN (A.BOOKED_DATE = B.DATE_B6 OR A.BOOKED_DATE = B.DATE_F6) THEN 'Y'
                    WHEN (A.BOOKED_DATE = B.DATE_B7 OR A.BOOKED_DATE = B.DATE_F7) THEN 'Y'
                    END) AS VALUE
    FROM
    (
    SELECT /*+ PARALLEL(A,2) */ DISTINCT
    PARTNER_ID,
    trim(upper(OPPORTUNITY_END_CUSTOMER)) as OPPORTUNITY_END_CUSTOMER,
    trunc(BOOKED_DATE) as BOOKED_DATE
    FROM
    T2_SALES_CHANNEL.WORK_BOOKINGS A
    ) A
    INNER JOIN
    (
    SELECT /*+ PARALLEL(B,2) */ DISTINCT
    PARTNER_ID,
    trim(upper(OPPORTUNITY_END_CUSTOMER)) as OPPORTUNITY_END_CUSTOMER,
    TRUNC(BOOKED_DATE) as BOOKED_DATE,
    TRUNC(BOOKED_DATE-1) AS DATE_B1,
    TRUNC(BOOKED_DATE-2) AS DATE_B2,
    TRUNC(BOOKED_DATE-3) AS DATE_B3,
    TRUNC(BOOKED_DATE-4) AS DATE_B4,
    TRUNC(BOOKED_DATE-5) as DATE_B5,
    TRUNC(BOOKED_DATE-6) as DATE_B6,
    TRUNC(BOOKED_DATE-7) AS DATE_B7,
    TRUNC(BOOKED_DATE+1) AS DATE_F1,
    TRUNC(BOOKED_DATE+2) AS DATE_F2,
    TRUNC(BOOKED_DATE+3) AS DATE_F3,
    TRUNC(BOOKED_DATE+4) AS DATE_F4,
    TRUNC(BOOKED_DATE+5) as DATE_F5,
    TRUNC(BOOKED_DATE+6) as DATE_F6,
    TRUNC(BOOKED_DATE+7) AS DATE_F7
    FROM
    T2_SALES_CHANNEL.WORK_BOOKINGS B ) B
    ON A.PARTNER_ID = B.PARTNER_ID AND
       A.OPPORTUNITY_END_CUSTOMER = B.OPPORTUNITY_END_CUSTOMER AND
       (A.BOOKED_DATE = B.DATE_B1 OR A.BOOKED_DATE = B.DATE_B2 OR A.BOOKED_DATE = B.DATE_B3 OR A.BOOKED_DATE = B.DATE_B4 OR
        A.BOOKED_DATE = B.DATE_B5 OR A.BOOKED_DATE = B.DATE_B6 OR A.BOOKED_DATE = B.DATE_B7 OR
        A.BOOKED_DATE = B.DATE_F1 OR A.BOOKED_DATE = B.DATE_F2 OR A.BOOKED_DATE = B.DATE_F3 OR A.BOOKED_DATE = B.DATE_F4 OR
        A.BOOKED_DATE = B.DATE_F5 OR A.BOOKED_DATE = B.DATE_F6 OR A.BOOKED_DATE = B.DATE_F7)"""

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

