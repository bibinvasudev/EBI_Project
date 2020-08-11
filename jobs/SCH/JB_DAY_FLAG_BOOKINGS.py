# SCH1101.sh  --> JB_DAY_FLAG_BOOKINGS

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
app_name = "JB_DAY_FLAG_BOOKINGS"
log_filename = app_name + '_' + log_date + '.log'

# Oracle details
write_table = 'DAY_FLAG_LOGIC_BOOKINGS'

# Query for loading invoice table
def query_data(db_schema):

	query = """
        INSERT INTO T2_SALES_CHANNEL.DAY_FLAG_LOGIC_BOOKINGS
        SELECT
        PARTNER_ID,
        OPPORTUNITY_END_CUSTOMER,
        SO_NUMBER,
        BOOKED_DATE,
        DAY_0,
        DAY_1,
        DAY_2,
        DAY_3,
        DAY_4,
        DAY_5,
        DAY_6,
        DAY_7
        FROM
        (
        SELECT
        PARTNER_ID,
        OPPORTUNITY_END_CUSTOMER,
        'N' as SO_NUMBER,
        BOOKED_DATE,
        'N' as  DAY_0,
        MAX(CASE WHEN DAY=1 THEN VALUE ELSE 'N' END)  OVER(PARTITION BY PARTNER_ID,OPPORTUNITY_END_CUSTOMER,BOOKED_DATE ORDER BY PARTNER_ID,OPPORTUNITY_END_CUSTOMER,BOOKED_DATE) DAY_1,
        MAX(CASE WHEN DAY=2 THEN VALUE ELSE 'N' END)  OVER(PARTITION BY PARTNER_ID,OPPORTUNITY_END_CUSTOMER,BOOKED_DATE ORDER BY PARTNER_ID,OPPORTUNITY_END_CUSTOMER,BOOKED_DATE) DAY_2,
        MAX(CASE WHEN DAY=3 THEN VALUE ELSE 'N' END)  OVER(PARTITION BY PARTNER_ID,OPPORTUNITY_END_CUSTOMER,BOOKED_DATE ORDER BY PARTNER_ID,OPPORTUNITY_END_CUSTOMER,BOOKED_DATE) DAY_3,
        MAX(CASE WHEN DAY=4 THEN VALUE ELSE 'N' END) OVER(PARTITION BY PARTNER_ID,OPPORTUNITY_END_CUSTOMER,BOOKED_DATE ORDER BY PARTNER_ID,OPPORTUNITY_END_CUSTOMER,BOOKED_DATE)  DAY_4,
        MAX(CASE WHEN DAY=5 THEN VALUE ELSE 'N' END) OVER(PARTITION BY PARTNER_ID,OPPORTUNITY_END_CUSTOMER,BOOKED_DATE ORDER BY PARTNER_ID,OPPORTUNITY_END_CUSTOMER,BOOKED_DATE) DAY_5,
        MAX(CASE WHEN DAY=6 THEN VALUE ELSE 'N' END) OVER(PARTITION BY PARTNER_ID,OPPORTUNITY_END_CUSTOMER,BOOKED_DATE ORDER BY PARTNER_ID,OPPORTUNITY_END_CUSTOMER,BOOKED_DATE) DAY_6,
        MAX(CASE WHEN DAY=7 THEN VALUE ELSE 'N' END) OVER(PARTITION BY PARTNER_ID,OPPORTUNITY_END_CUSTOMER,BOOKED_DATE ORDER BY PARTNER_ID,OPPORTUNITY_END_CUSTOMER,BOOKED_DATE) DAY_7,
        ROW_NUMBER() OVER(PARTITION BY PARTNER_ID,OPPORTUNITY_END_CUSTOMER,BOOKED_DATE ORDER BY PARTNER_ID,OPPORTUNITY_END_CUSTOMER,BOOKED_DATE) DE_DUP
        FROM
        T2_SALES_CHANNEL.BOOKINGS_DAY_FLAG_TEMP
        ) T
        WHERE DE_DUP=1"""
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

