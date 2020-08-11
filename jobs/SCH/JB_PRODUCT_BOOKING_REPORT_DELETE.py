# SCH1160.py  --> JB_PRODUCT_BOOKING_REPORT_DELETE.py

# **************************************************************************************************************
#
# Created by  : Vijay Nagappa
# Modified by : bibin
# Version      : 1.1
#
# Description  :
#
#
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-10-31                    Initial creation
# 2018-11-02                    bibin : Using job_debugger_print() for print any string in Job
#
# **************************************************************************************************************


# Importing required Lib
# To Add all the Import libraries
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
start_date = "'" + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"
log_date = strftime("%Y%m%d", gmtime())

# Job Naming Details
script_name = "SCH1160.SH"
app_name = 'JB_PRODUCT_BOOKING_REPORT_DELETE'
log_filename = app_name + '_' + log_date + '.log'


# Query for Extract data
def query_data(db_schema):

    query = """DELETE FROM """ +db_schema+ """.PRODUCT_BOOKING_REPORT
             WHERE FISCAL_QTR = '2019Q2' """

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
        spark, config = start_spark(app_name=app_name)

        # Create class Object
        Ebi_read_write_obj = EbiReadWrite(app_name, spark, config, logger)

        #
        log_file = config['LOG_DIR_NAME'] + "/" + log_filename
        # DB prop Key of Source DB
        db_prop_key_load = config['DB_PROP_KEY_LOAD']

        db_schema = config['DB_SCHEMA']

        # SQL Query
        query = query_data(db_schema)

        # Calling Job Class method --> get_target_data_update()
        Ebi_read_write_obj.get_target_data_update(query, db_prop_key_load)

        end_date = "'" + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"

        data_format = "JOB START DT : " + start_date + " | SCRIPT NAME : " + script_name + " | JOB : " + app_name + " | SRC COUNT : " + src_count + " | TGT COUNT : " + dest_count + " | JOB END DT : " + end_date + " | STATUS : %(message)s"

        Ebi_read_write_obj.create_log(data_format, log_file, logger)

        logger.info("Success")
        Ebi_read_write_obj.job_debugger_print("	\n  __main__ " + app_name + " --> Job " + app_name + " Succeed \n")

    except Exception as err:
        # Write expeption in spark log or console
        end_date = "'" + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"
        data_format = "JOB START DT : " + start_date + " | SCRIPT NAME : " + script_name + " | JOB : " + app_name + " | SRC COUNT : " + src_count + " | TGT COUNT : " + dest_count + " | JOB END DT : " + end_date + " | STATUS : %(message)s"
        Ebi_read_write_obj.create_log(data_format, log_file, logger)
        logger.info("[Error] Failed")
        Ebi_read_write_obj.job_debugger_print("     \n Job " + app_name + " Failed\n")
        logger.error("\n __main__ " + app_name + " --> Exception-Traceback :: " + str(err))
        raise

# Entry point for script
if __name__ == "__main__":
    # Calling main() method
    main()

