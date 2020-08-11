# SCH2003.sh  --> JB_SR_GOAL.py

# **************************************************************************************************************
#
# Created by  : Bibin Vasudevan
# Version      : 1.0
#
# Description  :
# 1. This script will data into 'SR_GOAL' table.
#
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-11-05                    Initial creation
#
# **************************************************************************************************************


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
log_date = strftime("%Y%m%d", gmtime())

# Job Naming Details
script_name = "SCH2003.SH"
app_name = "JB_SR_GOAL"
log_filename = app_name + '_' + log_date + '.log'


# Query for loading invoice table
def query_data(db_schema):
    query = """ INSERT INTO """ + db_schema + """ .SR_GOAL (
                    FISCAL_QTR,
                    SALES_REP_NAME,
                    SALES_REP_NUMBER,
                    SALES_REP_USD_BOOKING_GOAL,
                    USD_BOOKING_AMOUNT,
                    QUARTER_KEY,
                    ENTRY_DATE
                  )
                SELECT
                  a.FISCAL_QTR,
                  a.SALES_REP_NAME,
                  a.SALES_REP_NUMBER,
                  Decode(
                    b.SALES_REP_USD_BOOKING_GOAL,
                    NULL,
                    0,
                    b.SALES_REP_USD_BOOKING_GOAL
                  ) SALES_REP_USD_BOOKING_GOAL,
                  Decode(a.USD_BOOKING_AMOUNT, NULL, 0, a.USD_BOOKING_AMOUNT) USD_BOOKING_AMOUNT,
                  c.FISCAL_QUARTER_KEY as QUARTER_KEY,
                  A.ENTRY_DATE
                FROM
                  (
                    SELECT
                      *
                    FROM
                      T2_SALES_CHANNEL.STG_SR_GOAL
                    WHERE
                      USD_BOOKING_AMOUNT IS NOT NULL
                    ORDER BY
                      SALES_REP_NUMBER,
                      FISCAL_QTR
                  ) a
                  LEFT OUTER JOIN (
                    SELECT
                      *
                    FROM
                      T2_SALES_CHANNEL.STG_SR_GOAL
                    WHERE
                      SALES_REP_USD_BOOKING_GOAL IS NOT NULL
                    ORDER BY
                      SALES_REP_NUMBER,
                      FISCAL_QTR
                  ) b ON b.SALES_REP_NUMBER = a.SALES_REP_NUMBER
                  and b.FISCAL_QTR = a.FISCAL_QTR
                  inner join T2_SALES_CHANNEL.FISCAL_QUARTER C on C.FISCAL_YEAR_QTR_TEXT = a.FISCAL_QTR

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
            spark, config = start_spark(app_name=app_name)

            # Create class Object
            ebi_read_write_obj = EbiReadWrite(app_name, spark, config, logger)

            # DB prop Key of Source DB
            db_prop_key_load = config['DB_PROP_KEY_LOAD']
            db_schema = config['DB_SCHEMA']
            log_file = config['LOG_DIR_NAME'] + "/" + log_filename

            query = query_data(db_schema)

            # Calling Job Class method --> get_target_data_update()
            ebi_read_write_obj.get_target_data_update(query, db_prop_key_load)

            end_date = "'" + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"

            data_format = "JOB START DT : " + start_date + " | SCRIPT NAME : " + script_name + " | JOB : " + app_name + \
                          " | SRC COUNT : " + src_count + " | TGT COUNT : " + dest_count + " | JOB END DT : "\
                          + end_date + " | STATUS : %(message)s"

            ebi_read_write_obj.create_log(data_format, log_file, logger)

            logger.info("Success")
            ebi_read_write_obj.job_debugger_print("     \n  __main__ " + app_name + " --> Job " + app_name + " Succeed \n")

        except Exception as err:
            # Write expeption in spark log or console
            end_date = "'" + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"
            data_format = "JOB START DT : "+start_date+" | SCRIPT NAME : "+script_name+" | JOB : "+app_name\
                          + " | SRC COUNT : "+src_count+" | TGT COUNT : "+dest_count+" | JOB END DT : "+end_date\
                          + " | STATUS : %(message)s"
            ebi_read_write_obj.create_log(data_format, log_file, logger)
            logger.info("[Error] Failed")
            ebi_read_write_obj.job_debugger_print("     \n Job " + app_name + " Failed\n")
            logger.error("\n __main__ " + app_name + " --> Exception-Traceback :: " + str(err))
            raise


# Entry point for script
if __name__ == "__main__":
    # Calling main() method
    main()
