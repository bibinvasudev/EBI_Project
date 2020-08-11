# SCH1160.sh  --> JB_UPD_PRODUCT_BOOKING_REPORT.py

#**************************************************************************************************************
#
# Created by   : Vijay Nagappa
# Modified by  : bibin
# Version      : 1.1
#
# Description  :
#        1. Updates the table 'Product_Booking_Report' table based on Oracle procedure
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-10-31                    Initial creation
# 2018-11-02                    bibin : Using job_debugger_print() for print any string in Job
#
#**************************************************************************************************************


# Importing required Lib
from dependencies.spark import start_spark
from dependencies.EbiReadWrite import EbiReadWrite
import sys
from time import gmtime, strftime
import cx_Oracle
import py4j
import logging

# Spark logging
logger = logging.getLogger(__name__)

# Date Formats
start_date = "'" + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"
log_date = strftime("%Y%m%d", gmtime())

# Job Naming Details
# Need to have job and app name to represent the actual task/etl being performed Ex: "SCH_PRODUCT_BOOKING_LOAD.PY"
script_name = "SCH1160.SH"
app_name = 'JB_UPD_PRODUCT_BOOKING_REPORT'
log_filename = app_name + '_' + log_date + '.log'


def query_data(db_schema):

    query = """SELECT /*+ parallel(T762683, 6) */
        T762683.SITE_COUNTRY_NAME AS DISTRIBUTOR_SITE_COUNTRY_NAME,
         COUNT (DISTINCT BK_SALES_ORDER_NUMBER) AS HUNDRED_PCT_PVR
    FROM DIMS.ORGANIZATION_PARTY T762683,
         DIMS.QUOTE T762071,
         DIMS.ENTERPRISE_LIST_OF_VALUES T705584,
         FACTS.SALES_BOOKING T762157,
         DIMS.CALENDAR T763102,
         DIMS.SALES_ORDER T762251
   WHERE (    T762157.DISTRIBUTOR_AS_IS_KEY = T762683.ORGANIZATION_PARTY_KEY
          AND T705584.ATTRIBUTE_CODE = 'CORP FLAG'
          AND T705584.ATTRIBUTE_CODE_VALUE = T762157.CORP_FLAG
          AND T762157.SOURCE_TRANSACTION_DATE_KEY = T763102.DATE_KEY
          AND T762071.QUOTE_KEY = T762157.QUOTE_KEY
          AND T705584.ATTRIBUTE_CODE_DESCRIPTION = 'Y'
          AND T762157.SALES_ORDER_KEY = T762251.SALES_ORDER_KEY
          AND (T763102.FISCAL_YEAR_QTR_TEXT = '2019Q2')
          AND T762071.HUNDRED_PCT_DISC_REASON_NAME IS NOT NULL
          AND T762157.ME_EXT_LIST_PRICE <> '0')
GROUP BY T762683.SITE_COUNTRY_NAME"""

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

        # DB prop Key of Source DB
        db_prop_key_load = config['DB_PROP_KEY_LOAD']
        db_schema = config['DB_SCHEMA']
        log_file = config['LOG_DIR_NAME'] + "/" + log_filename
        # SQL Query
        query = query_data(db_schema)

        # Calling Job Class method --> get_target_data_update()
        Ebi_read_write_obj.extract_data_oracle(query, db_prop_key_load)

        end_date = "'" + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"

        data_format = "JOB START DT : " + start_date + " | SCRIPT NAME : " + script_name + " | JOB : " + app_name + " | SRC COUNT : " + src_count + " | TGT COUNT : " + dest_count + " | JOB END DT : " + end_date + " | STATUS : %(message)s"

        Ebi_read_write_obj.create_log(data_format, log_file, logger)

        logger.info("Success")
        Ebi_read_write_obj.job_debugger_print("	\n  __main__ " + app_name + " --> Job " + app_name + " Succeed \n")

    except Exception as err:
        # Write expeption in spark log or console
        Ebi_read_write_obj.job_debugger_print("\n Table PRODUCT_BOOKING_REPORT Update Failed\n")
        Ebi_read_write_obj.job_debugger_print(err)
        raise

# Entry point for script
if __name__ == "__main__":
    # Calling main() method
    main()


