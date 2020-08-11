# SCH1101.sh  --> JB_EVALUATE_ROWS_NUMBER.py


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
app_name = "JB_EVALUATE_ROWS_NUMBER"
log_filename = app_name + '_' + log_date + '.log'


# Query for loading invoice table
def query_data(db_schema):

        query = """ (SELECT FISCAL_MONTH_OF_QTR_NUMBER from """+ db_schema +""".FISCAL_MONTH
		WHERE CURRENT_FISCAL_MONTH_FLAG  = 'Y' AND FISCAL_MONTH_OF_QTR_NUMBER = 2) """

        return query


#  Main method
def main():
        try:
            evaluate_rows_count = '0'
            
            # start Spark application and get Spark session, logger and config
            spark, config = start_spark(
                app_name=app_name)

            # Create class Object
            Ebi_read_write_obj = EbiReadWrite(app_name,spark,config,logger)

            # DB prop Key of Source DB
            db_prop_key_extract =  config['DB_PROP_KEY_EXTRACT']
            db_schema = config['DB_SCHEMA']
            log_file = config['LOG_DIR_NAME'] + "/" + log_filename


            #SQL Query
            query = query_data(db_schema)
            Ebi_read_write_obj.job_debugger_print(query)

            # Calling Job Class method --> get_target_data_update()
            evaluate_rows_number_DF = Ebi_read_write_obj.extract_data_oracle(query,db_prop_key_extract)

            evaluate_rows_count = evaluate_rows_number_DF.count()

            end_date="'"+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"'"

            data_format = "JOB START DT : "+start_date+" | SCRIPT NAME : "+script_name+" | JOB : "+app_name+" | EVALUATE ROWS COUNT : "+ str(evaluate_rows_count) +" | JOB END DT : "+end_date+" | STATUS : %(message)s"

            Ebi_read_write_obj.create_log(data_format,log_file,logger)

            logger.info("Success")
            Ebi_read_write_obj.job_debugger_print("     \n  __main__ " + app_name +" --> Job "+app_name+" Succeed \n")

            if evaluate_rows_count == 1:
                sys.exit(1)
            else :
                sys.exit(0)

        except Exception as err:
            # Write expeption in spark log or console
            end_date="'"+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"'"
            data_format = "JOB START DT : "+start_date+" | SCRIPT NAME : "+script_name+" | JOB : "+app_name+" | EVALUATE ROWS COUNT : "+ str(evaluate_rows_count) +" | JOB END DT : "+end_date+" | STATUS : %(message)s"
            Ebi_read_write_obj.create_log(data_format,log_file,logger)
            logger.info("[Error] Failed")
            Ebi_read_write_obj.job_debugger_print("     \n Job "+app_name+" Failed\n")
            logger.error("\n __main__ "+ app_name +" --> Exception-Traceback :: " + str(err))
            raise

# Entry point for script
if __name__ == "__main__":
    # Calling main() method
    main()


