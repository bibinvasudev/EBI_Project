# SCH1101.sh  --> JB_UPD_BOOKINGS.py

#**************************************************************************************************************
#
# Created by  : bibin
# Version      : 1.1
#
# Description  :
#        1. Updates the table 'WORK_BOOKINGS' table based on Oracle procedure
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
import sys
from time import gmtime, strftime
import cx_Oracle
import py4j
import logging

logger = logging.getLogger(__name__)


# Job Naming Details
script_name = "SCH1101.sh"
app_name = "JB_UPD_BOOKINGS"


#  Main method
def main():
        try:
           # start Spark application and get Spark session and config
           spark, config = start_spark(
                app_name=app_name)

           # DB prop Key of LOAD DB
           db_prop_key_load = config['DB_PROP_KEY_LOAD']
           db_schema = config['DB_SCHEMA']
           # Create class Object
           Ebi_read_write_obj = EbiReadWrite(app_name,spark,config,logger)

           # Update Query
           update_query = "MERGE INTO "+db_schema+".BOOKINGS A \
                        USING T2_SALES_CHANNEL.WORK_DEBOOKING B \
                           ON (A.BOOKING_FISCAL_QTR = B.BOOKED_QTR AND A.SO_NUMBER = B.SO_NUMBER) \
                        WHEN MATCHED \
                            THEN UPDATE \
                            SET A.DEBOOKED_FLAG = 'Y', \
                        A.DEBOOKED_QTR = B.DEBOOKED_QTR"


           # Calling Update method
           Ebi_read_write_obj.get_target_data_update(update_query,db_prop_key_load)

           Ebi_read_write_obj.job_debugger_print("\n   Table BOOKINGS has been Updated\n")
        except Exception as err:
            # Write expeption in spark log or console
            Ebi_read_write_obj.job_debugger_print("\n Table BOOKINGS Update Failed\n")
            Ebi_read_write_obj.job_debugger_print(err)
            raise

# Entry point for script
if __name__ == "__main__":
        # Calling main() method
        main()

