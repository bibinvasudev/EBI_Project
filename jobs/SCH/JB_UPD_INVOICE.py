# SCH1002.sh  --> JB_UPD_INVOICE.py

#**************************************************************************************************************
#
# Programmer   : bibin
# Version      : 1.0
#
# Description  :
#        1. Updates the table 'Partner_Program_Benefits' table based on Oracle procedure
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-10-23                    Initial creation
#
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
script_name = "SCH1100.sh"
app_name = 'JB_UPD_INVOICE'


#  Main method
def main():
        try:
           # start Spark application and get Spark session and config
           spark, config = start_spark(
                app_name=app_name)

           # DB prop Key of LOAD DB
           db_prop_key_load =  config['DB_PROP_KEY_LOAD']
           db_schema = config['DB_SCHEMA']

           # Create class Object
           Ebi_read_write_obj = EbiReadWrite(app_name,spark,config,logger)

           # Update Query
           update_query = """Update """ +db_schema+ """.WORK_INVOICE
                            Set Partner_ID = Sold_To_Partner_ID
                            Where Partner_ID is Null And Distributor_NAGP = 'UNKNOWN' """

           # Calling Update method
           Ebi_read_write_obj.get_target_data_update(update_query,db_prop_key_load)

           Ebi_read_write_obj.job_debugger_print("\n   Table WORK_INVOICE has been Updated\n")
        except Exception as err:
            # Write expeption in spark log or console
            Ebi_read_write_obj.job_debugger_print("\n Table WORK_INVOICE Update Failed\n")
            Ebi_read_write_obj.job_debugger_print(err)
            raise

# Entry point for script
if __name__ == "__main__":
        # Calling main() method
        main()

