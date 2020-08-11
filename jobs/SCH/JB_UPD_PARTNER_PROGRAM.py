# SCH1004.sh --> JB_UPD_PARTNER_PROGRAM.py

#**************************************************************************************************************
#
# Created by  : Vinay Kumbakonam
# Modified by : bibin
# Version      : 1.1
#
# Description  :
#        1. Updates the table 'PARTNER_PROGRAM' based on Oracle procedure.
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-09-27                    Initial creation
# 2018-10-30                    bibin : Getting DB schema, db_prop_key_load from Config file, log DIR
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

logger = logging.getLogger(__name__)

# Job Naming Details
script_name = "SCH1004.sh"
app_name = 'JB_UPD_PARTNER_PROGRAM'

# Target Table Details
target_table = 'PARTNER_PROGRAM'

#  Main method
def main():
        try:
            # start Spark application and get Spark session, logger and config
            spark, config = start_spark(
                app_name=app_name)

            # DB prop Key of Source DB
            db_prop_key_load = config['DB_PROP_KEY_LOAD']
            db_schema = config['DB_SCHEMA']

            # Create class Object
            Ebi_read_write_obj = EbiReadWrite(app_name,spark,config,logger)

            # Update Query
            update_query = "Update "+db_schema+"."+target_table+"\
                            SET PROGRAM_START_DT_FISCAL_QTR = ( SELECT FISCAL_YEAR_QTR_TEXT FROM "+db_schema+".CALENDAR \
                            WHERE TRUNC(PARTNER_PROGRAM_START_DATE) = TRUNC(BK_CALENDAR_DATE))"

            # Calling Update method
            Ebi_read_write_obj.get_target_data_update(update_query,db_prop_key_load)

            Ebi_read_write_obj.job_debugger_print("\n   Table PARTNER_PROGRAM has been Updated\n")
        except Exception as err:
            # Write expeption in spark log or console
            Ebi_read_write_obj.job_debugger_print("\n	Table PARTNER_PROGRAM Update Failed\n")
            Ebi_read_write_obj.job_debugger_print(err)
            raise

# Entry point for script
if __name__ == "__main__":
        # Calling main() method
        main()

