# SCH1101.SH --> JB_BOOKINGS_DAY_FLAG_TEMP_TRUNCATE.py

#**************************************************************************************************************
#
# Created by  : Vinay Kumbakonam
# Modified by : bibin
# Version      : 1.2
#
# Description  :
#        1. Truncates the 'BOOKINGS_DAY_FLAG_TEMP' table based on Oracle procedure
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-09-28                    Initial creation
# 2018-10-30                    Getting DB schema, db_prop_key_load from Config file
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
script_name = "SCH1101"
app_name = "JB_BOOKINGS_DAY_FLAG_TEMP_TRUNCATE"

# Truncate Table Details
truncate_table = 'BOOKINGS_DAY_FLAG_TEMP'
procedure = 'SP_TRUNCATETABLE'
#  Main method
def main():
        try:
            # start Spark application and get Spark session, logger and config
            spark, config = start_spark(
                app_name=app_name)

            # Create class Object
            Ebi_read_write_obj = EbiReadWrite(app_name,spark,config,logger)
            # DB prop Key of Source DB
            db_prop_key = config['DB_PROP_KEY_LOAD']
            schema = config['DB_SCHEMA']
            # Calling Truncate method
            trunc_method = Ebi_read_write_obj.truncate_table(truncate_table,schema,procedure,db_prop_key)
            Ebi_read_write_obj.job_debugger_print("\n   Table "+truncate_table+" has been Truncated\n")
        except Exception as err:
            # Write expeption in spark log or console

            Ebi_read_write_obj.job_debugger_print("\n Table "+truncate_table+" Truncate Failed\n")
            Ebi_read_write_obj.job_debugger_print("\n   "+err)
            raise

# Entry point for script
if __name__ == "__main__":
        main()
