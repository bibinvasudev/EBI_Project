# SCH1150.SH --> JB_INSTALLEDBASE_BOOKINGS_TRUNCATE.py

#**************************************************************************************************************
#
# Created by  : Vinay Kumbakonam
# Modified by : bibin
# Version      : 1.2
#
# Description  :
#             1. This script will Truncates the 'INSTALLEDBASE_BOOKINGS' table.
#
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-10-17                    Initial creation
# 2018-10-30                    bibin : Getting DB schema, db_prop_key_load from Config file
# 2018-11-02                    bibin : Using job_debugger_print() for print any string in Job
#
#**************************************************************************************************************

# Importing required Lib

import logging
import sys
from time import gmtime, strftime
import cx_Oracle
import py4j

from dependencies.spark import start_spark
from dependencies.EbiReadWrite import EbiReadWrite

# Spark logging
logger = logging.getLogger(__name__)

# Job Naming Details
script_name = "SCH1150.SH"
app_name = "JB_INSTALLEDBASE_BOOKINGS_TRUNCATE"

# truncate Table Details
truncate_table = 'INSTALLEDBASE_BOOKINGS'
procedure = 'SP_TRUNCATETABLE'

#  Main method
def main():
        try:
            # start Spark application and get Spark session, logger and config
            spark, config = start_spark(
                app_name=app_name)

            # DB prop Key of Source DB
            db_prop_key_load =  config['DB_PROP_KEY_LOAD']
            schema = config['DB_SCHEMA']

            # log that main ETL job is starting
            # log.warn('SCH_job is up-and-running')
            # Create class Object
            Ebi_read_write_obj = EbiReadWrite(app_name,spark,config,logger)

            # Calling Truncate method
            trunc_method = Ebi_read_write_obj.truncate_table(truncate_table,schema,procedure,db_prop_key_load)

            Ebi_read_write_obj.job_debugger_print("\n   Table "+truncate_table+" has been Truncated\n")
        except Exception as err:
            # Write expeption in spark log or console
            Ebi_read_write_obj.job_debugger_print("\n Table "+truncate_table+" Truncate Failed\n")
            Ebi_read_write_obj.job_debugger_print(err)
            raise

# Entry point for script
if __name__ == "__main__":
        # Calling main() method
        main()

