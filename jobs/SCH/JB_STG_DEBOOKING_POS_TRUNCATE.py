# SCH1101.SH --> JB_STG_DEBOOKING_POS_TRUNCATE.py

#**************************************************************************************************************
#
# Programmer   : bibin
# Version      : 1.0
#
# Description  :
#        1. Truncates the 'STG_DEBOOKING_POS_TRUNCATE' table based on Oracle procedure
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-10-30                    Initial creation
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
script_name = "SCH1101"
app_name = "JB_STG_DEBOOKING_POS_TRUNCATE"

# truncate Table Details
truncate_table = 'STG_DEBOOKING_POS'
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
            db_prop_key_load = config['DB_PROP_KEY_LOAD']
            schema = config['DB_SCHEMA']

            # Calling Truncate method
            trunc_method = Ebi_read_write_obj.truncate_table(truncate_table,schema,procedure,db_prop_key_load)
            print("\n   Table "+truncate_table+" has been Truncated\n")
        except Exception as err:
            # Write expeption in spark log or console

            print("\n Table "+truncate_table+" Truncate Failed\n")
            print("\n   "+err)
            raise

# Entry point for script
if __name__ == "__main__":
        main()
