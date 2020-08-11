# SCH1007.sh --> JB_PARTNER_SELLING_TERRITORY_TRUNCATE.py

#**************************************************************************************************************
#
# Created by  : Vinay Kumbakonam
# Modified by : bibin
# Version      : 1.1
#
# Description  :
#        1. Truncates the 'PARTNER_SELLING_TERRITORY' table based on Oracle procedure
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-09-28                    Initial creation
# 2018-10-30                    Getting DB schema, db_prop_key_load from Config file, log DIR
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
script_name = "SCH1007.sh"
app_name = 'JB_PARTNER_SELLING_TERRITORY_TRUNCATE.py'

# Truncate Table Details
truncate_table = 'PARTNER_SELLING_TERRITORY'
procedure = 'SP_TRUNCATETABLE'

#  Main method
def main():
        try:
            # start Spark application and get Spark session, logger and config
            spark, config = start_spark(
                app_name=app_name)

            db_prop_key_load = config['DB_PROP_KEY_LOAD']
            db_schema = config['DB_SCHEMA']

            # Create class Object
            Ebi_read_write_obj = EbiReadWrite(app_name,spark,config,logger)

            # Calling Truncate method
            trunc_method = Ebi_read_write_obj.truncate_table(truncate_table,db_schema,procedure,db_prop_key_load)

            print("\n   Table "+truncate_table+" has been Truncated\n")
        except Exception as err:
            # Write expeption in spark log or console
            
            print("\n	Table "+truncate_table+" Truncate Failed\n")
            print("\n	"+err)
            raise

# Entry point for script
if __name__ == "__main__":
        # Calling main() method
        main()

