# SCH1101.SH --> JB_STG_QUOTE_COPIED_PARTITION_TRUNCATE.py

#**************************************************************************************************************
#
# Programmer   : Vinay Kumbakonam
# Version      : 1.0
#
# Description  :
#
#
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-11-05                    Initial creation
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
script_name = "SCH1100.sh"
app_name = "JB_STG_QUOTE_COPIED_PARTITION_TRUNCATE"

# Target Table Details
truncate_table = 'STG_QUOTE_COPIED'
procedure = 'SP_TRUNCATEPARTITION'

#  Main method
def main():
        try:
            # start Spark application and get Spark session, logger and config
            spark, config = start_spark(
                app_name=app_name)

            # DB prop Key of Source DB
            db_prop_key_extract =  config['DB_PROP_KEY_EXTRACT']
            db_prop_key_load = config['DB_PROP_KEY_LOAD']
            db_schema = config['DB_SCHEMA']

            # Create class Object
            Ebi_read_write_obj = EbiReadWrite(app_name,spark,config,logger)

            # Calling partition column method
            partition_column = Ebi_read_write_obj.get_v_partition(db_schema,truncate_table,db_prop_key_extract)

            Ebi_read_write_obj.job_debugger_print("\n	Partition Column : "+str(partition_column)+"\n")
            
            # Calling Partition Truncate methos
            Truncate = Ebi_read_write_obj.truncate_table_partition(truncate_table,db_schema,procedure,partition_column,db_prop_key_load)
            Ebi_read_write_obj.job_debugger_print("\n	Table "+truncate_table+" Partition Column - "+partition_column+" is Truncated\n")
        except Exception as err:
            # Write expeption in spark log or console

            Ebi_read_write_obj.job_debugger_print(err)
            raise

# Entry point for script
if __name__ == "__main__":
        # Calling main() method
        main()

