# SCH2004.SH --> JB_SR_GOAL_PARTITION_TRUNCATE.py

#**************************************************************************************************************
#
# Created by : Bibin Vasudevan
# Version      : 1.0
#
# Description  :
#
#             1. This script will Truncates the Partition for 'SR_GOAL' table.
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
script_name = "SCH2004.SH"
app_name = "JB_SR_GOAL_PARTITION_TRUNCATE"

# Truncate Table Details
truncate_table = 'SR_GOAL'
procedure = 'SP_TRUNCATEPARTITION'

#  Main method
def main():
        try:
            # start Spark application and get Spark session, logger and config
            spark, config = start_spark(app_name=app_name)

            # DB prop Key of Source DB
            db_prop_key_load = config['DB_PROP_KEY_LOAD']
            schema = config['DB_SCHEMA']

            # Create class Object
            ebi_read_write_obj = EbiReadWrite(app_name,spark,config,logger)

            # Calling partition column method
            partition_column = ebi_read_write_obj.get_v_partition(schema,truncate_table,db_prop_key_load)
            ebi_read_write_obj.job_debugger_print("\n	Partition Column : "+str(partition_column)+"\n")
            
            # Calling Partition Truncate methos
            Truncate = ebi_read_write_obj.truncate_table_partition(truncate_table,schema,procedure,partition_column,db_prop_key_load)
            ebi_read_write_obj.job_debugger_print("\n	Table "+truncate_table+" Partition Column - "+partition_column+" is Truncated\n")
        except Exception as err:
            # Write expeption in spark log or console

            ebi_read_write_obj.job_debugger_print(err)
            raise

# Entry point for script
if __name__ == "__main__":
        # Calling main() method
        main()

