# SCH1002.sh  --> JB_UPD_BUSINESS_MODEL_LSD.py

#**************************************************************************************************************
#
# Created by  : Vinay Kumbakonam
# Modified by : bibin
# Version      : 1.2
#
# Description  :
#        1. Updates the table 'PARTNER_BUSINESS_MODEL' table based on Oracle procedure
#
#  Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-10-11                    Initial creation
# 2018-10-30                    bibin : Getting DB schema, db_prop_key_load from Config file, log DIR
# 2018-11-02                    bibin : Using job_debugger_print() for print any string in Job
#
#**************************************************************************************************************

# Importing required Lib
from dependencies.spark import start_spark
from dependencies.EbiReadWrite import EbiReadWrite
import sys
import cx_Oracle
import py4j
import logging

logger = logging.getLogger(__name__)

# Job Naming Details
script_name = "SCH1002.sh"
app_name = 'JB_UPD_BUSINESS_MODEL_LSD'

# Target Table Details
target_table = 'PARTNER_BUSINESS_MODEL'

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
            update_query = "Update "+db_schema+"."+target_table+" \
                           SET LEVEL_START_DT_FISCAL_QTR = ( SELECT FISCAL_YEAR_QTR_TEXT FROM "+db_schema+".CALENDAR \
                           WHERE TRUNC(BK_CALENDAR_DATE) = TRUNC(LEVEL_START_DATE))" 
			
            # Calling Update method
            update_method = Ebi_read_write_obj.get_target_data_update(update_query,db_prop_key_load)

            Ebi_read_write_obj.job_debugger_print("\n   Table Partner_Business_Model has been Updated\n")
        except Exception as err:
            # Write expeption in spark log or console
            Ebi_read_write_obj.job_debugger_print("\n Table Partner_Business_Model Update Failed\n")
            Ebi_read_write_obj.job_debugger_print(err)
            raise

# Entry point for script
if __name__ == "__main__":
        # Calling main() method
        main()

