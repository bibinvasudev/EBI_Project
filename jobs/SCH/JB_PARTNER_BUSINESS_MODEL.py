# SCH1002.sh  --> JB_PARTNER_BUSINESS_MODEL.py

#**************************************************************************************************************
#
# Created by  : Vinay Kumbakonam
# Modified by : bibin
# Version      : 1.2
#
# Description  :
#        1. Reads the 'Business Rights' worksheet from partner details xlsx.
#        2. Writes into Oracle table 'PARTNER_BUSINESS_MODEL'.
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-10-11                    Initial creation
# 2018-10-30                    bibin : Getting DB schema, db_prop_key_load from Config file
# 2018-11-02                    bibin : Using job_debugger_print() for print any string in Job
#
#**************************************************************************************************************

# Importing required Lib
from dependencies.spark import start_spark
from dependencies.EbiReadWrite import EbiReadWrite
import logging
import sys
from time import gmtime, strftime
import cx_Oracle
import py4j

# Spark logging
logger = logging.getLogger(__name__)


# Date Formats
start_date = "'"+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"'"
log_date = strftime("%Y%m%d", gmtime())

# Job Naming Details 
script_name = "SCH1002.sh"
app_name = 'JB_PARTNER_BUSINESS_MODEL'
log_filename = app_name + '_' + log_date + '.log'

# Worksheet Read Details
read_file_name = '/home/spark/v_kumbakonam/Partner_Details.xlsx'
sheet_name = 'Business Rights'
use_header = 'true'
infer_schema = 'true'
src_count = '0'

# Target Table Details
write_table = 'PARTNER_BUSINESS_MODEL'

# Oracle write Details
save_mode = 'append'
dest_count = '0'

#  Main method
def main():
         try:
            # start Spark application and get Spark session, logger and config
            spark, config = start_spark(
                app_name=app_name)

            # Create class Object
            Ebi_read_write_obj = EbiReadWrite(app_name,spark,config,logger)

            db_prop_key_load = config['DB_PROP_KEY_LOAD']
            db_schema = config['DB_SCHEMA']
            log_file = config['LOG_DIR_NAME'] + "/" + log_filename

            target_table_name = db_schema+"."+write_table

            # Calling Job Class method --> extract_data_worksheet()
            dataFrame = Ebi_read_write_obj.extract_data_worksheet(read_file_name,sheet_name,use_header,infer_schema)

            #Creating Temp Table
            dataFrame.createOrReplaceTempView("partner_business_model_temp")	

            partner_business_df=spark.sql("select cast(`Partner Id` as int)PARTNER_ID\
                                , cast(substr(`Primary Model?(y/n)`,0,1) as varchar(1))PRIMARY_MODEL\
                                , cast(`Business Rights` as varchar(50))BUSINESS_MODEL\
                                , cast(Level as varchar(25))PARTNER_LEVEL\
                                , cast(`Business Right Status` as varchar(25))BUSINESS_MODEL_STATUS\
                                , `Business Right Start Date` as MODEL_START_DATE\
                                , `Business Right End Date` as MODEL_END_DATE, `Level Start Date` as LEVEL_START_DATE\
                                , `Level End Date` LEVEL_END_DATE, cast("+start_date+" as date)ENTRY_DATE\
                                from partner_business_model_temp")
            partner_business_df.show(5)
			
            # Checking file_record count
            src_count=str(partner_business_df.count())
            Ebi_read_write_obj.job_debugger_print("     \n Source Count : "+src_count+"\n")

            # Calling Job Class method --> load_data_oracle()
            Ebi_read_write_obj.load_data_oracle(partner_business_df,target_table_name,save_mode,db_prop_key_load)
			
            # getTargetDataCount
            dest_count = str(Ebi_read_write_obj.get_target_data_count(target_table_name,db_prop_key_load))
            Ebi_read_write_obj.job_debugger_print("\n Target Table Count(After write) : " +dest_count+"\n")
            end_date="'"+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"'"
            
            # Log Format
            data_format = "JOB START DT : "+start_date+" | SCRIPT NAME : "+script_name+" | JOB : "+app_name+" | SRC COUNT : "+src_count+" | TGT COUNT : "+dest_count+" | JOB END DT : "+end_date+" | STATUS : %(message)s"

            Ebi_read_write_obj.create_log(data_format,log_file,logger)
            logger.info("Success")
            Ebi_read_write_obj.job_debugger_print("     \n Job "+app_name+" Succeed \n")

         except Exception as err:

            # Write expeption in spark log or console
            data_format = "JOB START DT : "+start_date+" | SCRIPT NAME : "+script_name+" | JOB : "+app_name+" | SRC COUNT : "+src_count+" | TGT COUNT : "+dest_count+" | JOB END DT : "+end_date+" | STATUS : %(message)s"

            Ebi_read_write_obj.create_log(data_format,log_file,logger)
            logger.info("[Error] Failed")
            Ebi_read_write_obj.job_debugger_print("     \n Job "+app_name+" Failed\n")
            logger.error("\n __main__ SCH_job --> Exception-Traceback :: " + str(err))

            raise			
			
# Entry point for script
if __name__ == "__main__":
        # Calling main() method
        main()			

			


