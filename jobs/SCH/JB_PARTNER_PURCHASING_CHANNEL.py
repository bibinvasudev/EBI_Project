# SCH1006.sh  --> JB_PARTNER_PURCHASING_CHANNEL.py

#**************************************************************************************************************
#
# Created by  : Vinay Kumbakonam
# Modified by : bibin
# Version      : 1.2
#
# Description  :
#        1. Reads the 'Purchasing Channel' worksheet from partner details xlsx.
#        2. Writes into Oracle table 'PARTNER_PURCHASING_CHANNEL'.
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-09-28                    Initial creation
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
script_name = "SCH1006.sh"
app_name = 'JB_PARTNER_PURCHASING_CHANNEL'
log_filename = app_name + '_' + log_date + '.log'

# Worksheet Read Details
read_file_name = '/home/spark/v_kumbakonam/Partner_Details.xlsx'
sheet_name = 'Purchasing Channel'
use_header = 'true'
infer_schema = 'true'
src_count = '0'

# Target Table Details
write_table = 'PARTNER_PURCHASING_CHANNEL'


# Oracle write Details
save_mode = 'append'
dest_count = '0'

#  Main method
def main():
        try:
            # start Spark application and get Spark session, logger and config
            spark, config = start_spark(
                app_name=app_name)

            db_prop_key_load = config['DB_PROP_KEY_LOAD']
            db_schema = config['DB_SCHEMA']
            log_file = config['LOG_DIR_NAME'] + "/" + log_filename

            target_table_name = db_schema+"."+write_table

            # Create class Object
            Ebi_read_write_obj = EbiReadWrite(app_name,spark,config,logger)

            # Calling Job Class method --> loadWorksheetData()
            dataFrame = Ebi_read_write_obj.extract_data_worksheet(read_file_name,sheet_name,use_header,infer_schema)

            #Creating Temp Table
            dataFrame.createOrReplaceTempView("purchasing_channel_temp")
			
            purchasing_channel_df = spark.sql("select cast("+start_date+" as date)as ENTRY_DATE\
                                    , cast((case when (`Primary Purchasing Channel(y/n)`=='Yes') then replace(`Primary Purchasing Channel(y/n)`,'Yes','Y') \
                                    when (`Primary Purchasing Channel(y/n)`=='No') then replace(`Primary Purchasing Channel(y/n)`,'No','N')END)\
                                    as varchar(1))PRIMARY_PURCHASING_CHANNEL\
                                    , (case when (`Purchasing Channel Start Date`=='1899-12-30') then '1899/12/30 00:00:00.000' else\
                                    `Purchasing Channel Start Date` END)PURCHASING_CHANNEL_START_DATE\
                                    , (case when (`Purchasing Channel End Date`=='1899-12-30') then '1899/12/30 00:00:00.000' else\
                                    `Purchasing Channel End Date` END)PURCHASING_CHANNEL_END_DATE\
                                    , cast(`Partner Id` as int)PARTNER_ID, cast(`Partner Tier` as varchar(50))PARTNER_TIER\
                                    , cast(`Tier 1 Purchasing Channel` as varchar(250))TIER1_PURCHASING_CHANNEL\
                                    , cast(`Tier 2 Purchasing Channel` as varchar(250))TIER2_PURCHASING_CHANNEL \
                                    from purchasing_channel_temp")
			
            purchasing_channel_df.show(5)
			
            # Checking file_record count
            src_count=str(purchasing_channel_df.count())
            Ebi_read_write_obj.job_debugger_print("     \n Source Count : "+src_count+"\n")

            # Calling Job Class method --> saveDataOracle()
            Ebi_read_write_obj.load_data_oracle(purchasing_channel_df,target_table_name,save_mode,db_prop_key_load)

            # getTargetDataCount
            dest_count = str(Ebi_read_write_obj.get_target_data_count(target_table_name,db_prop_key_load))
            Ebi_read_write_obj.job_debugger_print("\n Target Table Count(After write) : " +dest_count+"\n")
            end_date="'"+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"'"

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
            logger.error("\n __main__ "+ app_name +" --> Exception-Traceback :: " + str(err))

            raise

# Entry point for script
if __name__ == "__main__":
        # Calling main() method
        main()			
		
