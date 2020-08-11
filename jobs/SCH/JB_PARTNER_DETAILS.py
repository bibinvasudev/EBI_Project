# SCH1001.sh --> JB_PARTNER_DETAILS.py

#**************************************************************************************************************
#
# Created by  : Vinay Kumbakonam
# Modified by : bibin
# Version      : 1.1
#
# Description  :
#
#        1. Reads the 'Partner Summary' worksheet from partner details xlsx.
#        2. Writes into Oracle table 'PARTNER_DETAILS'.
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
script_name = "SCH1001.sh"
app_name = 'JB_PARTNER_DETAILS'
log_filename = app_name + '_' + log_date + '.log'

# Worksheet Read Details
read_file_name = '/home/spark/v_kumbakonam/Partner_Details.xlsx'
sheet_name = 'Partner Summary'
use_header = 'true'
infer_schema = 'true'
src_count = '0'

# Target Table Details
write_table = 'PARTNER_DETAILS'

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

            # Calling Job Class method --> extract_data_worksheet()
            dataFrame = Ebi_read_write_obj.extract_data_worksheet(read_file_name,sheet_name,use_header,infer_schema)
            # Calling Lookup table
            lkp_table = Ebi_read_write_obj.extract_data_oracle(target_table_name,db_prop_key_load)
             
            #Creating Temp Table
            dataFrame.createOrReplaceTempView("partner_details_temp")
            lkp_table.createOrReplaceTempView("partner_details_lkp")
            # Joining Source file data to Lookup table
            lkp_query = spark.sql("select b.PARTNER_ID as PARTNER_ID_LOOKUP\
                        , upper(a.`Partner Name`)PARTNER_NAME,a.`Partner Id` as PARTNER_ID\
                        , a.`Cmat Company Id` as CMAT_ID, a.ADDRESS_LINE1, a.ADDRESS_LINE2\
                        , a.ADDRESS_LINE3, a.CITY, a.STATE, a.ZIP, a.COUNTRY, a.Level as PARTNER_LEVEL\
                        , a.`Home GEO` as HOME_GEO, a.`CMAT NAGP name` as CMAT_NAGP_NAME\
                        , a.`Partner Type` as PARTNER_TYPE, a.`Primary Business Right` as PRIMARY_BUSINESS_MODEL\
                        , a.`Business Right Status` as BUSINESS_MODEL_STATUS \
                        ,a.`Primary CDM` as PRIMARY_CDM, a.`Primary Purchasing Channel` as PRIMARY_PURCHASING_CHANNEL \
                        from partner_details_temp a \
                        left outer join partner_details_lkp b on (a.`Partner Id`=b.PARTNER_ID)")

            lkp_query.createOrReplaceTempView("lkp_query_table")
			
            partner_details = spark.sql("select PARTNER_NAME, PARTNER_ID, CMAT_ID\
                              , ADDRESS_LINE1, ADDRESS_LINE2\
                              , ADDRESS_LINE3, CITY, STATE\
                              , ZIP, COUNTRY, PARTNER_LEVEL\
                              , HOME_GEO, CMAT_NAGP_NAME\
                              , PARTNER_TYPE, PRIMARY_BUSINESS_MODEL\
                              , BUSINESS_MODEL_STATUS,PRIMARY_CDM\
                              , PRIMARY_PURCHASING_CHANNEL,cast("+start_date+" as date)ENTRY_DATE\
                              from lkp_query_table \
                              where PARTNER_ID_LOOKUP IS NULL")
            partner_details.show(5)	

            # Checking file_record count
            src_count=str(partner_details.count())
            print("     \n Source Count : "+src_count+"\n")
			
            # Calling Job Class method --> load_data_oracle()
            Ebi_read_write_obj.load_data_oracle(partner_details,target_table_name,save_mode,db_prop_key_load)

            # getTargetDataCount
            dest_count = str(Ebi_read_write_obj.get_target_data_count(target_table_name,db_prop_key_load))
            print("\n Target Table Count(After write) : " +dest_count+"\n")
            end_date="'"+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"'"

            # Log Format
            data_format = "JOB START DT : "+start_date+" | SCRIPT NAME : "+script_name+" | JOB : "+app_name+" | SRC COUNT : "+src_count+" | TGT COUNT : "+dest_count+" | JOB END DT : "+end_date+" | STATUS : %(message)s"

            Ebi_read_write_obj.create_log(data_format,log_file,logger)
            logger.info("Success")
            print("     \n Job "+app_name+" Succeed \n")

        except Exception as err:

            # Write expeption in spark log or console
            data_format = "JOB START DT : "+start_date+" | SCRIPT NAME : "+script_name+" | JOB : "+app_name+" | SRC COUNT : "+src_count+" | TGT COUNT : "+dest_count+" | JOB END DT : "+end_date+" | STATUS : %(message)s"

            Ebi_read_write_obj.create_log(data_format,log_file,logger)

            logger.info("[Error] Failed")
            print("     \n Job "+app_name+" Failed\n")
            logger.error("\n __main__ SCH_job --> Exception-Traceback :: " + str(err))

            raise

# Entry point for script
if __name__ == "__main__":
        # Calling main() method
        main()			
