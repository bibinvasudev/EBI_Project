# Author : bibin
# Date : 09/20/2018
# Description : Sample Usage of SCH Sheet Job [Reading from Worksheet-XLSX(Source) save/load to Oracle(Target)]

# Importing required Lib
import logging
from time import gmtime, strftime
import py4j

from dependencies.spark import start_spark
from dependencies.EbiReadWrite import EbiReadWrite



# Spark logging
logger = logging.getLogger(__name__)

# appName --> Name of the Job (AppName for Spark application)
app_name = 'SCH_SHEET_JOB'

# Sheet file name
fileName = '/home/spark/v_kumbakonam/Partner_Details.xlsx'
# Sheet option
useHeader = 'true'
inferSchema = 'true'
sheetName = 'Partner Summary'



# DB prop Key of target DB
db_prop_key_load = 'EBI_EDW'
# targetTableName
targetTableName = 'DIMS.SP_ES_SYSTEM_DISK_MODEL'


# DB property filename with path
propertyFile = '/home/spark/configFiles/project.properties' 
#propertyFile = sys.argsv[0] 
    


timeStamp =strftime('%Y%m%d', gmtime())            
logFilename = app_name + '_' + timeStamp + '.log'
        
startDate=strftime('%Y-%m-%d %H:%M:%S', gmtime())
endDate=strftime('%Y-%m-%d %H:%M:%S', gmtime())
srcCount = '0'
destCount = '0'     

#  Main method                                    
def main():
        try:
            """Main ETL script definition.
            :return: None
            """
            src_count = '0'
            dest_count = '0'

            # start Spark application and get Spark session, logger and config
            spark, config = start_spark(
                app_name='my_etl_job',
                files=['configs/etl_config.json'])

            # log that main ETL job is starting
            # log.warn('SCH_job is up-and-running')
            # Create class Object
            Ebi_read_write_obj = EbiReadWrite(app_name,spark,config,logger)
            
            
            # Calling Job Class method --> loadWorksheetData()
            dataFrame = Ebi_read_write_obj.extract_data_worksheet(fileName,sheetName,useHeader,inferSchema)
            dataFrame.show(5)
            srcCount = str(dataFrame.count())
            
            # Calling Job Class method --> saveDataOracle()
            Ebi_read_write_obj.load_data_oracle(dataFrame,targetTableName,'overwrite',db_prop_key_load)
            # getTargetDataCount
            destCount = str(Ebi_read_write_obj.get_target_data_count(targetTableName,db_prop_key_load))
            
            logDataFormat = startDate +' | '+app_name+' | '+ srcCount +' | '+destCount+' | '+endDate+' | %(message)s'
            Ebi_read_write_obj.create_log(logDataFormat,logFilename,logger)
            logger.info("success")
        except py4j.protocol.Py4JJavaError:
            # Write exception (if any) in Log file 
            logDataExp = startDate +' | '+ app_name +' | '+ srcCount +' | ' + destCount + ' | '+endDate+' | %(message)s'
            Ebi_read_write_obj.create_log(logDataExp,logger,logFilename)
        except Exception as err:
            # Write expeption in spark log or console
            logger.error('\n __main__ SCH_job --> Exception-Traceback :: ' + str(err))
            raise
        

# Entry point for script
if __name__ == '__main__':
    # Calling main() method
    main()

