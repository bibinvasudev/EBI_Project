# Auther : bibin
# Date : 09/20/2018
# Sample Usage of SCH Job [Reading from Oracle(Source) save/load to Oracle(Target)]

# Importing required Lib

"""
SCH_job.py
~~~~~~~~~~

This Python module contains an example Apache Spark ETL job definition
that implements best practices for production ETL jobs. It can be
submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise). For
example, this example script can be executed as follows,

    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/project.properties \
    jobs/SCH_job.py

where packages.zip contains Python modules required by ETL job (in
this example it contains a class to provide access to Spark's logger),
which need to be made available to each executor process on every node
in the cluster; etl_config.json is a text file sent to the cluster,
containing a JSON object with all of the configuration parameters
required by the ETL job; and, etl_job.py contains the Spark application
to be executed by a driver process on the Spark master node.

For more details on submitting Spark applications, please see here:
http://spark.apache.org/docs/latest/submitting-applications.html

Our chosen approach for structuring jobs is to separate the individual
'units' of ETL - the Extract, Transform and Load parts - into dedicated
functions, such that the key Transform steps can be covered by tests
and jobs or called from within another environment (e.g. a Jupyter or
Zeppelin notebook).
"""

from dependencies.spark import start_spark
from dependencies.EbiReadWrite import EbiReadWrite
import logging
from time import gmtime, strftime
import py4j

# Spark logging
logger = logging.getLogger(__name__)

# project property filename with Full path
etl_config_file = 'configs/etl_config.json'
#propertyFile = sys.arv[0]          
# appName --> Name of the Job (AppName for Spark application)
app_name = 'SCH_JOB'

# DB prop Key of Source DB
db_prop_key_extract = 'EBI_EDW'

# DB prop Key of target DB
db_prop_key_load = 'EBI_EDW'
# targetTableName
targetTableName = 'DIMS.SP_ES_SYSTEM_DISK_MODEL'

# Variable for logging purpose 
timestamp =strftime('%Y%m%d', gmtime())            
logFilename = app_name + '_' + timestamp + '.log'                    
start_date=strftime('%Y-%m-%d %H:%M:%S', gmtime())
end_date=strftime('%Y-%m-%d %H:%M:%S', gmtime())


# Query for Sample data
def queryData():
        p_CreateUser = 'ETLADM'
        p_UpdateUser  = 'ETLADM'
        p_JobGroupName = 'GROUP1'
        Runid ='12'
        
        query = "(SELECT \
            B.SP_SYS_RISK_KEY, \
            A.SERIAL_NUMBER AS BK_SYSTEM_SERIAL_NUMBER, \
            A.RISK_CATEGORY AS BK_RISK_CATEGORY, \
            A.RISK_DETAIL AS BK_RISK_DETAIL, \
            A.RISK_LEVEL AS BK_RISK_LEVEL, \
            A.WK_TO_WK_CHANGE, \
            A.RISK_AGE_IN_WKS, \
            A.RISK_AGE_DATE, \
            A.ERROR_DETAIL, \
            A.ACKNOWLEDGED_BY, \
            A.APPROVED_BY, \
            A.ACKNOWLEDGED_DATE, \
            A.JUSTIFICATION, \
            A.OPERATING_SYSTEM, \
            A.IS_PUBLIC, \
            A.RISK_ID, \
            A.BURT_ID, \
            A.FILE_NAME, \
            '"+p_CreateUser+"' AS EDW_CREATE_USER, \
                     SYSDATE AS EDW_CREATE_DATE , \
                     '"+p_UpdateUser+"' AS EDW_UPDATE_USER , \
                     SYSDATE AS EDW_UPDATE_DATE , \
                   '"+p_JobGroupName+"' AS  ETL_JOB_GROUP_NAME , \
                   '"+Runid+"' AS ETL_JOB_GROUP_RUN_ID  , \
                    1 as DUMMY \
            FROM ETL.SP_ES_SYSTEM_RISKS A \
            LEFT OUTER JOIN DIMS.SP_SYSTEM_RISKS B \
            ON A.SERIAL_NUMBER = B.BK_SYSTEM_SERIAL_NUMBER \
            AND A.RISK_CATEGORY=B.BK_RISK_CATEGORY \
            AND A.RISK_DETAIL=B.BK_RISK_DETAIL \
            AND A.RISK_LEVEL=B.BK_RISK_LEVEL)"
                    
        return query

           
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
            
            #SQL Query
            query = queryData()                     
            
            # Calling Job Class method --> loadDataOracle()
            dataFrame = Ebi_read_write_obj.extract_data_oracle(query,db_prop_key_extract)

            src_count = str(dataFrame.count())
            print(src_count)

            # Calling Job Class method --> saveDataOracle()
            #Ebi_read_write_obj.load_data_oracle(dataFrame,targetTableName,'append',db_prop_key_load)
            # getTargetDataCount
            #dest_count = str(Ebi_read_write_obj.get_target_data_count(targetTableName,db_prop_key_load))
            
            
            logDataFormat = start_date +' | '+ app_name + ' | '+ src_count +' | '+dest_count+' | '+end_date+' | %(message)s'
            print(logDataFormat)
            Ebi_read_write_obj.create_log(logDataFormat,logFilename,logger)
            
            logger.info("success")
        except py4j.protocol.Py4JJavaError:
            # Write exception (if any) in Log file 
            logDataExp = start_date +' | '+ app_name +' | '+ src_count +' | '+dest_count+' | '+end_date+' | %(message)s'
            Ebi_read_write_obj.create_log(logDataExp,logFilename,logger)
        except Exception as err:
            # Write expeption in spark log or console
            logger.error('\n __main__ SCH_job --> Exception-Traceback :: ' + str(err))
            raise
        

# Entry point for script
if __name__ == '__main__':
    # Calling main() method
	main()

