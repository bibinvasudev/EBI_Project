"""
# @Author : bibin
# @Date : 09/20/2018
# @Class description : EbiReadWrite Class for Spark Jobs :
#     *** Reading from Oracle(Source) save/load to Oracle(Target)
#     *** Reading from Worksheet-XLSX(Source) save/load to Oracle(Target)
"""

# Importing required Lib
#from pyspark.sql import SparkSession
import logging
import cx_Oracle
#import ConfigParser
#from six.moves import configparser
from properties import p
from pyspark.sql import functions



class EbiReadWrite:

    # Spark Logging
    logger = logging.getLogger(__name__)

    def __init__(self, app_name, spark_session,prop_dict,log):
        # Spark application name / JOB name
        self.app_name = app_name
        # Spark Session
        self.spark_session = spark_session
        # Config JSON file into DICT key-value
        self.prop_dict = prop_dict
        # Spark Logging
        self.log = log

    def DBConnection(self,db_prop_key):
        """
        # @Method description : Get the DB details from configFiles\project.propertiesl in array dbDetails
        # @Parameter : dbPropKey - Key for DB you wants to connect
        #     For Exmaple : 'EBI_EDW' will be the dbPropKey
        #       EBI_EDW_dbname=XXX
        #       EBI_EDW_host=XXX
        #       EBI_EDW_port=XXX
        #       EBI_EDW_username=XXX
        #       EBI_EDW_password=XXX
        """

        db_details = {}

        db_details['dbname'] = self.prop_dict[db_prop_key + '_DBNAME']
        db_details['host'] = self.prop_dict[db_prop_key +  '_HOST']
        db_details['port'] = self.prop_dict[db_prop_key +  '_PORT']
        db_details['username'] = self.prop_dict[db_prop_key +  '_USERNAME']
        db_details['password'] = self.prop_dict[db_prop_key +  '_PASSWORD']
        
        return db_details
    
    def extract_data_worksheet(self,filename,sheet_name,use_header,infer_schema):

        """
        # @Method description :: Reading one worksheet (sheetName) in XLSX file (fileName)
        # @Parameter 1 :: filename : "Partner_Details.xlsx"
        # @Parameter 2 :: sheet_name : "Partner Summary"
        # @Parameter 3 :: app_name : Name of the Job or AppName for Spark application
        # @Parameter 4 :: use_header : 'true' or 'false'
        # @Parameter 5 :: infer_schema : 'true' or 'false'
        """

        try:
            dataFrame = self.spark_session.read.format("com.crealytics.spark.excel") \
                .option("path", filename) \
                .option("inferSchema", infer_schema) \
                .option("useHeader", use_header) \
                .option("sheetName", sheet_name).load()
            return dataFrame
        except Exception as e:
            self.log.error('\n Class :: EbiReadWrite --> Method :: extract_data_worksheet() --> Exception-Traceback :: ' + str(e))

    

    def extract_data_oracle(self,query,db_prop_key): 

        """
        # @Method description :: Read the Data from oracle DB (Source) and return dataFrame
        # @Parameter 1 :: query -- SQL Query
        # @Parameter 2 :: db_prop_key - Key for DB you wants to connect
        #     For Exmaple : 'EBI_EDW' will be the dbPropKey
        #       EBI_EDW_dbname=XXX
        #       EBI_EDW_host=XXX
        #       EBI_EDW_port=XXX
        #       EBI_EDW_username=XXX
        #       EBI_EDW_password=XXX
        """
        try:
            
            db_details = self.DBConnection(db_prop_key)
        
            
            dbname = db_details['dbname']
            host = db_details['host']
            port = db_details['port']
            username = db_details['username']    
            password = db_details['password']
            
            ORACLE_CONNECTION_URL  = 'jdbc:oracle:thin:'+ username +'/'+ password +'@//'+ host +':'+ port +'/' + dbname



            properties={
            'user': username,
            'password':password,
            'driver':'oracle.jdbc.driver.OracleDriver'
            }  
            
            #EbiReadWrite.logger.error('\n Class :: EbiReadWrite --> Method :: extract_data_oracle --> Oracle_CONNECTION_URL :: '+ORACLE_CONNECTION_URL)
            dataframe = self.spark_session.read.jdbc(url=ORACLE_CONNECTION_URL, table=query, properties=properties)
            return dataframe        
        except Exception as e:
            self.log.error('\n Class :: EbiReadWrite --> Method :: extract_data_oracle() --> Exception-Traceback :: ' + str(e))
            raise
            


    def create_log(self,data_format,log_filename ,log ):
        """
        # @Method description :: Creating Log file
        # @Parameter 1 :: data_format -- Log file data
        # @Parameter 2 :: log_filename -- Log filename
        # @Parameter 3 :: log -- log object
        """
        logging.basicConfig(filename=log_filename,format=data_format)
        log.setLevel(logging.DEBUG)

    
    def load_data_oracle(self,dataframe,db_table,save_mode,db_prop_key):
        """
        # @Method description :: Saving a Data in Oracle DB (Target)
        # @Parameter 1 :: dataframe - Data Frame (Spark) which  needs to save in target table
        # @Parameter 2 :: db_table - Name of target table
        # @Parameter 3 :: save_mode - Save mode (Spark) like 'overwrite' or 'append' or 'ignore'
        # @Parameter 4 :: db_prop_key - Key for DB you wants to connect
        #     For Exmaple : 'EBI_EDW' will be the dbPropKey
        #       EBI_EDW_dbname=XXX
        #       EBI_EDW_host=XXX
        #       EBI_EDW_port=XXX
        #       EBI_EDW_username=XXX
        #       EBI_EDW_password=XXX
        """
        try: 
            db_details = self.DBConnection(db_prop_key)
            
            dbname = db_details['dbname']
            host = db_details['host']
            port = db_details['port']
            username = db_details['username']    
            password = db_details['password']            
            
            ORACLE_CONNECTION_URL = 'jdbc:oracle:thin:'+ username +'/'+ password +'@//'+ host +':'+ port +'/' + dbname

            dataframe.write.format('jdbc').options( \
            url=ORACLE_CONNECTION_URL, \
            dbtable=db_table, \
            #batchsize="4000", \
            user=username,  \
            password=password).mode(save_mode).save()
        except Exception as e:
            self.log.error('\n Class :: EbiReadWrite --> Method :: load_data_oracle() --> Exception-Traceback :: ' + str(e))
            raise

    
         
    def get_target_data_count(self,target_table_name,db_prop_key):

        """
        # @Method description :: Get the saved data count in Target table for logging purpose only
        # @Parameter 1 ::  targetTableName - Target table name with schema for example : DIMS.SP_ES_SYSTEM_DISK_MODEL
        # @Parameter 2 :: dbPropKey - Key for DB you wants to connect
        #     For Exmaple : 'EBI_EDW' will be the dbPropKey
        #       EBI_EDW_dbname=XXX
        #       EBI_EDW_host=XXX
        #       EBI_EDW_port=XXX
        #       EBI_EDW_username=XXX
        #       EBI_EDW_password=XXX
        """
        try:
            db_details = self.DBConnection(db_prop_key)
            
            dbname = db_details['dbname']
            host = db_details['host']
            port = db_details['port']
            username = db_details['username']    
            password = db_details['password']
            
            ORACLE_CONNECTION_URL = username +'/'+ password +'@//'+ host +':'+ port +'/' + dbname      
            con = cx_Oracle.connect(ORACLE_CONNECTION_URL)
            cur = con.cursor()
            dest_count=cur.execute('select count(*) from '+ target_table_name).fetchone()
            return dest_count[0]
        except Exception as e:
            self.log.error('\n Class :: EbiReadWrite --> Method :: get_target_data_count() --> Exception-Traceback :: ' + str(e))
            raise


    def extract_data_cursor_oracle(self,query,db_prop_key):

        """
        # @Method description :: Get the saved data count in Target table for logging purpose only
        # @Parameter 1 ::  targetTableName - Target table name with schema for example : DIMS.SP_ES_SYSTEM_DISK_MODEL
        # @Parameter 2 :: dbPropKey - Key for DB you wants to connect
        #     For Exmaple : 'EBI_EDW' will be the dbPropKey
        #       EBI_EDW_dbname=XXX
        #       EBI_EDW_host=XXX
        #       EBI_EDW_port=XXX
        #       EBI_EDW_username=XXX
        #       EBI_EDW_password=XXX
        """
        try:
            db_details = self.DBConnection(db_prop_key)

            dbname = db_details['dbname']
            host = db_details['host']
            port = db_details['port']
            username = db_details['username']
            password = db_details['password']

            ORACLE_CONNECTION_URL = username +'/'+ password +'@//'+ host +':'+ port +'/' + dbname
            con = cx_Oracle.connect(ORACLE_CONNECTION_URL)
            cur = con.cursor()
            #result =cur.execute(query).fetchmany()
            result =cur.execute(query).fetchone()
            return result[0]
        except Exception as e:
            self.log.error('\n Class :: EbiReadWrite --> Method :: extract_data_cursor_oracle() --> Exception-Traceback :: ' + str(e))
            raise



    def truncate_table(self,table_name,schema,procedure,db_prop_key):
        """
        # @Method description :: truncate the table
        # @Parameter 1 ::  table_name - Name of the DB table which needs to truncate
        # @Parameter 2 ::  schema - DB schema from where table needs to truncate table
        # @Parameter 3 ::  procedure - SP name
        # @Parameter 4 :: dbPropKey - Key for DB you wants to connect
        #     For Exmaple : 'EBI_EDW' will be the dbPropKey
        #       EBI_EDW_dbname=XXX
        #       EBI_EDW_host=XXX
        #       EBI_EDW_port=XXX
        #       EBI_EDW_username=XXX
        #       EBI_EDW_password=XXX
        """
        truncateTable = False
        try:
            db_details = self.DBConnection(db_prop_key)

            dbname = db_details['dbname']
            host = db_details['host']
            port = db_details['port']
            username = db_details['username']
            password = db_details['password']

            ORACLE_CONNECTION_URL = username +'/'+ password +'@//'+ host +':'+ port +'/' + dbname
            con = cx_Oracle.connect(ORACLE_CONNECTION_URL)
            cur = con.cursor()
             # Calling Oracle Procedure
            if (cur.callproc(schema+"."+procedure,[schema,table_name])):
                truncateTable = True
            return truncateTable
        except Exception as e:
            self.log.error("\n Class :: EbiReadWrite --> Method :: truncate_table() --> Exception-Traceback :: " + str(e))
            raise


    def truncate_table_partition(self,table_name,schema,procedure,v_partition,db_prop_key):
        """
        # @Method description :: truncate the table by partition
        # @Parameter 1 ::  table_name - Name of the DB table which needs to truncate
        # @Parameter 2 ::  schema - DB schema from where table needs to truncate table
        # @Parameter 3 ::  procedure - SP name
        # @Parameter 4 :: dbPropKey - Key for DB you wants to connect
        #     For Exmaple : 'EBI_EDW' will be the dbPropKey
        #       EBI_EDW_dbname=XXX
        #       EBI_EDW_host=XXX
        #       EBI_EDW_port=XXX
        #       EBI_EDW_username=XXX
        #       EBI_EDW_password=XXX
        """
        truncateTable = False
        try:
            db_details = self.DBConnection(db_prop_key)

            dbname = db_details['dbname']
            host = db_details['host']
            port = db_details['port']
            username = db_details['username']
            password = db_details['password']

            ORACLE_CONNECTION_URL = username +'/'+ password +'@//'+ host +':'+ port +'/' + dbname
            con = cx_Oracle.connect(ORACLE_CONNECTION_URL)
            cur = con.cursor()
             # Calling Oracle Procedure
            if (cur.callproc(schema+"."+procedure,[schema,table_name,v_partition])):
                truncateTable = True
            return truncateTable
        except Exception as e:
            self.log.error("\n Class :: EbiReadWrite --> Method :: truncate_table_partition() --> Exception-Traceback :: " + str(e))
            raise

    def get_fiscal_month(self,db_schema,db_prop_key):

            """
            # @Method description :: get fiscal param
            # @Parameter 1 ::  db_schema - schema
            # @Parameter 2 :: db_prop_key - Key for DB you wants to connect
            #     For Exmaple : 'EBI_EDW' will be the dbPropKey
            #       EBI_EDW_dbname=XXX
            #       EBI_EDW_host=XXX
            #       EBI_EDW_port=XXX
            #       EBI_EDW_username=XXX
            #       EBI_EDW_password=XXX
            """
            try:

                query = "(SELECT DISTINCT \
                CASE  \
                when FISCAL_MONTH_OF_QTR_NUMBER = 1 then  \
                concat(concat(q'|'|', CONCAT(SUBSTR(CURR_QTR_TEXT,5,2),'M1') ),q'|'|')||q'|,|'||concat(concat(q'|'|', CONCAT(SUBSTR(CURR_QTR_TEXT,5,2),'M2') ),q'|'|')||q'|,|'||concat(concat(q'|'|', CONCAT(SUBSTR(CURR_QTR_TEXT,5,2),'M3') ),q'|'|')  \
                WHEN FISCAL_MONTH_OF_QTR_NUMBER = 2 THEN  \
                concat(concat(q'|'|',SUBSTR(FISCAL_YEAR_QTR_TEXT,5,2)||'M1'),q'|'|')  \
                WHEN FISCAL_MONTH_OF_QTR_NUMBER = 3 THEN  \
                concat(concat(q'|'|', CONCAT(SUBSTR(FISCAL_YEAR_QTR_TEXT,5,2),'M1') ),q'|'|')||q'|,|'||concat(concat(q'|'|', CONCAT(SUBSTR(FISCAL_YEAR_QTR_TEXT,5,2),'M2') ),q'|'|')  \
                end PARAM_FISCAL_MONTH,  \
                CASE WHEN FISCAL_MONTH_OF_QTR_NUMBER = 1 THEN concat(concat(q'|'|',CURR_QTR_TEXT),q'|'|') ELSE concat(concat(q'|'|',FISCAL_YEAR_QTR_TEXT),q'|'|') END PARAM_FISCAL_YEAR_QUARTER_TEXT  \
                FROM "+ db_schema + ".CALENDAR a,"+ db_schema + ".QUARTER_LOOK_UP b \
                where \
                CURRENT_DAY_FLAG ='Y' and \
                B.NEXT_QTR_TEXT = a.FISCAL_YEAR_QTR_TEXT)"

                dataFrame = self.extract_data_oracle(query,db_prop_key)

                param_fiscal_month = ', '.join(dataFrame.select("PARAM_FISCAL_MONTH").rdd.flatMap(lambda x: x).collect())

                return param_fiscal_month

            except Exception as e:
                EbiReadWrite.logger.error('\n Class :: EbiReadWrite --> Method :: get_fiscal_month() --> Exception-Traceback :: ' + str(e))
                raise

    def get_fiscal_year_quarter(self,db_schema,db_prop_key):

            """
            # @Method description :: get fiscal param
            # @Parameter 1 ::  db_schema - schema
            # @Parameter 2 :: db_prop_key - Key for DB you wants to connect
            #     For Exmaple : 'EBI_EDW' will be the dbPropKey
            #       EBI_EDW_dbname=XXX
            #       EBI_EDW_host=XXX
            #       EBI_EDW_port=XXX
            #       EBI_EDW_username=XXX
            #       EBI_EDW_password=XXX
            """
            try:

                query = "(SELECT DISTINCT \
                CASE  \
                when FISCAL_MONTH_OF_QTR_NUMBER = 1 then  \
                concat(concat(q'|'|', CONCAT(SUBSTR(CURR_QTR_TEXT,5,2),'M1') ),q'|'|')||q'|,|'||concat(concat(q'|'|', CONCAT(SUBSTR(CURR_QTR_TEXT,5,2),'M2') ),q'|'|')||q'|,|'||concat(concat(q'|'|', CONCAT(SUBSTR(CURR_QTR_TEXT,5,2),'M3') ),q'|'|')  \
                WHEN FISCAL_MONTH_OF_QTR_NUMBER = 2 THEN  \
                concat(concat(q'|'|',SUBSTR(FISCAL_YEAR_QTR_TEXT,5,2)||'M1'),q'|'|')  \
                WHEN FISCAL_MONTH_OF_QTR_NUMBER = 3 THEN  \
                concat(concat(q'|'|', CONCAT(SUBSTR(FISCAL_YEAR_QTR_TEXT,5,2),'M1') ),q'|'|')||q'|,|'||concat(concat(q'|'|', CONCAT(SUBSTR(FISCAL_YEAR_QTR_TEXT,5,2),'M2') ),q'|'|')  \
                end PARAM_FISCAL_MONTH,  \
                CASE WHEN FISCAL_MONTH_OF_QTR_NUMBER = 1 THEN concat(concat(q'|'|',CURR_QTR_TEXT),q'|'|') ELSE concat(concat(q'|'|',FISCAL_YEAR_QTR_TEXT),q'|'|') END PARAM_FISCAL_YEAR_QUARTER_TEXT  \
                FROM "+ db_schema + ".CALENDAR a,"+ db_schema + ".QUARTER_LOOK_UP b \
                where \
                CURRENT_DAY_FLAG ='Y' and \
                B.NEXT_QTR_TEXT = a.FISCAL_YEAR_QTR_TEXT)"

                dataFrame = self.extract_data_oracle(query,db_prop_key)

                param_fiscal_year_quarter = ', '.join(dataFrame.select("PARAM_FISCAL_YEAR_QUARTER_TEXT").rdd.flatMap(lambda x: x).collect())

                return param_fiscal_year_quarter

            except Exception as e:
                EbiReadWrite.logger.error('\n Class :: EbiReadWrite --> Method :: get_fiscal_year_quarter() --> Exception-Traceback :: ' + str(e))
                raise

    def get_fiscal_current_quarter(self,db_prop_key):

            """
            # @Method description :: get fiscal current quarter
            # @Parameter 1 :: db_prop_key - Key for DB you wants to connect
            #     For Exmaple : 'EBI_EDW' will be the dbPropKey
            #       EBI_EDW_dbname=XXX
            #       EBI_EDW_host=XXX
            #       EBI_EDW_port=XXX
            #       EBI_EDW_username=XXX
            #       EBI_EDW_password=XXX
            """
            try:

                query = """(SELECT FISCAL_QTR FROM ETL.PYSPARK_PARAM_SCH)"""

                dataFrame = self.extract_data_oracle(query,db_prop_key)

                fiscal_current_quarter = ''.join(dataFrame.select("FISCAL_QTR").rdd.flatMap(lambda x: x).collect())

                return fiscal_current_quarter

            except Exception as e:
                EbiReadWrite.logger.error('\n Class :: EbiReadWrite --> Method :: get_fiscal_current_quarter() --> Exception-Traceback :: ' + str(e))
                raise

    def get_debooked_fiscal_quarter(self,db_schema,db_prop_key):

            """
            # @Method description :: get DEBOOKED_FISCAL_QUARTER
            # @Parameter 1 ::  db_schema - schema
            # @Parameter 2 :: db_prop_key - Key for DB you wants to connect
            #     For Exmaple : 'EBI_EDW' will be the dbPropKey
            #       EBI_EDW_dbname=XXX
            #       EBI_EDW_host=XXX
            #       EBI_EDW_port=XXX
            #       EBI_EDW_username=XXX
            #       EBI_EDW_password=XXX
            """
            try:
                param_fiscal_year_quarter = self.get_fiscal_year_quarter(db_schema,db_prop_key)
                query = """(SELECT DISTINCT CONCAT(concat(q'|'|',FISCAL_YEAR_QTR_TEXT),q'|'|') DEBOOKED_FISCAL_QUARTER FROM DIMS.CALENDAR
                            WHERE FISCAL_YEAR_QTR_TEXT IN ("""+ param_fiscal_year_quarter +""")
                            ORDER BY CONCAT(concat(q'|'|',FISCAL_YEAR_QTR_TEXT),q'|'|'))"""

                dataFrame = self.extract_data_oracle(query,db_prop_key)

                debooked_fiscal_quarter = ''.join(dataFrame.select("DEBOOKED_FISCAL_QUARTER").rdd.flatMap(lambda x: x).collect())

                return debooked_fiscal_quarter

            except Exception as e:
                EbiReadWrite.logger.error('\n Class :: EbiReadWrite --> Method :: get_debooked_fiscal_year_quarter() --> Exception-Traceback :: ' + str(e))
                raise

    def get_v_fiscal_quarter(self,db_schema,param_fiscal_year_quarter,db_prop_key):

        """
        # @Method description :: get  V_FISCAL_QUARTER
        # @Parameter 1 ::  db_schema - schema
        # @Parameter 2 :: db_prop_key - Key for DB you wants to connect
        #     For Exmaple : 'EBI_EDW' will be the dbPropKey
        #       EBI_EDW_dbname=XXX
        #       EBI_EDW_host=XXX
        #       EBI_EDW_port=XXX
        #       EBI_EDW_username=XXX
        #       EBI_EDW_password=XXX
        """

        try:

            query = """(SELECT DISTINCT FISCAL_YEAR_QTR_TEXT V_FISCAL_QUARTER
            FROM  """+ db_schema +""".CALENDAR
            WHERE FISCAL_YEAR_QTR_TEXT IN ("""+ param_fiscal_year_quarter +"""))"""

            dataFrame = self.extract_data_oracle(query,db_prop_key)


            v_fiscal_quarter = ', '.join(dataFrame.select("V_FISCAL_QUARTER").rdd.flatMap(lambda x: x).collect())

            return v_fiscal_quarter

        except Exception as e:
            EbiReadWrite.logger.error('\n Class :: EbiReadWrite --> Method :: get_v_fiscal_quarter() --> Exception-Traceback :: ' + str(e))
            raise


    def get_v_partition(self,db_schema,table_name,db_prop_key):

        """
        # @Method description :: get  v_partition
        # @Parameter 1 ::  db_schema - schema
        # @Parameter 2 ::  table_name
        # @Parameter 3 :: db_prop_key - Key for DB you wants to connect
        #     For Exmaple : 'EBI_EDW' will be the dbPropKey
        #       EBI_EDW_dbname=XXX
        #       EBI_EDW_host=XXX
        #       EBI_EDW_port=XXX
        #       EBI_EDW_username=XXX
        #       EBI_EDW_password=XXX
        """

        try:
            param_fiscal_year_quarter = self.get_fiscal_year_quarter(db_schema,db_prop_key)
            v_fiscal_quarter = self.get_v_fiscal_quarter(db_schema,param_fiscal_year_quarter,db_prop_key)
            query = """(select PARTITION_NAME V_PARTITION from ALL_TAB_PARTITIONS
            where TABLE_NAME = '"""+ table_name +"""' and PARTITION_NAME like (
                    select distinct '%_FY'||SUBSTR(FISCAL_YEAR_QTR_TEXT,-4) from """+ db_schema +""".CALENDAR
                    WHERE FISCAL_YEAR_QTR_TEXT = '""" + v_fiscal_quarter + """' ))"""

            dataFrame = self.extract_data_oracle(query,db_prop_key)

            v_partition = ', '.join(dataFrame.select("V_PARTITION").rdd.flatMap(lambda x: x).collect())

            return v_partition

        except Exception as e:
            EbiReadWrite.logger.error('\n Class :: EbiReadWrite --> Method :: get_v_partition() --> Exception-Traceback :: ' + str(e))
            raise


    def get_target_data_update(self,query,db_prop_key):

            """
            # @Method description :: Update the target table
            # @Parameter 1 ::  query - SQL query
            # @Parameter 2 :: db_prop_key - Key for DB you wants to connect
            #     For Exmaple : 'EBI_EDW' will be the dbPropKey
            #       EBI_EDW_dbname=XXX
            #       EBI_EDW_host=XXX
            #       EBI_EDW_port=XXX
            #       EBI_EDW_username=XXX
            #       EBI_EDW_password=XXX
            """
            try:
                db_details = self.DBConnection(db_prop_key)

                dbname = db_details['dbname']
                host = db_details['host']
                port = db_details['port']
                username = db_details['username']
                password = db_details['password']

                ORACLE_CONNECTION_URL = username +'/'+ password +'@//'+ host +':'+ port +'/' + dbname
                con = cx_Oracle.connect(ORACLE_CONNECTION_URL)
                cur = con.cursor()
                update_table=cur.execute(query)
                self.job_debugger_print(update_table)

                con.commit()
            except Exception as e:
                EbiReadWrite.logger.error('\n Class :: EbiReadWrite --> Method :: get_target_data_update() --> Exception-Traceback :: ' + str(e))
                raise


    def job_debugger_print(self,print_message):

            """
            # @Method description :: Print the debugger message --- If JOB_DEBUGGER is true in etl_config.json
            # @Parameter 1 ::  print_message
            #
            """
            try:
                job_debugger = self.prop_dict['JOB_DEBUGGER']
                if job_debugger :
                    print(print_message)
            except Exception as e:
                EbiReadWrite.logger.error('\n Class :: EbiReadWrite --> Method :: job_debugger_print() --> Exception-Traceback :: ' + str(e))
                raise
