# SCH1101.sh  --> JB_SALES_HIERARCHY_FLAG_N_SR.py


#**************************************************************************************************************
#
# Created by  : bibin
# Version      : 1.0
#
# Description  :
#               1. This script will load the data into 'SALES_HIERARCHY' table based on stream lookups.
#
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-11-02                    Initial creation
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
import pyspark

# Spark logging
logger = logging.getLogger(__name__)

# Date Formats
start_date = "'"+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"'"
log_date =strftime("%Y%m%d", gmtime())

# Job Naming Details
script_name = "SCH1101.SH"
app_name = "JB_SALES_HIERARCHY_FLAG_N_SR"
log_filename = app_name + '_' + log_date + '.log'


# Query for loading invoice table
def query_data(db_schema):

        query = """INSERT INTO """+ db_schema +""".SALES_HIERARCHY
        (SALES_GEOGRAPHY, SALES_MULTI_AREA, SALES_AREA, SALES_MULTI_REGION, SALES_REGION, SALES_DISTRICT, SALES_TEAM, EMPLOYEE_ID,
        SALES_REP_NUMBER, LOGIN_ID, SALES_REP_NAME, SALES_REP_ORG, COMP_PLAN_TYPE_CODE, COMP_PLAN_TITLE, COMP_PLAN_CATEGORY_CODE, COMP_PLAN_DESCRIPTION,
        GOAL_CURR_CODE, START_DATE, END_DATE, STATUS_CODE, PARTICIPANT_LEVEL_CODE, SALES_REP_TYPE_CODE, CURRENT_RECORD_FLAG, LAST_HIRE_DATE)
        SELECT
        B.WW_DIRECT_GEO_DESCRIPTION AS SALES_GEOGRAPHY,
        B.MULTI_AREA_DESCRIPTION AS SALES_MULTI_AREA,
        B.AREA_DESCRIPTION AS SALES_AREA,
        B.MULTI_REGION_DESCRIPTION AS SALES_MULTI_REGION,
        SUBSTR(B.REGION_DESCRIPTION,1,50) AS SALES_REGION,
        SUBSTR(B.DISTRICT_DESCRIPTION,1,50) AS SALES_DISTRICT,
        SUBSTR(B.TEAM_DESCRIPTION,1,50) AS SALES_TEAM,
        A.EMPLOYEE_ID,
        A.BK_SALES_REP_NUMBER AS SALES_REP_NUMBER,
        SUBSTR(A.EMP_SYS_LOGIN_ID,1,10) AS LOGIN_ID,
        SUBSTR(A.SALES_REP_NAME,1,50) AS SALES_REP_NAME,
        A.ORGANIZATION_NAME AS SALES_REP_ORG,
        A.COMP_PLAN_TYPE_CODE,
        A.COMP_PLAN_TITLE,
        A.COMP_PLAN_CATEGORY_CODE,
        A.COMP_PLAN_DESCRIPTION,
        NULL AS GOAL_CURR_CODE ,
        A.START_DATE,
        A.END_DATE,
        A.STATUS_CODE,
        A.PARTICIPANT_LEVEL_CODE,
        SUBSTR(A.SALES_REP_TYPE_CODE,1,5) AS SALES_REP_TYPE_CODE,
        A.CURRENT_RECORD_FLAG,
        C.RECENT_HIRE_DATE AS LAST_HIRE_DATE
        FROM
        (
        SELECT a.*,ROW_NUMBER() over (partition by BK_SALES_REP_NUMBER ORDER BY END_DATE desc) as RANK
        FROM DIMS.SALES_PARTICIPANT a
        WHERE
        BK_SALES_REP_NUMBER NOT IN (SELECT DISTINCT BK_SALES_REP_NUMBER FROM DIMS.SALES_PARTICIPANT WHERE CURRENT_RECORD_FLAG = 'Y')
        AND PARTICIPANT_LEVEL_CODE = 'SR'
        ORDER BY BK_SALES_REP_NUMBER,SALES_PARTICIPANT_KEY
        ) A
        INNER JOIN DIMS.SALES_TERR_HIERAR_AS_IS_MV B ON B.TERRITORY_KEY = A.TERRITORY_KEY
        LEFT OUTER JOIN
        (SELECT LTRIM(BK_EMPLOYEE_ID,'0') BK_EMPLOYEE_ID,RECENT_HIRE_DATE FROM DIMS.WORKER_DETAIL WHERE CURRENT_RECORD_IND = 1 ) C
        ON C.BK_EMPLOYEE_ID = A.EMPLOYEE_ID
        WHERE RANK  =  1"""

        return query


#  Main method
def main():
        try:
            src_count = '0'
            dest_count = '0'

            # start Spark application and get Spark session, logger and config
            spark, config = start_spark(
                app_name=app_name)

            # Create class Object
            Ebi_read_write_obj = EbiReadWrite(app_name,spark,config,logger)

            # DB prop Key of Source DB
            db_prop_key_load = config['DB_PROP_KEY_LOAD']
            db_prop_key_extract =  config['DB_PROP_KEY_EXTRACT']	
            db_schema = config['DB_SCHEMA']
            log_file = config['LOG_DIR_NAME'] + "/" + log_filename


            #SQL Query
            query = query_data(db_schema)

            # Calling Job Class method --> get_target_data_update()
            Ebi_read_write_obj.get_target_data_update(query,db_prop_key_load)

            end_date="'"+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"'"

            data_format = "JOB START DT : "+start_date+" | SCRIPT NAME : "+script_name+" | JOB : "+app_name+" | SRC COUNT : "+src_count+" | TGT COUNT : "+dest_count+" | JOB END DT : "+end_date+" | STATUS : %(message)s"

            Ebi_read_write_obj.create_log(data_format,log_file,logger)

            logger.info("Success")
            Ebi_read_write_obj.job_debugger_print("     \n  __main__ " + app_name +" --> Job "+app_name+" Succeed \n")

        except Exception as err:
            # Write expeption in spark log or console
            end_date="'"+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"'"
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


