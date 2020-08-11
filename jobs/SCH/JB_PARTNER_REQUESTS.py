# SCH1052.sh  --> JB_PARTNER_REQUESTS.py

#**************************************************************************************************************
#
# Created by  : Vinay Kumbakonam
# Modified by : bibin
# Version      : 1.2
#
# Description  :
#        1. Reads the AMER, APAC, EMEA Consolidated Weekly Payment Summar xlsx files.
#        2. Based on above files data performs DataFrame level Union operations and Loads into STG Temporary Oracle Table.
#        3. Performs Oracle level lookup join operations and Writes into desctiantion Oracle Table.
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-10-16                     Initial creation
# 2018-10-30                    bibin : Getting DB schema, db_prop_key_load from Config file
# 2018-11-02                    bibin : Using job_debugger_print() for print any string in Job
#
#**************************************************************************************************************

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
script_name = "SCH1052.SH"
app_name = "JB_PARTNER_REQUESTS"
log_filename = app_name + '_' + log_date + '.log'

# Worksheet Read Details
read_file_amer=sys.argv[0]
read_file_apac=sys.argv[1]
read_file_emea=sys.argv[2]

sheet_name = 'Sheet1'
use_header = 'true'
infer_schema = 'false'
src_count = '0'

# Target Table Details
temp_table = 'WORK_PARTNER_REQUESTS'
write_table = 'PARTNER_REQUESTS'


# Oracle write Details
save_mode = 'append'
dest_table_count = '0'

lkp_query = "(select trim(BUDGET) as BUDGET, OBJECTIVE\
              , trim(VERTICAL) as VERTICAL, CITY\
              , CURRENCY, REQUESTED\
              , APPROVED, PAID\
              , HFLOF, PARTNER_GEO\
              , PARTNER_AREA, PARTNER_REGION\
              , PARTNER_COUNTRY, BUDGET_TYPE\
              , BUSINESS_MODEL, PARTNER_LEVEL\
              , PARTNER_NAME, SOLD_TO_PARTNER\
              , cast(REQUEST_SUBMIT_DATE as DATE)REQUEST_SUBMIT_DATE\
              , cast(REQUEST_APPROVED_DATE as DATE)REQUEST_APPROVED_DATE\
              , cast(ACTIVITY_START_DATE as DATE)ACTIVITY_START_DATE\
              , cast(ACTIVITY_END_DATE as DATE)ACTIVITY_END_DATE\
              , cast(CLAIM_DUE_DATE as DATE)CLAIM_DUE_DATE\
              , DATE_CANCELLED, REQUEST_NUMBER\
              , REQUEST_STATUS, ACTIVITY_AREA\
              , ACTIVITY_REGION, ACTIVITY_TYPE\
              , ACTIVITY_COUNTRY, ACTIVITY_STATE\
              , NETAPP_FX_RATE_AT_REQ_SBMT_DT, CLAIM_SUBMITTED\
              , CLAIM_PASSED_AUDIT, AMOUNT_EXPIRED\
              , AMOUNT_CANCELLED, OPEN_APPROVED_AMOUNT\
              , BUDGET_TYPE_NAME, REQUEST_APPROVED_AMOUNT\
              , HOUSE_ACCT_EXP_REQ_APPRAMT, NONHOUSE_ACCT_EXP_REQ_APPRAMT\
              , QUARTER_KEY, cast(ENTRY_DATE as date)ENTRY_DATE\
              , HOUSE_ACCT_EXP_OPEN_APPRAMT, NONHOUSE_ACCT_EXP_OPEN_APPRAMT\
              , FISCAL_QTR, ACTIVITY_DESCRIPTION\
              , GOVT_OFFICIAL_FLAG, ACTIVITY_NAME\
              , PARTNER_ID, GOVT_GIFT_FLAG FROM\
                       (select a.BUDGET, a.OBJECTIVE\
                        , a.VERTICAL, a.CITY\
                        , a.CURRENCY, a.REQUESTED\
                        , a.APPROVED, a.PAID\
                        , a.HFLOF, a.PARTNER_GEO\
                        , a.PARTNER_AREA, a.PARTNER_REGION\
                        , a.PARTNER_COUNTRY, a.BUDGET_TYPE\
                        , a.BUSINESS_MODEL, a.PARTNER_LEVEL\
                        , upper(a.PARTNER_NAME) as PARTNER_NAME\
                        , a.SOLD_TO_PARTNER, a.REQUEST_SUBMIT_DATE\
                        , a.REQUEST_APPROVED_DATE, a.ACTIVITY_START_DATE\
                        , a.ACTIVITY_END_DATE, a.CLAIM_DUE_DATE\
                        , a.DATE_CANCELLED, a.REQUEST_NUMBER\
                        , a.REQUEST_STATUS, a.ACTIVITY_AREA\
                        , a.ACTIVITY_REGION, a.ACTIVITY_TYPE\
                        , a.ACTIVITY_COUNTRY, a.ACTIVITY_STATE\
                        , a.NETAPP_FX_RATE_AT_REQ_SBMT_DT, a.CLAIM_SUBMITTED\
                        , a.CLAIM_PASSED_AUDIT, a.AMOUNT_EXPIRED\
                        , a.AMOUNT_CANCELLED, a.OPEN_APPROVED_AMOUNT\
                        , a.BUDGET_TYPE_NAME, a.REQUEST_APPROVED_AMOUNT\
                        , a.HOUSE_ACCT_EXP_REQ_APPRAMT, a.NONHOUSE_ACCT_EXP_REQ_APPRAMT\
                        , a.HOUSE_ACCT_EXP_OPEN_APPRAMT, a.NONHOUSE_ACCT_EXP_OPEN_APPRAMT\
                        , a.FISCAL_QTR, a.ACTIVITY_DESCRIPTION\
                        , a.GOVT_OFFICIAL_FLAG, a.GOVT_GIFT_FLAG\
                        , a.ACTIVITY_NAME, a.ENTRY_DATE\
                        , b.FISCAL_QUARTER_KEY as QUARTER_KEY\
                        , c.PARTNER_ID from T2_SALES_CHANNEL.WORK_PARTNER_REQUESTS a \
               LEFT OUTER JOIN \
                       (SELECT fiscal_year_short_text||fiscal_qtr_short_text as QTR_TEXT\
                        , fiscal_quarter_key from T2_SALES_CHANNEL.fiscal_quarter) b\
               ON a.NET_APP_FISCAL = b.QTR_TEXT\
               LEFT OUTER JOIN\
                       (SELECT TRIM(UPPER(PARTNER_NAME)) PARTNER_NAME,PARTNER_ID\
                        FROM (SELECT A.*, RANK() OVER (PARTITION BY A.PARTNER_NAME ORDER BY A.BUSINESS_MODEL_STATUS ) AS RANK_1\
                        FROM T2_SALES_CHANNEL.PARTNER_DETAILS A ORDER BY BUSINESS_MODEL_STATUS )A WHERE RANK_1 = 1)c \
               ON upper(a.PARTNER_NAME) = c.PARTNER_NAME where a.PARTNER_GEO is not null)v)"

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
            stg_table_name = db_schema+"."+temp_table

            # Create class Object
            Ebi_read_write_obj = EbiReadWrite(app_name,spark,config,logger)

            # Calling Job Class method --> loadWorksheetData()
            amer_df = Ebi_read_write_obj.extract_data_worksheet(read_file_amer,sheet_name,use_header,infer_schema)
            apac_df = Ebi_read_write_obj.extract_data_worksheet(read_file_apac,sheet_name,use_header,infer_schema)
            emea_df = Ebi_read_write_obj.extract_data_worksheet(read_file_emea,sheet_name,use_header,infer_schema)

            #Creating Temp Table
            amer_df.createOrReplaceTempView("amer_temp")
            apac_df.createOrReplaceTempView("apac_temp")
            emea_df.createOrReplaceTempView("emea_temp")

            # Unioning above three DataFrames
            mdf_union = spark.sql("select * from amer_temp union\
                         select * from apac_temp union\
                         select * from emea_temp").createOrReplaceTempView("mdf_view")

            mdf_union_view = spark.sql("select BUDGET, OBJECTIVE\
                             , VERTICAL, CITY\
                             , CURRENCY, REQUESTED\
                             , APPROVED, PAID\
                             , HFLOF, `Partner Geo` as PARTNER_GEO\
                             , `Partner Area` as PARTNER_AREA\
                             , `Partner Region` as PARTNER_REGION\
                             , `Partner Country` as PARTNER_COUNTRY\
                             , `Budget Type` as BUDGET_TYPE\
                             , `Business Model` as BUSINESS_MODEL\
                             , `Partner Level` as PARTNER_LEVEL\
                             , `Partner Name` as PARTNER_NAME\
                             , `Sold To Partner` as SOLD_TO_PARTNER\
                             , cast(UNIX_TIMESTAMP(`Request Submit Date`, 'dd/MM/yyyy')as timestamp)REQUEST_SUBMIT_DATE\
                             , cast(UNIX_TIMESTAMP(`Request Approved Date`, 'dd/MM/yyyy')as timestamp)REQUEST_APPROVED_DATE\
                             , cast(UNIX_TIMESTAMP(`Activity Start Date`, 'dd/MM/yyyy')as timestamp)ACTIVITY_START_DATE\
                             , cast(UNIX_TIMESTAMP(`Activity End Date`, 'dd/MM/yyyy')as timestamp)ACTIVITY_END_DATE\
                             , cast(UNIX_TIMESTAMP(`Claim Due Date`, 'dd/MM/yyyy')as timestamp)CLAIM_DUE_DATE\
                             , cast(UNIX_TIMESTAMP(`Date Cancelled`, 'dd/MM/yyyy')as timestamp)DATE_CANCELLED\
                             , `Request Number` as REQUEST_NUMBER\
                             , `Request Status` as REQUEST_STATUS\
                             , `Activity Area` as ACTIVITY_AREA\
                             , `Activity Region` as ACTIVITY_REGION\
                             , `Activity Type` as ACTIVITY_TYPE\
                             , `Activity Country` as ACTIVITY_COUNTRY\
                             , `Activity State/Province` as ACTIVITY_STATE\
                             , `NetApp FX Rate at Request Submit Date` as NETAPP_FX_RATE_AT_REQ_SBMT_DT\
                             , `Claim Submitted` as CLAIM_SUBMITTED\
                             , `Claim Passed Audit` as CLAIM_PASSED_AUDIT\
                             , `Amount Expired` as AMOUNT_EXPIRED\
                             , `Amount Cancelled` as AMOUNT_CANCELLED\
                             , `Open Approved Amount` as OPEN_APPROVED_AMOUNT\
                             , `Budget Type Name` as BUDGET_TYPE_NAME\
                             , `Request Approved Amount (Request Status = Approved, Claim Due, Claimed)` as REQUEST_APPROVED_AMOUNT, `House Account Expense Request Approved Amount (Request Status = Approved, Claim Due, Claimed)` as HOUSE_ACCT_EXP_REQ_APPRAMT\
                             , `Non-House Account Expense Request Approved Amount (Request Status = Approved, Claim Due, Claimed)` as NONHOUSE_ACCT_EXP_REQ_APPRAMT\
                             , `House Account Expense Open Approved Amount (Request Status = Approved, Claim Due, Claimed)` as HOUSE_ACCT_EXP_OPEN_APPRAMT, `Non-House Account Expense Open Approved Amount (Request Status = Approved, Claim Due, Claimed)` as NONHOUSE_ACCT_EXP_OPEN_APPRAMT\
                             , `NetApp Fiscal FYQQ (at Request Approved Date)` as FISCAL_QTR\
                             , cast(`Activity Description` as varchar(2000))ACTIVITY_DESCRIPTION\
                             , `Will any government officials be involved in this activity?` as GOVT_OFFICIAL_FLAG, `Will any government officials involved in this activity receive any gift, entertainment, or anything of value?` as GOVT_GIFT_FLAG\
                             , `NetApp Fiscal FYQQ (at Request Approved Date)` as NET_APP_FISCAL\
                             , `Activity Name` as ACTIVITY_NAME, cast("+start_date+" as Date)ENTRY_DATE\
                             FROM mdf_view a\
                             WHERE a.`Partner Geo` is not null")

            mdf_union_view.show(4)
            # Checking file_record count
            stg_df_count=str(mdf_union_view.count())
            Ebi_read_write_obj.job_debugger_print("     \n Source Count : "+stg_df_count+"\n")

            # Calling Job Class method --> load_data_oracle()
            Ebi_read_write_obj.load_data_oracle(mdf_union_view,stg_table_name,save_mode,db_prop_key_load)

            # get Temporary Table Data Count
            temp_table_count = str(Ebi_read_write_obj.get_target_data_count(stg_table_name,db_prop_key_load))
            Ebi_read_write_obj.job_debugger_print("\n Temporary Table Count : " +temp_table_count+"\n")

            # Reading lookup query
            Ebi_read_write_obj.job_debugger_print("\n	Reading Lookup Query Table \n")
            lkp_df = Ebi_read_write_obj.extract_data_oracle(lkp_query,db_prop_key_load)

            lkp_df.show(4)
            # Writing into destination table
            Ebi_read_write_obj.load_data_oracle(lkp_df,target_table_name,save_mode,db_prop_key_load)

            # getTargetDataCount
            dest_table_count = str(Ebi_read_write_obj.get_target_data_count(target_table_name,db_prop_key_load))
            Ebi_read_write_obj.job_debugger_print("\n Target Table Count(After write) : "+dest_table_count+"\n")

            end_date="'"+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"'"

            # Log Format
            data_format = "JOB START DT : "+start_date+" | SCRIPT NAME : "+script_name+" | JOB : "+app_name+" | SRC COUNT : "+stg_df_count+" | TGT COUNT : "+dest_table_count+" | JOB END DT : "+end_date+" | STATUS : %(message)s"

            Ebi_read_write_obj.create_log(data_format,log_file,logger)
            logger.info("Success")
            Ebi_read_write_obj.job_debugger_print("     \n Job "+app_name+" Succeed \n")

        except Exception as err:
            # Write expeption in spark log or console
            data_format = "JOB START DT : "+start_date+" | SCRIPT NAME : "+script_name+" | JOB : "+app_name+" | SRC COUNT : "+src_count+" | TGT COUNT : "+dest_count+" | JOB END DT : "+end_date+" | STATUS : %(message)s"

            Ebi_read_write_obj.create_log(data_format,log_file,logger)
            logger.info("[Error] Failed")

            Ebi_read_write_obj.job_debugger_print("	\n Job "+app_name+" Failed\n")
            logger.error("\n __main__ "+ app_name +" --> Exception-Traceback :: " + str(err))
            raise

if __name__ == "__main__":
        main()

