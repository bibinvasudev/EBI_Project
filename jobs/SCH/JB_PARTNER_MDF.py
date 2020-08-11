# SCH1051.sh  --> JB_PARTNER_MDF.py

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
script_name = "SCH1051.SH"
app_name = 'JB_PARTNERMDF_APAC'
log_filename = app_name + '_' + log_date + '.log'

# Worksheet Read Details
read_file_amer=sys.argv[0]
read_file_apac=sys.argv[1]
read_file_emea=sys.argv[2]

sheet_name = 'Sheet1'
use_header = 'true'
infer_schema = 'true'
src_count = '0'

# Target Table Details
lookup_table_fiscal = 'FISCAL_QUARTER'
lookup_table_partner = 'PARTNER_DETAILS'
temp_table = 'WORK_PARTNER_MDF'
write_table = 'PARTNER_MDF_BKP'

# Oracle write Details
save_mode = 'append'
dest_table_count = '0'

lkp_query = "(select trim(BUDGET) as BUDGET, CURRENCY\
              , OBJECTIVE, trim(VERTICAL) as VERTICAL\
              , PARTNER_GEO, PARTNER_AREA\
              , PARTNER_REGION, PARTNER_COUNTRY\
              , trim(ACTIVITY_NAME) as ACTIVITY_NAME, REQUEST_NUMBER\
              , CLAIM_NUMBER, PAYMENT_REFERENCE_NUMBER\
              , BUSINESS_MODEL, PARTNER_LEVEL\
              , PARTNER_NAME, SOLD_TO_PARTNER\
              , BUDGET_TYPE, CLAIM_SUBMITTED_FOR_AUDIT_DATE\
              , CLAIM_APPROVED_DATE, PAYMENT_DATE\
              , SUMMARY_INVOICE, AMOUNT_CLAIMED\
              , CLAIM_APPROVED_AMOUNT, AMOUNT_PAID\
              , ACTIVITY_AREA, ACTIVITY_REGION\
              , ACTIVITY_COUNTRY, ACTIVITY_STATE_PROVINCE\
              , ACTIVITY_TYPE, CLAIMED_APPR_AMT_LESS_AMT_PAID\
              , AWT_PYMT_WITH_NO_PYMT_REF_NUM,QUARTER_KEY\
              , FISCAL_QTR, PARTNER_ID, ENTRY_DATE from (\
                        select a.BUDGET, a.CURRENCY\
                        ,a.OBJECTIVE, a.VERTICAL\
                        , a.PARTNER_GEO, a.PARTNER_AREA\
                        , a.PARTNER_REGION, a.PARTNER_COUNTRY\
                        , a.ACTIVITY_NAME, a.REQUEST_NUMBER\
                        , a.CLAIM_NUMBER, a.PAYMENT_REFERENCE_NUMBER\
                        , a.BUSINESS_MODEL, a.PARTNER_LEVEL\
                        , upper(a.PARTNER_NAME) as PARTNER_NAME\
                        , a.SOLD_TO_PARTNER, a.BUDGET_TYPE\
                        , a.CLAIM_SUBMITTED_FOR_AUDIT_DATE\
                        , a.CLAIM_APPROVED_DATE, a.PAYMENT_DATE\
                        , a.SUMMARY_INVOICE, a.AMOUNT_CLAIMED\
                        , a.CLAIM_APPROVED_AMOUNT, a.AMOUNT_PAID\
                        , a.ACTIVITY_AREA, a.ACTIVITY_REGION\
                        , a.ACTIVITY_COUNTRY, a.ACTIVITY_STATE_PROVINCE\
                        , a.ACTIVITY_TYPE, a.CLAIMED_APPR_AMT_LESS_AMT_PAID\
                        , a.AWT_PYMT_WITH_NO_PYMT_REF_NUM, a.ENTRY_DATE\
                        , b.FISCAL_QUARTER_KEY as QUARTER_KEY\
                        , b.FISCAL_YEAR_QTR_TEXT as FISCAL_QTR\
                        , c.PARTNER_ID\
              from t2_sales_channel.work_partner_mdf a\
              left outer join\
                        (select fiscal_year_short_text||fiscal_qtr_short_text as QTR_TEXT\
                        , fiscal_quarter_key,FISCAL_YEAR_QTR_TEXT\
                        From t2_sales_channel.fiscal_quarter) b\
              on a.NET_APP_FISCAL = b.QTR_TEXT left outer join \
                        (SELECT Partner_ID as Partner_ID,Partner_ID as Distributor_Partner_ID\
                        , TRIM(UPPER(Partner_Name)) Partner_Name FROM\
                        (SELECT A.*, RANK() OVER (PARTITION BY PARTNER_NAME ORDER BY BUSINESS_MODEL_STATUS ) AS RANK_1\
                        FROM t2_sales_channel.PARTNER_DETAILS A\
                        ORDER BY BUSINESS_MODEL_STATUS )WHERE RANK_1 = 1) c \
              on upper(a.PARTNER_NAME) = c.PARTNER_NAME\
              where a.PARTNER_GEO is not null)v)"

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
            mdf_union = spark.sql("select * from amer_temp union \
                        select * from apac_temp union \
                        select * from emea_temp").createOrReplaceTempView("mdf_view")

            mdf_union_view = spark.sql("select a.BUDGET,a.CURRENCY,a.OBJECTIVE,a.VERTICAL\
                                      , a.`Partner Geo` as PARTNER_GEO, a.`Partner Area` as PARTNER_AREA\
                                      , a.`Partner Region` as PARTNER_REGION, a.`Partner Country` as PARTNER_COUNTRY\
                                      , a.`Activity Name` as ACTIVITY_NAME, a.`Request Number` as REQUEST_NUMBER\
                                      , a.`Claim Number` as CLAIM_NUMBER, a.`Payment Reference Number` as PAYMENT_REFERENCE_NUMBER\
                                      , a.`Business Model` as BUSINESS_MODEL, a.`Partner Level` as PARTNER_LEVEL\
                                      , upper(a.`Partner Name`) as PARTNER_NAME, a.`Sold To Partner` as SOLD_TO_PARTNER\
                                      , a.`Budget Type` as BUDGET_TYPE, a.`Claim Submitted for Audit Date` as CLAIM_SUBMITTED_FOR_AUDIT_DATE\
                                      , a.`Claim Approved Date` as CLAIM_APPROVED_DATE, a.`Payment Date` as PAYMENT_DATE\
                                      , a.`Summary Invoice` as SUMMARY_INVOICE, a.`Amount Claimed` as AMOUNT_CLAIMED\
                                      , a.`Claim Approved Amount` as CLAIM_APPROVED_AMOUNT, a.`Amount Paid` as AMOUNT_PAID\
                                      , a.`Activity Area` as ACTIVITY_AREA, a.`Activity Region` as ACTIVITY_REGION\
                                      , a.`Activity Country` as ACTIVITY_COUNTRY, a.`Activity State/Province` as ACTIVITY_STATE_PROVINCE\
                                      , a.`Activity Type` as ACTIVITY_TYPE\
                                      , a.`Claimed approved amount Less Amount Paid` as CLAIMED_APPR_AMT_LESS_AMT_PAID\
                                      , a.`Awaiting payment with No Payment Reference Number` as AWT_PYMT_WITH_NO_PYMT_REF_NUM\
                                      , a.`NetApp Fiscal FYQQ (at Payment Date)` as NET_APP_FISCAL,cast("+start_date+" as Date)ENTRY_DATE\
                                      FROM mdf_view a\
                                      WHERE a.`Partner Geo` is not null")

            mdf_union_view.show(4)
            # Checking file_record count
            stg_df_count=str(mdf_union_view.count())
            Ebi_read_write_obj.job_debugger_print("     \n Source Count : "+stg_df_count+"\n")

            # Calling Job Class method --> saveDataOracle()
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
            data_format = "JOB START DT : "+start_date+" | SCRIPT NAME : "+script_name+" | JOB : "+app_name+" | SRC COUNT : "+src_count+" | TGT COUNT : "+dest_table_count+" | JOB END DT : "+end_date+" | STATUS : %(message)s"

            Ebi_read_write_obj.create_log(data_format,log_file,logger)
            logger.info("Success")
            Ebi_read_write_obj.job_debugger_print("     \n Job "+app_name+" Succeed \n")


        except Exception as err:
            # Write expeption in spark log or console
            data_format = "JOB START DT : "+start_date+" | SCRIPT NAME : "+script_name+" | JOB : "+app_name+" | SRC COUNT : "+src_count+" | TGT COUNT : "+dest_table_count+" | JOB END DT : "+end_date+" | STATUS : %(message)s"

            Ebi_read_write_obj.create_log(data_format,log_file,logger) 

            logger.info("[Error] Failed")

            Ebi_read_write_obj.job_debugger_print("	\n Job "+app_name+" Failed\n")
            logger.error("\n __main__ SCH_job --> Exception-Traceback :: " + str(err))
            raise

if __name__ == "__main__":
        main()

