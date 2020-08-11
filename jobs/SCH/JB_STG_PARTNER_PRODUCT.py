# SCH1101.sh  --> JB_STG_PARTNER_PRODUCT.py


#**************************************************************************************************************
#
# Created by  : bibin
# Version      : 1.0
#
# Description  :
#               1. This script will load the data into 'STG_PARTNER_PRODUCT' table based on stream lookups.
#
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-10-31                    Initial creation
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
app_name = "JB_STG_PARTNER_PRODUCT"
log_filename = app_name + '_' + log_date + '.log'


# Query for loading invoice table
def query_data(db_schema,param_fiscal_year_quarter):

        query = """
        INSERT INTO """+ db_schema +""".STG_PARTNER_PRODUCT
        (FISCAL_YEAR_QTR_TEXT,HOME_GEO,COUNTRY,PARTNER_NAME,PRODUCT_FAMILY_NAME,SO_NUMBER,FLAG,USD_BOOKING_AMOUNT,USD_EXTENDED_LIST_PRICE,USD_EXTENDED_DISCOUNT)
            SELECT FISCAL_YEAR_QTR_TEXT,HOME_GEO,COUNTRY,PARTNER_NAME,PRODUCT_FAMILY_NAME,SO_NUMBER,FLAG,USD_BOOKING_AMOUNT,USD_EXTENDED_LIST_PRICE,USD_EXTENDED_DISCOUNT
            FROM
            (
            SELECT
            T1227377.FISCAL_YEAR_QTR_TEXT,
            T1225789.HOME_GEO as HOME_GEO,
            T1225789.COUNTRY as COUNTRY,
            T1225789.PARTNER_NAME as PARTNER_NAME,
            T1226301.PRODUCT_FAMILY_NAME as PRODUCT_FAMILY_NAME,
            T1227840.SO_NUMBER as SO_NUMBER,
            case  when T1227840.TIER_FLAG = '1' then 'Tier 1' when T1227840.TIER_FLAG = '2' then 'Tier 2' end  as FLAG,
            sum(T1227840.USD_BOOKING_AMOUNT) as USD_BOOKING_AMOUNT,
            sum(T1227840.USD_EXTENDED_LIST_PRICE) as USD_EXTENDED_LIST_PRICE,
            sum(T1227840.USD_EXTENDED_DISCOUNT) as USD_EXTENDED_DISCOUNT
            from
            T2_SALES_CHANNEL.PRODUCT T1226301,
            T2_SALES_CHANNEL.PARTNER_DETAILS T1225789,
            T2_SALES_CHANNEL.CALENDAR T1227377 /* CALENDAR_FISCAL */ ,
            T2_SALES_CHANNEL.BOOKINGS_TIER T1227840
            WHERE   (T1225789.PARTNER_ID = T1227840.COMMON_PARTNER_ID and T1226301.PRODUCT_KEY = T1227840.PRODUCT_AS_IS_KEY and
                     T1227377.DATE_KEY = T1227840.SOURCE_TRANSACTION_DATE_KEY and T1227377.FISCAL_YEAR_QTR_TEXT = """+ param_fiscal_year_quarter +""")
            GROUP by T1227377.FISCAL_YEAR_QTR_TEXT,T1225789.COUNTRY, T1225789.HOME_GEO, T1225789.PARTNER_NAME,
            T1226301.PRODUCT_FAMILY_NAME, T1227840.SO_NUMBER,
            case  when T1227840.TIER_FLAG = '1' then 'Tier 1' when T1227840.TIER_FLAG = '2' then 'Tier 2' end)"""

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

            param_fiscal_year_quarter = Ebi_read_write_obj.get_fiscal_year_quarter(db_schema,db_prop_key_extract)

            #SQL Query
            query = query_data(db_schema,param_fiscal_year_quarter)
            print(query)

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


