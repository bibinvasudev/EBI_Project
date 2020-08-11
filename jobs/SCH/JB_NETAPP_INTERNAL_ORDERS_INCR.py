# SCH2002.sh  --> JB_NETAPP_INTERNAL_ORDERS_INCR.py

# **************************************************************************************************************
#
# Created by  : Bibin Vasudevan
# Version      : 1.0
#
# Description  :
# 1. This script will data into 'NETAPP_INTERNAL_ORDERS' table.
#
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-11-05                    Initial creation
#
# **************************************************************************************************************


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
log_date = strftime("%Y%m%d", gmtime())

# Job Naming Details
script_name = "SCH2001.SH"
app_name = "JB_NETAPP_INTERNAL_ORDERS_INCR"
log_filename = app_name + '_' + log_date + '.log'


# Query for loading invoice table
def query_data(db_schema):
    query = """ INSERT INTO """ + db_schema + """ .NETAPP_INTERNAL_ORDERS (
                BOOKED_DATE,
                SALES_REP_NAME,
                SHIPTO_CUSTOMER_AS_IS_KEY,
                BILLTO_CUSTOMER_AS_IS_KEY,
                DATE_KEY,
                SALES_ORDER_NUMBER,
                USD_BOOKING_AMOUNT,
                SALES_GEOGRAPHY,
                SALES_AREA,
                SALES_REGION,
                SALES_REP_NUMBER,
                SHIP_TO_CUSTOMER_CITY,
                SHIP_TO_CUSTOMER_COUNTY,
                SHIP_TO_CUSTOMER_NAGP,
                SHIP_TO_CUSTOMER_SITE,
                SALES_ORDER_SUB_TYPE_CODE,
                MONTH_KEY,
                SYSTEM_QTY,
                BOOKING_QTY,
                PRODUCT_AS_IS_KEY,
                USD_EXTENDED_LIST_PRICE,
                ENTRY_DATE,
                SOLDTO_CUSTOMER_AS_IS_KEY,
                BOOKED_QTR,
                FISCAL_DATE,
                FISCAL_QTR
              )
            select
              BOOKED_DATE,
              SALES_REP_NAME,
              SHIPTO_CUSTOMER_AS_IS_KEY,
              BILLTO_CUSTOMER_AS_IS_KEY,
              DATE_KEY,
              SALES_ORDER_NUMBER,
              USD_BOOKING_AMOUNT,
              SALES_GEOGRAPHY,
              SALES_AREA,
              SALES_REGION,
              SALES_REP_NUMBER,
              SHIP_TO_CUSTOMER_CITY,
              SHIP_TO_CUSTOMER_COUNTY,
              SHIP_TO_CUSTOMER_NAGP,
              SHIP_TO_CUSTOMER_SITE,
              SALES_ORDER_SUB_TYPE_CODE,
              MONTH_KEY,
              SYSTEM_QTY,
              BOOKING_QTY,
              PRODUCT_AS_IS_KEY,
              USD_EXTENDED_LIST_PRICE,
              ENTRY_DATE,
              SOLDTO_CUSTOMER_AS_IS_KEY,
              BOOKED_QTR,
              FISCAL_DATE,
              FISCAL_QTR
            from
              (
                WITH DRIVER_TBL_VW AS (
                  select
                    sum(T762157.ME_EXT_LIST_PRICE) as USD_EXTENDED_LIST_PRICE,
                    sum(T762157.ME_EXT_SALE_PRICE) as USD_BOOKING_AMOUNT,
                    sum(T762157.SYSTEM_QUANTITY) as SYSTEM_QTY,
                    sum(T762157.QUANTITY) as BOOKING_QTY,
                    trunc(T762251.BOOKED_DATE) as BOOKED_DATE,
                    T762251.BK_SALES_ORDER_NUMBER AS SALES_ORDER_NUMBER,
                    T762251.ORDER_SUB_TYPE_CODE AS SALES_ORDER_SUB_TYPE_CODE,
                    T762251.SALES_ORDER_TYPE_CODE,
                    T762438.SALES_REP_NAME,
                    T762438.BK_SALES_REP_NUMBER AS SALES_REP_NUMBER,
                    T762518.AREA_DESCRIPTION AS SALES_AREA,
                    T762518.GEOGRAPHY_DESCRIPTION AS SALES_GEOGRAPHY,
                    T762518.REGION_DESCRIPTION AS SALES_REGION,
                    case when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.CITY_NAME else 'Protected Party' end as SHIP_TO_CUSTOMER_CITY,
                    case when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.COUNTY_NAME else 'Protected Party' end as SHIP_TO_CUSTOMER_COUNTY,
                    T762725.PROTECTED_NAGP_NAME AS SHIP_TO_CUSTOMER_NAGP,
                    T762725.PROTECTED_SITE_NAME AS SHIP_TO_CUSTOMER_SITE,
                    T762518.REGION_CODE,
                    T762157.SHIPTO_CUSTOMER_AS_IS_KEY,
                    T762157.BILLTO_CUSTOMER_AS_IS_KEY,
                    T762157.SOLDTO_CUSTOMER_AS_IS_KEY,
                    T762157.PRODUCT_AS_IS_KEY,
                    sysdate as ENTRY_DATE,
                    TRUNC(T763102.BK_CALENDAR_DATE) AS FISCAL_DATE
                  from
                    DIMS.CALENDAR T763102
                    /* CALENDAR_FISCAL */,
                    DIMS.SALES_TERR_HIERAR_AS_IS_MV T762518,
                    DIMS.ORGANIZATION_PARTY T762725
                    /* ORG_PARTY_SHIP_TO_CUST */,
                    DIMS.SALES_PARTICIPANT T762438,
                    DIMS.ORGANIZATION_PARTY T762641,
                    DIMS.SALES_ORDER T762251,
                    FACTS.SALES_BOOKING T762157
                  where
                    (
                      T762157.SHIPTO_CUSTOMER_AS_IS_KEY = T762725.ORGANIZATION_PARTY_KEY
                      and T762157.SALES_REP_AS_IS_KEY = T762438.SALES_PARTICIPANT_KEY
                      and T762157.BILLTO_CUSTOMER_AS_IS_KEY = T762641.ORGANIZATION_PARTY_KEY
                      and T762157.SALES_ORDER_KEY = T762251.SALES_ORDER_KEY
                      and T762157.SOURCE_TRANSACTION_DATE_KEY = T763102.DATE_KEY
                      and T762157.SLED_TERRITORY_AS_IS_KEY = T762518.TERRITORY_KEY
                      and T763102.FISCAL_YEAR_QTR_TEXT in ('2019Q2')
                      and T763102.FISCAL_QTR_MONTH_TEXT IN ('Q2M1', 'Q2M2')
                      and T762251.SALES_ORDER_TYPE_CODE like '%DNV%'
                    )
                  group by
                    T762251.BK_SALES_ORDER_NUMBER,
                    T762251.BOOKED_DATE,
                    T762251.ORDER_SUB_TYPE_CODE,
                    T762251.SALES_ORDER_TYPE_CODE,
                    T762438.BK_SALES_REP_NUMBER,
                    T762438.SALES_REP_NAME,
                    T762518.AREA_CODE,
                    T762518.AREA_DESCRIPTION,
                    T762518.GEOGRAPHY_CODE,
                    T762518.GEOGRAPHY_DESCRIPTION,
                    T762518.REGION_CODE,
                    T762518.REGION_DESCRIPTION,
                    T762725.PROTECTED_NAGP_NAME,
                    T762725.PROTECTED_SITE_NAME,
                    case when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.CITY_NAME else 'Protected Party' end,
                    case when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.COUNTY_NAME else 'Protected Party' end,
                    T762157.SHIPTO_CUSTOMER_AS_IS_KEY,
                    T762157.BILLTO_CUSTOMER_AS_IS_KEY,
                    SOLDTO_CUSTOMER_AS_IS_KEY,
                    T762157.PRODUCT_AS_IS_KEY,
                    TRUNC(T763102.BK_CALENDAR_DATE)
                )
                SELECT
                  Z.*,
                  X.DATE_KEY,
                  X.FISCAL_MONTH_KEY AS MONTH_KEY,
                  X.FISCAL_YEAR_QTR_TEXT AS BOOKED_QTR,
                  Y.FISCAL_YEAR_QTR_TEXT AS FISCAL_QTR
                FROM
                  DRIVER_TBL_VW Z
                  LEFT OUTER JOIN (
                    SELECT
                      DATE_KEY,
                      FISCAL_MONTH_KEY,
                      TRUNC(BK_CALENDAR_DATE) BK_CALENDAR_DATE,
                      A.FISCAL_YEAR_QTR_TEXT
                    FROM
                      T2_SALES_CHANNEL.CALENDAR a,
                      T2_SALES_CHANNEL.FISCAL_MONTH b
                    WHERE
                      b.BK_FISCAL_MONTH_START_DATE = a.FISCAL_MONTH_START_DATE
                      AND b.FISCAL_MONTH_END_DATE = a.FISCAL_MONTH_END_DATE
                    ORDER BY
                      DATE_KEY
                  ) X ON (Z.BOOKED_DATE = X.BK_CALENDAR_DATE)
                  LEFT OUTER JOIN (
                    SELECT
                      TRUNC(BK_CALENDAR_DATE) BK_CALENDAR_DATE,
                      FISCAL_YEAR_QTR_TEXT
                    FROM
                      T2_SALES_CHANNEL.CALENDAR
                  ) Y ON (Z.FISCAL_DATE = Y.BK_CALENDAR_DATE)
              )
            """

    return query


#  Main method
def main():
        try:
            src_count = '0'
            dest_count = '0'
            """Main ETL script definition.
            :return: None
            """

            # start Spark application and get Spark session, logger and config
            spark, config = start_spark(app_name=app_name)

            # Create class Object
            ebi_read_write_obj = EbiReadWrite(app_name, spark, config, logger)

            # DB prop Key of Source DB
            db_prop_key_load = config['DB_PROP_KEY_LOAD']
            db_schema = config['DB_SCHEMA']
            log_file = config['LOG_DIR_NAME'] + "/" + log_filename

            query = query_data(db_schema)

            # Calling Job Class method --> get_target_data_update()
            ebi_read_write_obj.get_target_data_update(query, db_prop_key_load)

            end_date = "'" + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"

            data_format = "JOB START DT : " + start_date + " | SCRIPT NAME : " + script_name + " | JOB : " + app_name + \
                          " | SRC COUNT : " + src_count + " | TGT COUNT : " + dest_count + " | JOB END DT : "\
                          + end_date + " | STATUS : %(message)s"

            ebi_read_write_obj.create_log(data_format, log_file, logger)

            logger.info("Success")
            ebi_read_write_obj.job_debugger_print("     \n  __main__ " + app_name + " --> Job " + app_name + " Succeed \n")

        except Exception as err:
            # Write expeption in spark log or console
            end_date = "'" + strftime("%Y-%m-%d %H:%M:%S", gmtime()) + "'"
            data_format = "JOB START DT : "+start_date+" | SCRIPT NAME : "+script_name+" | JOB : "+app_name\
                          + " | SRC COUNT : "+src_count+" | TGT COUNT : "+dest_count+" | JOB END DT : "+end_date\
                          + " | STATUS : %(message)s"
            ebi_read_write_obj.create_log(data_format, log_file, logger)
            logger.info("[Error] Failed")
            ebi_read_write_obj.job_debugger_print("     \n Job " + app_name + " Failed\n")
            logger.error("\n __main__ " + app_name + " --> Exception-Traceback :: " + str(err))
            raise


# Entry point for script
if __name__ == "__main__":
    # Calling main() method
    main()
