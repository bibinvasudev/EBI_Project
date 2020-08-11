# SCH2007.sh  --> JB_PVR_APPROVAL_INCR.py

# **************************************************************************************************************
#
# Created by  : Bibin Vasudevan
# Version      : 1.0
#
# Description  :
# 1. This script will data into 'PVR_APPROVAL' table.
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
script_name = "SCH2007.SH"
app_name = "JB_PVR_APPROVAL_INCR"
log_filename = app_name + '_' + log_date + '.log'


# Query for loading invoice table
def query_data(db_schema):
    query = """ INSERT INTO """ + db_schema + """.PVR_APPROVAL (
                USD_EXTENDED_SALE_PRICE,
                QUOTE_DISC_PER,
                DISC_100_REASON,
                PVR_LAST_APPROVER_NAME,
                PVR_LAST_APPROVER_ROLE,
                APPROVAL_DATE,
                PVR_USED_FLAG,
                QUOTE_BOOKED_FLAG,
                QUOTE_COPIED_FLAG,
                QUOTE_CREATE_DATE,
                QUOTE_NUMBER,
                AUTO_APPROVED_FLAG,
                EXCEPTION_PVR_FLAG,
                PVR_APPROVAL_STATUS,
                APPROVAL_NAME,
                APPROVAL_ROLE,
                SALES_REP_NUMBER,
                PVR_PARTNER_REASON,
                PVR_REASON,
                ENTRY_DATE,
                QUOTE_APPR_DT,
                SALES_GEO,
                SALES_AREA,
                SALES_REP,
                FISCAL_QTR_APPR_DT,
                FISCAL_QTR_QUOTE_DT,
                DATE_KEY,
                MONTH_KEY,
                QUOTE_APPR_FISCAL_QTR
              )
            SELECT
              USD_EXTENDED_SALE_PRICE,
              QUOTE_DISC_PER,
              DISC_100_REASON,
              PVR_LAST_APPROVER_NAME,
              PVR_LAST_APPROVER_ROLE,
              PVR_APPROVED_DATE AS APPROVAL_DATE,
              PVR_USED_FLAG,
              QUOTE_BOOKED_FLAG,
              QUOTE_COPIED_FLAG,
              SOURCE_CREATED_DATE AS QUOTE_CREATE_DATE,
              QUOTE_NUMBER,
              AUTO_APPROVED_FLAG,
              EXCEPTION_PVR_FLAG,
              PVR_APPROVAL_STATUS,
              APPROVAL_NAME,
              APPROVAL_ROLE,
              BK_SALES_REP_NUMBER AS SALES_REP_NUMBER,
              PVR_PARTNER_REASON,
              PVR_REASON,
              ENTRY_DATE,
              QUOTE_APPR_DT,
              SALES_GEO,
              SALES_AREA,
              SALES_REP_NAME1 AS SALES_REP,
              FISCAL_QTR_APPR_DT,
              FISCAL_QTR_QUOTE_DT,
              DATE_KEY,
              MONTH_KEY,
              QUOTE_APPR_FISCAL_QTR
            FROM
              (
                WITH DRIVER_TBL_VW AS (
                  select
                    sum(T723661.SALE_USD_PRICE) as USD_EXTENDED_SALE_PRICE,
                    sum(T723661.QUANTITY) as QUANTITY,
                    avg(T723661.DISCOUNT_PERCENT) as QUOTE_DISC_PER,
                    T723181.HUNDRED_PCT_DISC_REASON_NAME as DISC_100_REASON,
                    T723181.PVR_LAST_ACTION_FOR_NAME as PVR_Last_Approver_Name,
                    T723181.PVR_LAST_ACTION_FOR_ROLE_NAME as PVR_Last_Approver_Role,
                    TRUNC(T723181.PVR_APPROVED_DATE) as PVR_APPROVED_DATE,
                    T723181.PVR_USED_FLAG as PVR_USED_FLAG,
                    T723181.QUOTE_BOOKED_FLAG as QUOTE_BOOKED_FLAG,
                    T723181.QUOTE_COPIED_FLAG as QUOTE_COPIED_FLAG,
                    TRUNC(T723181.SOURCE_CREATED_DATE) as SOURCE_CREATED_DATE,
                    T723181.QUOTE_NUMBER as QUOTE_NUMBER,
                    T717730.AUTO_APPROVED_FLAG as AUTO_APPROVED_FLAG,
                    T723181.EXCEPTION_PVR_FLAG as EXCEPTION_PVR_FLAG,
                    T717730.PVR_APPROVAL_STATUS as PVR_APPROVAL_STATUS,
                    T717730.PVR_LAST_APPROVER_NAME as Approval_Name,
                    T717730.PVR_LAST_APPROVER_ROLE as Approval_Role,
                    T723928.SALES_REP_NAME as SALES_REP_NAME,
                    T723928.BK_SALES_REP_NUMBER as BK_SALES_REP_NUMBER,
                    replace(T723181.PVR_REASON_FROM_PARTNER_TEXT, '" ', '') as PVR_PARTNER_REASON,
                    replace(TRIM(T723181.PVR_REASON_TEXT), ' "', '') as PVR_REASON,
                    sysdate as ENTRY_DATE,
                    CASE WHEN T717730.PVR_APPROVAL_STATUS = 'PVR Pre-Approved' THEN TRUNC(T723181.SOURCE_CREATED_DATE) ELSE TRUNC(T723181.PVR_APPROVED_DATE) END QUOTE_APPR_DT
                  from
                    DIMS.QUOTE_PVR_VERSION T717730,
                    DIMS.QUOTE T723181,
                    DIMS.SALES_PARTICIPANT T723928,
                    DIMS.CALENDAR T724513
                    /* CALENDAR_FISCAL */,
                    FACTS.QUOTE_DETAIL T723661
                  where
                    (
                      T717730.QUOTE_KEY = T723181.QUOTE_KEY
                      and T723181.QUOTE_KEY = T723661.QUOTE_KEY
                      and T723661.PRIMARY_SALES_REP_AS_IS_KEY = T723928.SALES_PARTICIPANT_KEY
                      and T723661.QUOTE_CREATE_DATE_KEY = T724513.DATE_KEY
                      and T724513.FISCAL_YEAR_QTR_TEXT in ('2019Q2')
                    )
                    AND T724513.FISCAL_QTR_MONTH_TEXT IN ('Q2M1', 'Q2M2')
                  GROUP BY
                    T723181.HUNDRED_PCT_DISC_REASON_NAME,
                    T723181.PVR_LAST_ACTION_FOR_NAME,
                    T723181.PVR_LAST_ACTION_FOR_ROLE_NAME,
                    TRUNC(T723181.PVR_APPROVED_DATE),
                    T723181.PVR_USED_FLAG,
                    T723181.QUOTE_BOOKED_FLAG,
                    T723181.QUOTE_COPIED_FLAG,
                    TRUNC(T723181.SOURCE_CREATED_DATE),
                    T723181.QUOTE_NUMBER,
                    T717730.AUTO_APPROVED_FLAG,
                    T723181.EXCEPTION_PVR_FLAG,
                    T717730.PVR_APPROVAL_STATUS,
                    T717730.PVR_LAST_APPROVER_NAME,
                    T717730.PVR_LAST_APPROVER_ROLE,
                    T723928.SALES_REP_NAME,
                    T723928.BK_SALES_REP_NUMBER,
                    replace(T723181.PVR_REASON_FROM_PARTNER_TEXT, '" ', ''),
                    replace(TRIM(T723181.PVR_REASON_TEXT), ' "', ''),
                    sysdate,
                    CASE WHEN T717730.PVR_APPROVAL_STATUS = 'PVR Pre-Approved' THEN TRUNC(T723181.SOURCE_CREATED_DATE) ELSE TRUNC(T723181.PVR_APPROVED_DATE) END
                )
                SELECT
                  Z.*,
                  X.SALES_GEOGRAPHY AS SALES_GEO,
                  X.SALES_AREA,
                  X.SALES_REP_NAME AS SALES_REP_NAME1,
                  Y.CALENDAR_YEAR_QTR_TEXT AS FISCAL_QTR_APPR_DT,
                  P.CALENDAR_YEAR_QTR_TEXT AS FISCAL_QTR_QUOTE_DT,
                  P.DATE_KEY,
                  P.FISCAL_MONTH_KEY AS MONTH_KEY,
                  Q.FISCAL_YEAR_QTR_TEXT AS QUOTE_APPR_FISCAL_QTR
                FROM
                  DRIVER_TBL_VW Z
                  LEFT OUTER JOIN (
                    select
                      *
                    from
                      T2_SALES_CHANNEL.sales_hierarchy
                  ) X ON (Z.BK_SALES_REP_NUMBER = X.SALES_REP_NUMBER)
                  LEFT OUTER JOIN (
                    SELECT
                      TRUNC(BK_CALENDAR_DATE) BK_CALENDAR_DATE,
                      FISCAL_YEAR_QTR_TEXT AS CALENDAR_YEAR_QTR_TEXT
                    FROM
                      T2_SALES_CHANNEL.CALENDAR
                    where
                      BK_CALENDAR_DATE is not null
                  ) Y ON (Z.PVR_APPROVED_DATE = Y.BK_CALENDAR_DATE)
                  LEFT OUTER JOIN (
                    SELECT
                      DATE_KEY,
                      FISCAL_MONTH_KEY,
                      TRUNC(BK_CALENDAR_DATE) BK_CALENDAR_DATE,
                      A.FISCAL_YEAR_QTR_TEXT AS CALENDAR_YEAR_QTR_TEXT
                    FROM
                      T2_SALES_CHANNEL.CALENDAR A,
                      T2_SALES_CHANNEL.FISCAL_MONTH B
                    WHERE
                      b.BK_FISCAL_MONTH_START_DATE = a.FISCAL_MONTH_START_DATE
                      AND B.FISCAL_MONTH_END_DATE = A.FISCAL_MONTH_END_DATE
                    ORDER BY
                      DATE_KEY
                  ) P ON (Z.SOURCE_CREATED_DATE = P.BK_CALENDAR_DATE)
                  LEFT OUTER JOIN (
                    SELECT
                      TRUNC(BK_CALENDAR_DATE) BK_CALENDAR_DATE,
                      FISCAL_YEAR_QTR_TEXT
                    FROM
                      T2_SALES_CHANNEL.CALENDAR
                  ) Q ON (Z.QUOTE_APPR_DT = Q.BK_CALENDAR_DATE)
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
