#SCH1006.py  --> sch_work_invoice_dataprep.py

#**************************************************************************************************************
#
# Programmer   : bibin
# Version      : 1.0
#
# Description  :
#
#
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-10-17                    Initial creation
#
#
#**************************************************************************************************************


# Importing required Lib

import logging
import sys
from time import gmtime, strftime
import cx_Oracle
import py4j
import pyspark

from dependencies.spark import start_spark
from dependencies.EbiReadWrite import EbiReadWrite


# Spark logging
logger = logging.getLogger(__name__)

# Date Formats
start_date = "'"+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"'"
log_date =strftime("%Y%m%d", gmtime())

# Job Naming Details
script_name = "SCH1100.sh"
app_name = 'JB_WORK_INVOICE_DATAPREP'
log_filename = app_name + '_' + log_date + '.log'

# Query for Extract data
def query_data(db_schema,Param_fiscal_month,Param_fiscal_year_quarter):

        query = """INSERT INTO """+ db_schema +""".work_invoice_dataprep (
        USD_STANDARD_MARGIN, USD_EXTENDED_LIST_PRICE, USD_EXTENDED_COST, USD_EXTENDED_BURDENED_COST, USD_BURDENED_MARGIN, USD_BILLING_AMOUNT, SO_BILLING_AMOUNT, BILLING_QTY, AR_INVOICE_DATE, AR_INVOICE_NUMBER, MANUAL_INVOICE_FLAG, BILL_TO_CUSTOMER_NAGP, BILL_TO_COUNTRY, SO_CURRENCY_CODE, DISTRIBUTOR_DP, DISTRIBUTOR_NAGP, REVENUE_RECOGNIZED_DATE, CE_PART_CATEGORY_TYPE_CODE, PRODUCT_CATEGORY, PRODUCT_FAMILY, PRODUCT_LINE, PRODUCT_TYPE, BOOKED_DATE, SALES_CHANNEL_CODE, SO_NUMBER, SALES_REP_NAME, SALES_REP_NUMBER, SALES_AREA, SALES_DISTRICT, SALES_GEO, SALES_MULTI_AREA, SALES_REGION, SHIP_TO_CUSTOMER_ADDR_LINE_1, SHIP_TO_CUSTOMER_ADDR_LINE_2, SHIP_TO_CUSTOMER_ADDR_LINE_3, SHIP_TO_CUSTOMER_ADDR_LINE_4, SHIP_TO_CUSTOMER_CITY, SHIP_TO_CUSTOMER_COUNTRY_CODE, SHIP_TO_CUSTOMER_NAGP, SHIP_TO_CUSTOMER_POSTAL_CODE, SHIP_TO_COUNTRY, SOLD_TO_CUSTOMER_NAGP, SOLD_TO_COUNTRY, SOLD_TO_PARTNER_DP, SOLD_TO_PARTNER_DP_CMAT_ID, SOLD_TO_PARTNER_LEVEL, VALUE_ADD_PARTNER_NAGP, SOLD_TO_PARTNER_NAGP, DISTRIBUTOR_AS_IS_KEY, SOLDTO_PARTNER_AS_IS_KEY, BILLTO_CUSTOMER_AS_IS_KEY, SHIPTO_CUSTOMER_AS_IS_KEY, PRODUCT_AS_IS_KEY, SOURCE_TRANSACTION_DATE_KEY, AOO_CUSTOMER_AS_IS_KEY, DISTRIBUTOR_DP_CMAT_ID, GEOGRAPHY_SOLD_HIERARCHY_KEY, GEOGRAPHY_BILL_HIERARCHY_KEY, GEOGRAPHY_SHIP_HIERARCHY_KEY, USD_RECOGNIZABLE_REVENUE, ENTRY_DATE)
        WITH ORG_PART AS (
        SELECT /*+ qb_name(q1block) */

        ORGANIZATION_PARTY_KEY,
            PROTECTED_NAGP_NAME,
            NAGP_NAME,
            PROTECTED_COMPANY_NAME,
            PROTECTED_DOMESTIC_PARENT_NAME,
            DP_COMMON_ID,
            PROTECTION_SUBCLASS_KEY,
            LINE_1_ADDRESS,
            LINE_2_ADDRESS,
            LINE_3_ADDRESS,
            LINE_4_ADDRESS,
            CITY_NAME,
            ISO_COUNTRY_CODE,
            POSTAL_CODE,
            GEOGRAPHY_HIERARCHY_KEY

                FROM DIMS.ORGANIZATION_PARTY  A
                )
                select USD_STANDARD_MARGIN, USD_EXTENDED_LIST_PRICE, USD_EXTENDED_COST, USD_EXTENDED_BURDENED_COST, USD_BURDENED_MARGIN, USD_BILLING_AMOUNT, SO_BILLING_AMOUNT, BILLING_QTY, AR_INVOICE_DATE, AR_INVOICE_NUMBER, MANUAL_INVOICE_FLAG, BILL_TO_CUSTOMER_NAGP, BILL_TO_COUNTRY, SO_CURRENCY_CODE, DISTRIBUTOR_DP, DISTRIBUTOR_NAGP, REVENUE_RECOGNIZED_DATE, CE_PART_CATEGORY_TYPE_CODE, PRODUCT_CATEGORY, PRODUCT_FAMILY, PRODUCT_LINE, PRODUCT_TYPE, BOOKED_DATE, SALES_CHANNEL_CODE, SO_NUMBER, SALES_REP_NAME, SALES_REP_NUMBER, SALES_AREA, SALES_DISTRICT, SALES_GEO, SALES_MULTI_AREA, SALES_REGION, SHIP_TO_CUSTOMER_ADDR_LINE_1, SHIP_TO_CUSTOMER_ADDR_LINE_2, SHIP_TO_CUSTOMER_ADDR_LINE_3, SHIP_TO_CUSTOMER_ADDR_LINE_4, SHIP_TO_CUSTOMER_CITY, SHIP_TO_CUSTOMER_COUNTRY_CODE, SHIP_TO_CUSTOMER_NAGP, SHIP_TO_CUSTOMER_POSTAL_CODE, SHIP_TO_COUNTRY, SOLD_TO_CUSTOMER_NAGP, SOLD_TO_COUNTRY, SOLD_TO_PARTNER_DP, SOLD_TO_PARTNER_DP_CMAT_ID, SOLD_TO_PARTNER_LEVEL, VALUE_ADD_PARTNER_NAGP, SOLD_TO_PARTNER_NAGP, DISTRIBUTOR_AS_IS_KEY, SOLDTO_PARTNER_AS_IS_KEY, BILLTO_CUSTOMER_AS_IS_KEY, SHIPTO_CUSTOMER_AS_IS_KEY, PRODUCT_AS_IS_KEY, SOURCE_TRANSACTION_DATE_KEY, AOO_CUSTOMER_AS_IS_KEY, DISTRIBUTOR_DP_CMAT_ID, GEOGRAPHY_SOLD_HIERARCHY_KEY, GEOGRAPHY_BILL_HIERARCHY_KEY, GEOGRAPHY_SHIP_HIERARCHY_KEY, USD_RECOGNIZABLE_REVENUE, ENTRY_DATE
                from (
                select /*+ FULL(@q1block A) PARALLEL(@q1block A,6) */ sum(T761254.ME_EXT_SALE_PRICE - T761254.ME_EXT_STANDARD_COST) as USD_Standard_Margin,
                                                   sum(T761254.ME_EXT_LIST_PRICE) as USD_Extended_List_Price,
                                                   sum(T761254.ME_EXT_STANDARD_COST) as USD_Extended_Cost,
                                                   sum(T761254.ME_EXT_BURDENED_COST) as USD_Extended_Burdened_Cost,
                                                   sum(T761254.ME_EXT_SALE_PRICE - T761254.ME_EXT_BURDENED_COST) as USD_Burdened_Margin,
                                                   sum(T761254.ME_EXT_SALE_PRICE) as USD_Billing_Amount,
                                                   sum(T761254.TC_EXT_SALE_PRICE) as SO_Billing_Amount,
                                                   sum(T761254.INVOICED_QUANTITY) as Billing_Qty,
                                                   T760775.AR_INVOICE_DATE as AR_Invoice_Date,
                                                   T760775.AR_INVOICE_NUMBER as AR_Invoice_Number,
                                                   case  when T760775.AR_INVOICE_CREATE_SOURCE_NAME = 'RAXTRX' then 'N' else 'Y' end  as Manual_Invoice_Flag,
                                                   T762641.PROTECTED_NAGP_NAME as Bill_To_Customer_NAGP,
                                                   T764428.COUNTRY_NAME as Bill_To_Country,
                                                   T763622.BK_ISO_CURRENCY_CODE as SO_Currency_Code,
                                                   TRIM(UPPER(T762683.PROTECTED_COMPANY_NAME)) as Distributor_DP,
                                                   TRIM(UPPER(T762683.PROTECTED_NAGP_NAME)) as Distributor_NAGP,
                                                   T763102.BK_CALENDAR_DATE as Revenue_Recognized_Date,
                                                                                                                                                   T761675.CE_PART_CATEGORY_TYPE_CODE as CE_Part_Category_Type_Code,
                                                   T761675.PRODUCT_CATEGORY_NAME as Product_Category,
                                                   T761675.PRODUCT_FAMILY_NAME as Product_Family,
                                                   T761675.PRODUCT_LINE_NAME as Product_Line,
                                                   T761675.PRODUCT_TYPE_NAME as Product_Type,
                                                   T762251.BOOKED_DATE as Booked_Date,
                                                   T762247.BK_SALES_CHANNEL_CODE as Sales_Channel_Code,
                                                   T762247.SALES_CHANNEL_GROUPING_NAME as c25,
                                                   T762251.BK_SALES_ORDER_NUMBER as SO_Number,
                                                   T762438.SALES_REP_NAME as Sales_Rep_Name,
                                                   T762438.BK_SALES_REP_NUMBER as Sales_Rep_Number,
                                                   T762518.AREA_DESCRIPTION as Sales_Area,
                                                   T762518.DISTRICT_DESCRIPTION as Sales_District,
                                                   T762518.GEOGRAPHY_DESCRIPTION as Sales_Geo,
                                                   T762518.MULTI_AREA_DESCRIPTION as Sales_Multi_Area,
                                                   T762518.REGION_DESCRIPTION as Sales_Region,
                                                   case  when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.LINE_1_ADDRESS else 'Protected Party' end  as SHIP_TO_CUSTOMER_ADDR_LINE_1,
                                                   case  when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.LINE_2_ADDRESS else 'Protected Party' end  as SHIP_TO_CUSTOMER_ADDR_LINE_2,
                                                   case  when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.LINE_3_ADDRESS else 'Protected Party' end  as SHIP_TO_CUSTOMER_ADDR_LINE_3,
                                                   case  when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.LINE_4_ADDRESS else 'Protected Party' end  as SHIP_TO_CUSTOMER_ADDR_LINE_4,
                                                   case  when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.CITY_NAME else 'Protected Party' end  as Ship_To_Customer_City,
                                                   T762725.ISO_COUNTRY_CODE as Ship_To_Customer_Country_Code,
                                                   T762725.PROTECTED_NAGP_NAME as Ship_To_Customer_NAGP,
                                                   case  when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.POSTAL_CODE else 'Protected Party' end  as Ship_To_Customer_Postal_Code,
                                                   T763529.COUNTRY_NAME as Ship_To_Country,
                                                   T762769.PROTECTED_NAGP_NAME as Sold_To_Customer_NAGP,
                                                   T765592.COUNTRY_NAME as Sold_To_Country,
                                                   trim(upper(T762811.PROTECTED_COMPANY_NAME)) as Sold_To_Partner_DP,
                                                   T762811.DP_COMMON_ID as Sold_To_Partner_DP_CMAT_ID,
                                                   T762811.PROTECTED_DOMESTIC_PARENT_NAME as C45,--CHANGED FROM Sold_To_Partner_DP TO C45
                                                   T779975.PARTNER_LEVEL_NAME as Sold_To_Partner_Level,
                                                                                                                                                                                 case  when T774496.USER_ACCESS_CODE = 'ALLOWED' then T762769.NAGP_NAME else 'Protected Party' end  as c49,--changed from Sold_To_Partner_NAGP to c49
                                                   T762853.PROTECTED_NAGP_NAME as Value_Add_Partner_NAGP,
                                                   T762683.NAGP_NAME as c51,
                                                   T762811.PROTECTED_NAGP_NAME as Sold_To_Partner_NAGP,
                                                  T762811.NAGP_NAME as c53,
                                                  T762853.NAGP_NAME as c54,
                                                  T764428.COUNTRY_CODE as c55,
                                                  T762518.DISTRICT_CODE as c56,
                                                  T761254.DISTRIBUTOR_AS_IS_KEY ,
                                                  T761254.SOLDTO_PARTNER_AS_IS_KEY,
                                                  T761254.SOLDTO_CUSTOMER_AS_IS_KEY,
                                                  T761254.BILLTO_CUSTOMER_AS_IS_KEY,
                                                  T761254.SHIPTO_CUSTOMER_AS_IS_KEY,
                                                  T761254.PRODUCT_AS_IS_KEY,
                                                  T761254.SOURCE_TRANSACTION_DATE_KEY ,
                                                  T761254.AOO_CUSTOMER_AS_IS_KEY ,
                                                  TRIM(UPPER(T762683.PROTECTED_DOMESTIC_PARENT_NAME)) as c15,---changed from Distributor_DP to c15
                                                  T762683.DP_COMMON_ID as Distributor_DP_CMAT_ID,
                                                  T765592.GEOGRAPHY_HIERARCHY_KEY as GEOGRAPHY_SOLD_HIERARCHY_KEY ,
                                                  T764428.GEOGRAPHY_HIERARCHY_KEY as GEOGRAPHY_BILL_HIERARCHY_KEY,
                                                  T763529.GEOGRAPHY_HIERARCHY_KEY as GEOGRAPHY_SHIP_HIERARCHY_KEY,
                                                  0 as USD_Recognizable_Revenue,
                                                  sysdate as Entry_Date
                                              FROM
                                                   DIMS.PARTNER_BUSINESS_MODEL T779975 /* PARTNER_BUSINESS_MODEL_STP */ ,
                                                   DIMS.CALENDAR T763102 /* CALENDAR_FISCAL */ ,
                                                   DIMS.SALES_TERR_HIER_AS_IS T762518,
                                                   DIMS.SALES_PARTICIPANT T770344 /* SALES_PARTICIPANT_MISC2 */ ,
                                                   ORG_PART  T762683 /* DIMS.ORGANIZATION_PARTY , ORG_PARTY_DISTRIBUTOR */ ,
                                                   ORG_PART  T762853 /* DIMS.ORGANIZATION_PARTY, ORG_PARTY_VALUE_ADD_PART */ ,
                                                   ORG_PART T762811 /* DIMS.ORGANIZATION_PARTY,ORG_PARTY_SOLD_TO_PART */ ,
                                                   DIMS.SALES_CHANNEL T762247,
                                                   DIMS.SALES_PARTICIPANT T762438,
                                                   DIMS.SALES_ORDER T762251,
                                                   DIMS.ISO_CURRENCY T763622 /* ISO_CURRENCY_MISC1 */ ,
                                                   DIMS.AR_INVOICE T760775,
                                                   DIMS.ENTERPRISE_LIST_OF_VALUES T705584 /* LOV_CORP_FLAG */ ,
                                                   ORG_PART  T762769 /* DIMS.ORGANIZATION_PARTY, ORG_PARTY_SOLD_TO_CUST */ ,
                                                   DIMS.CUSTOMER_GEOGRAPHY_HIERARCHY T765592 /* CUSTOMER_GEO_SOLD_TO_HIERARCHY */ ,
                                                   ORG_PART  T762641 /* DIMS.ORGANIZATION_PARTY, ORG_PARTY_BILL_TO_CUST */ ,
                                                   DIMS.CUSTOMER_GEOGRAPHY_HIERARCHY T764428 /* CUSTOMER_GEO_BILL_TO_HIERARCHY */ ,
                                                   DIMS.PRODUCT T761675,
                                                   ORG_PART T762725 /* DIMS.ORGANIZATION_PARTY, ORG_PARTY_SHIP_TO_CUST */ ,
                                                   DIMS.CUSTOMER_GEOGRAPHY_HIERARCHY T763529 /* CUSTOMER_GEO_SHIP_TO_HIERARCHY */ ,
                                                   FACTS.INVOICE T761254,
                                                   DATASEC.USER_ACCESS_LIST T774496 /* USER_ACCESS_SOLD_TO_CUST */ ,
                                                   ORG_PART  T762895 /* DIMS.ORGANIZATION_PARTY, ORG_PARTY_AOO_CUST */
                                              where  ( T761254.SOURCE_TRANSACTION_DATE_KEY = T763102.DATE_KEY
                and T761254.SALES_REP_AS_IS_KEY = T770344.SALES_PARTICIPANT_KEY
                and T761254.DISTRIBUTOR_AS_IS_KEY = T762683.ORGANIZATION_PARTY_KEY
                and T761254.VALUE_ADD_PARTNER_AS_IS_KEY = T762853.ORGANIZATION_PARTY_KEY
                and T761254.SOLDTO_PARTNER_AS_IS_KEY = T762811.ORGANIZATION_PARTY_KEY
                and T761254.SALES_CHANNEL_CODE_KEY = T762247.SALES_CHANNEL_CODE_KEY
                and T761254.SALES_REP_AS_IS_KEY = T762438.SALES_PARTICIPANT_KEY
                and T761254.SALES_ORDER_KEY = T762251.SALES_ORDER_KEY
                and T761254.TC_TRX_CURR_CODE_KEY = T763622.ISO_CURRENCY_KEY
                and T760775.AR_INVOICE_KEY = T761254.AR_INVOICE_KEY
                and T761254.SOLDTO_PRTNR_MODEL_AS_IS_KEY = T779975.PARTNER_BUSINESS_MODEL_KEY
                and T705584.ATTRIBUTE_CODE = 'CORP FLAG'
                and T705584.ATTRIBUTE_CODE_VALUE = T761254.CORP_FLAG
                and T761254.BILLTO_CUSTOMER_AS_IS_KEY = T762641.ORGANIZATION_PARTY_KEY
                and T762518.TERRITORY_KEY = T770344.TERRITORY_KEY
                and T761254.PRODUCT_AS_IS_KEY = T761675.PRODUCT_KEY
                and T761254.SHIPTO_CUSTOMER_AS_IS_KEY = T762725.ORGANIZATION_PARTY_KEY
                and T762641.GEOGRAPHY_HIERARCHY_KEY = T764428.GEOGRAPHY_HIERARCHY_KEY
                and T705584.ATTRIBUTE_CODE_DESCRIPTION = 'Y'
                and T762725.GEOGRAPHY_HIERARCHY_KEY = T763529.GEOGRAPHY_HIERARCHY_KEY
                and T763102.FISCAL_YEAR_QTR_TEXT in (""" + Param_fiscal_year_quarter + """)
                and T763102.FISCAL_QTR_MONTH_TEXT IN (""" + Param_fiscal_month + """)
                and T761254.SOLDTO_CUSTOMER_AS_IS_KEY = T762769.ORGANIZATION_PARTY_KEY
                and T762769.GEOGRAPHY_HIERARCHY_KEY = T765592.GEOGRAPHY_HIERARCHY_KEY
                and T762769.PROTECTION_SUBCLASS_KEY = T774496.PROTECTION_SUBCLASS_KEY
                and T774496.USER_ID = 'adari'
                and T761254.AOO_CUSTOMER_AS_IS_KEY = T762895.ORGANIZATION_PARTY_KEY)
                group by
                T760775.AR_INVOICE_DATE, T760775.AR_INVOICE_NUMBER, T761675.CE_PART_CATEGORY_TYPE_CODE, T761675.PRODUCT_CATEGORY_NAME, T761675.PRODUCT_FAMILY_NAME, T761675.PRODUCT_LINE_NAME, T761675.PRODUCT_TYPE_NAME, T762247.SALES_CHANNEL_GROUPING_NAME, T762247.BK_SALES_CHANNEL_CODE, T762251.BK_SALES_ORDER_NUMBER, T762251.BOOKED_DATE, T762438.BK_SALES_REP_NUMBER, T762438.SALES_REP_NAME, T762518.AREA_CODE, T762518.AREA_DESCRIPTION, T762518.DISTRICT_CODE, T762518.DISTRICT_DESCRIPTION, T762518.GEOGRAPHY_CODE, T762518.GEOGRAPHY_DESCRIPTION, T762518.MULTI_AREA_CODE, T762518.MULTI_AREA_DESCRIPTION, T762518.REGION_CODE, T762518.REGION_DESCRIPTION, T762641.PROTECTED_NAGP_NAME, T762683.NAGP_NAME, T762683.PROTECTED_COMPANY_NAME, T762683.PROTECTED_NAGP_NAME, T762725.ISO_COUNTRY_CODE, T762725.PROTECTED_NAGP_NAME, T762769.PROTECTED_NAGP_NAME, T762811.NAGP_NAME, T762811.PROTECTED_COMPANY_NAME, T762811.PROTECTED_NAGP_NAME, T762811.DP_COMMON_ID, T762811.PROTECTED_DOMESTIC_PARENT_NAME, T762853.NAGP_NAME, T762853.PROTECTED_NAGP_NAME, T763102.BK_CALENDAR_DATE, T763529.COUNTRY_NAME, T763622.BK_ISO_CURRENCY_CODE, T764428.COUNTRY_CODE, T764428.COUNTRY_NAME, T765592.COUNTRY_NAME, T779975.PARTNER_LEVEL_NAME, case  when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.CITY_NAME else 'Protected Party' end , case  when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.LINE_1_ADDRESS else 'Protected Party' end , case  when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.LINE_2_ADDRESS else 'Protected Party' end , case  when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.LINE_3_ADDRESS else 'Protected Party' end , case  when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.LINE_4_ADDRESS else 'Protected Party' end , case  when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.POSTAL_CODE else 'Protected Party' end , case  when T774496.USER_ACCESS_CODE = 'ALLOWED' then T762769.NAGP_NAME else 'Protected Party' end ,
                case  when T760775.AR_INVOICE_CREATE_SOURCE_NAME = 'RAXTRX' then 'N' else 'Y' end ,
                T761254.DISTRIBUTOR_AS_IS_KEY ,
                T761254.SOLDTO_PARTNER_AS_IS_KEY,
                T761254.SOLDTO_CUSTOMER_AS_IS_KEY,
                T761254.BILLTO_CUSTOMER_AS_IS_KEY,
                T761254.SHIPTO_CUSTOMER_AS_IS_KEY,
                T761254.PRODUCT_AS_IS_KEY,
                T761254.SOURCE_TRANSACTION_DATE_KEY ,
                T761254.AOO_CUSTOMER_AS_IS_KEY ,
                T762683.PROTECTED_DOMESTIC_PARENT_NAME,
                T762683.DP_COMMON_ID,
                T765592.GEOGRAPHY_HIERARCHY_KEY  ,
                T764428.GEOGRAPHY_HIERARCHY_KEY ,
                T763529.GEOGRAPHY_HIERARCHY_KEY ) """

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
            spark, config = start_spark(
                app_name=app_name)

            # Create class Object
            Ebi_read_write_obj = EbiReadWrite(app_name,spark,config,logger)

            #
            log_file = config['LOG_DIR_NAME'] + "/" + log_filename
            # DB prop Key of Source DB
            db_prop_key_extract =  config['DB_PROP_KEY_EXTRACT']
            db_prop_key_load = config['DB_PROP_KEY_LOAD']

            db_schema = config['DB_SCHEMA']

            param_fiscal_year_quarter = Ebi_read_write_obj.get_fiscal_year_quarter(db_schema,db_prop_key_extract)

            param_fiscal_month = Ebi_read_write_obj.get_fiscal_month(db_schema,db_prop_key_extract)

            #SQL Query
            query = query_data(db_schema,param_fiscal_month,param_fiscal_year_quarter)
            
            # Calling Job Class method --> get_target_data_update()
            Ebi_read_write_obj.get_target_data_update(query,db_prop_key_load)

            end_date="'"+strftime("%Y-%m-%d %H:%M:%S", gmtime())+"'"

            data_format = "JOB START DT : "+start_date+" | SCRIPT NAME : "+script_name+" | JOB : "+app_name+" | SRC COUNT : "+src_count+" | TGT COUNT : "+dest_count+" | JOB END DT : "+end_date+" | STATUS : %(message)s"

            Ebi_read_write_obj.create_log(data_format,log_file,logger)

            logger.info("Success")
            Ebi_read_write_obj.job_debugger_print("	\n  __main__ " + app_name +" --> Job "+app_name+" Succeed \n")

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
