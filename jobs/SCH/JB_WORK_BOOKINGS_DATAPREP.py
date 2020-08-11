# SCH1101.sh  --> JB_WORK_BOOKINGS_DATAPREP.py


#**************************************************************************************************************
#
# Created by  : Vinay Kumbakonam
# Modified by : bibin
# Version      : 1.2
#
# Description  :
#               1. This script will load the data into 'WORK_BOOKINGS_DATAPREP' table based on stream lookups.
#
#
# Initial Creation:
#
# Date (YYYY-MM-DD)             Change Description
# -----------------             ------------------
# 2018-10-23                    Initial creation
# 2018-10-30                    bibin : Getting DB schema, db_prop_key_load from Config file, log DIR
# 2018-11-02                    bibin : Using job_debugger_print() for print any string in Job
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
app_name = "JB_WORK_BOOKINGS_DATAPREP"
log_filename = app_name + '_' + log_date + '.log'


# Query for loading invoice table
def query_data(db_schema,param_fiscal_year_quarter,param_fiscal_month):

        query = """
INSERT INTO """+ db_schema +""".WORK_BOOKINGS_DATAPREP
(USD_STANDARD_MARGIN, USD_EXTENDED_LIST_PRICE, USD_EXTENDED_DISCOUNT, USD_EXTENDED_COST, USD_EXTENDED_BURDENED_COST, USD_BURDENED_MARGIN, USD_BOOKING_AMOUNT, SO_BOOKING_AMOUNT, BE_USD_BOOKING_AMOUNT, BILL_TO_CUSTOMER_NAGP, BILL_TO_COUNTRY,
CHANNEL_TYPE, DISTRIBUTOR_COMPANY_CMAT_ID, DISTRIBUTOR_NAGP_CMAT_ID, DISTRIBUTOR_NAGP, ACCOUNT_NAGP, FISCAL_DATE, OPPORTUNITY_END_CUSTOMER, SFDC_DISTRIBUTOR_NAGP_ID, SFDC_DISTRIBUTOR, SFDC_RESELLING_NAGP_ID, SFDC_RESELLING_PARTNER, PATHWAY_TYPE,
CE_PART_CATEGORY_TYPE_CODE, PRODUCT_CATEGORY, PRODUCT_FAMILY, PRODUCT_LINE, PRODUCT_TYPE, PVR_APPROVED_DATE, PVR_APPROVAL_ROLE, PVR_STATUS, QUOTE_CREATED_DATE, QUOTE_NUMBER, SO_CURRENCY_CODE, CANCELLED_FLAG, BOOKED_DATE, PO_NUMBER, SO_NUMBER, SO_PRIMARY_RESERVE_CODE, ELA_ESA_CODE, ORDER_LINE,
ORDER_LINE_STATUS, PRODUCT_GROUPING_CODE, PRODUCT_PARENT_CODE, SHIPPED_DATE, SALES_REP_NAME, SALES_REP_NUMBER, SALES_AREA, SALES_DISTRICT, SALES_GEOGRAPHY, SALES_MULTI_AREA, SALES_REGION, SHIP_TO_CUSTOMER_ADDRESS1, SHIP_TO_CUSTOMER_ADDRESS2, SHIP_TO_CUSTOMER_ADDRESS3, SHIP_TO_CUSTOMER_ADDRESS4, SHIP_TO_CUSTOMER_CITY,
SHIP_TO_CUSTOMER_NAGP, SHIP_TO_CUSTOMER_POSTAL_CODE, SHIP_TO_COUNTRY, SOLD_TO_CUSTOMER_NAGP, SOLD_TO_COUNTRY, PARTNER_COMPANY_CMAT_ID, SOLD_TO_PARTNER_LEVEL, SOLD_TO_PARTNER_NAGP_CMAT_ID, SOLD_TO_PARTNER_NAGP, VALUE_ADD_PARTNER_NAGP_CMAT_ID, VALUE_ADD_PARTNER_NAGP,
SHIPTO_CUSTOMER_AS_IS_KEY, DISTRIBUTOR_AS_IS_KEY, BILLTO_CUSTOMER_AS_IS_KEY, SOLDTO_PARTNER_AS_IS_KEY, PRODUCT_AS_IS_KEY, SOURCE_TRANSACTION_DATE_KEY, AOO_CUSTOMER_AS_IS_KEY, SOLD_TO_PARTNER_DP, SOLD_TO_PARTNER_DP_CMAT_ID, DISTRIBUTOR_DP,
DISTRIBUTOR_DP_CMAT_ID, SALES_CHANNEL_CODE, GEOGRAPHY_SOLD_HIERARCHY_KEY, GEOGRAPHY_BILL_HIERARCHY_KEY, GEOGRAPHY_SHIP_HIERARCHY_KEY, ENTRY_DATE
)
WITH ORG_PART AS (
SELECT /*+ qb_name(q1block) */ A.* FROM DIMS.ORGANIZATION_PARTY  A
)
SELECT USD_STANDARD_MARGIN, USD_EXTENDED_LIST_PRICE, USD_EXTENDED_DISCOUNT, USD_EXTENDED_COST, USD_EXTENDED_BURDENED_COST, USD_BURDENED_MARGIN, USD_BOOKING_AMOUNT, SO_BOOKING_AMOUNT, BE_USD_BOOKING_AMOUNT, BILL_TO_CUSTOMER_NAGP, BILL_TO_COUNTRY,
CHANNEL_TYPE, DISTRIBUTOR_COMPANY_CMAT_ID, DISTRIBUTOR_NAGP_CMAT_ID, DISTRIBUTOR_NAGP, ACCOUNT_NAGP, FISCAL_DATE, OPPORTUNITY_END_CUSTOMER, SFDC_DISTRIBUTOR_NAGP_ID, SFDC_DISTRIBUTOR, SFDC_RESELLING_NAGP_ID, SFDC_RESELLING_PARTNER, PATHWAY_TYPE,
CE_PART_CATEGORY_TYPE_CODE, PRODUCT_CATEGORY, PRODUCT_FAMILY, PRODUCT_LINE, PRODUCT_TYPE, PVR_APPROVED_DATE, PVR_APPROVAL_ROLE, PVR_STATUS, QUOTE_CREATED_DATE, QUOTE_NUMBER, SO_CURRENCY_CODE, CANCELLED_FLAG, BOOKED_DATE,PO_NUMBER, SO_NUMBER, SO_PRIMARY_RESERVE_CODE, ELA_ESA_CODE, ORDER_LINE,
ORDER_LINE_STATUS, PRODUCT_GROUPING_CODE, PRODUCT_PARENT_CODE, SHIPPED_DATE, SALES_REP_NAME, SALES_REP_NUMBER, SALES_AREA, SALES_DISTRICT, SALES_GEOGRAPHY, SALES_MULTI_AREA, SALES_REGION, SHIP_TO_CUSTOMER_ADDRESS1, SHIP_TO_CUSTOMER_ADDRESS2, SHIP_TO_CUSTOMER_ADDRESS3, SHIP_TO_CUSTOMER_ADDRESS4, SHIP_TO_CUSTOMER_CITY,
SHIP_TO_CUSTOMER_NAGP, SHIP_TO_CUSTOMER_POSTAL_CODE, SHIP_TO_COUNTRY, SOLD_TO_CUSTOMER_NAGP, SOLD_TO_COUNTRY, PARTNER_COMPANY_CMAT_ID, SOLD_TO_PARTNER_LEVEL, SOLD_TO_PARTNER_NAGP_CMAT_ID, SOLD_TO_PARTNER_NAGP, VALUE_ADD_PARTNER_NAGP_CMAT_ID, VALUE_ADD_PARTNER_NAGP,
SHIPTO_CUSTOMER_AS_IS_KEY, DISTRIBUTOR_AS_IS_KEY, BILLTO_CUSTOMER_AS_IS_KEY, SOLDTO_PARTNER_AS_IS_KEY, PRODUCT_AS_IS_KEY, SOURCE_TRANSACTION_DATE_KEY, AOO_CUSTOMER_AS_IS_KEY, SOLD_TO_PARTNER_DP, SOLD_TO_PARTNER_DP_CMAT_ID, DISTRIBUTOR_DP,
DISTRIBUTOR_DP_CMAT_ID, SALES_CHANNEL_CODE, GEOGRAPHY_SOLD_HIERARCHY_KEY, GEOGRAPHY_BILL_HIERARCHY_KEY, GEOGRAPHY_SHIP_HIERARCHY_KEY, ENTRY_DATE
FROM (
select /*+ FULL(@q1block A) PARALLEL(@q1block A,6) */ sum(T762157.ME_EXT_SALE_PRICE - T762157.ME_EXT_STANDARD_COST) as USD_Standard_Margin,
                                   sum(T762157.ME_EXT_LIST_PRICE) as USD_Extended_List_Price,
                                   sum(T762157.ME_EXT_DISCOUNT_AMT) as USD_Extended_Discount,
                                   sum(T762157.ME_EXT_STANDARD_COST) as USD_Extended_Cost,
                                   sum(T762157.ME_EXT_BURDENED_COST) as USD_Extended_Burdened_Cost,
                                   sum(T762157.ME_EXT_SALE_PRICE - T762157.ME_EXT_BURDENED_COST) as USD_Burdened_Margin,
                                   sum(T762157.ME_EXT_SALE_PRICE) as USD_Booking_Amount,
                                   sum(T762157.TC_EXT_SALE_PRICE) as SO_Booking_Amount,
                                   sum(T762157.BE_EXT_SALE_PRICE) as BE_USD_Booking_Amount,
                                   T762641.PROTECTED_NAGP_NAME as Bill_To_Customer_NAGP,
                                   T764428.COUNTRY_NAME as Bill_To_Country,
                                   T760943.SALES_CHANNEL_TYPE_DESCRIPTION as Channel_Type,
                                   CAST(T762683.COMPANY_COMMON_ID AS INTEGER) as Distributor_Company_CMAT_ID,
                                   T762683.PROTECTED_COMPANY_NAME as c14,
                                   T762683.NAGP_COMMON_ID as Distributor_NAGP_CMAT_ID,
                                   T762683.PROTECTED_NAGP_NAME as Distributor_NAGP,
                                   T762895.PROTECTED_NAGP_NAME as ACCOUNT_NAGP,
                                   T763102.BK_CALENDAR_DATE as FISCAL_DATE,
                                   T761496.OPTY_END_CUSTOMER_NAME as Opportunity_End_Customer,
                                   T761496.SFDC_DISTRIBUTOR_NAGP_ID as SFDC_Distributor_NAGP_ID,
                                   TRIM(UPPER(T761496.SFDC_DISTRIBUTOR_NAME)) as SFDC_Distributor,
                                   T761496.SFDC_RESELLING_PARTNER_CMAT_ID as SFDC_Reselling_NAGP_ID,
                                   T761496.SFDC_RESELLING_PARTNER as SFDC_Reselling_Partner,
                                   T765610.BK_PATHWAY_TYPE_CODE as Pathway_Type,
                                   T761675.CE_PART_CATEGORY_TYPE_CODE as CE_Part_Category_Type_Code,
                                   T761675.PRODUCT_CATEGORY_NAME as Product_Category,
                                   T761675.PRODUCT_FAMILY_NAME as Product_Family,
                                   T761675.PRODUCT_LINE_NAME as Product_Line,
                                   T761675.PRODUCT_TYPE_NAME as Product_Type,
                                   T762071.PVR_APPROVED_DATE  as PVR_Approved_Date,
                                   T762071.PVR_LAST_ACTION_FOR_ROLE_NAME as PVR_Approval_Role,
                                   T762071.STATUS_CODE as PVR_Status,
                                   T762071.SOURCE_CREATED_DATE as Quote_Created_Date,
                                   T762071.QUOTE_NUMBER as Quote_Number,
                                   T763622.BK_ISO_CURRENCY_CODE as SO_CURRENCY_CODE,
                                   T705584.ATTRIBUTE_CODE_DESCRIPTION as Cancelled_Flag,
                                   T762251.BOOKED_DATE as Booked_Date,
                                   T762247.SALES_CHANNEL_GROUPING_NAME as c38,
                                   T762251.PURCHASE_ORDER_NUMBER as PO_Number,
                                   T762251.BK_SALES_ORDER_NUMBER as SO_Number,
                                   T765517.PRIMARY_RESERVE_CODE as SO_Primary_Reserve_Code,
                                   T762251.ORDER_SUB_TYPE_CODE as c42,
                                   T762251.SALES_ORDER_TYPE_CODE as c43,
                                   T760552.ATTRIBUTE_CODE_DESCRIPTION as c45,
                                   T760559.ATTRIBUTE_CODE_DESCRIPTION as ELA_ESA_Code ,
                                   T762300.BK_SALES_ORDER_LINE_NUMBER as Order_Line,
                                   T762300.LINE_STATUS_CODE as Order_Line_Status,
                                   T770644.PRODUCT_GROUPING_CODE as Product_Grouping_Code,
                                   T770644.PRODUCT_PARENT_CODE as Product_Parent_Code,
                                   T762300.SHIPPED_DATE as Shipped_Date,
                                   T762438.SALES_REP_NAME as Sales_Rep_Name,
                                   T762438.BK_SALES_REP_NUMBER as Sales_Rep_Number,
                                   T762518.AREA_DESCRIPTION as Sales_Area,
                                   T762518.DISTRICT_DESCRIPTION as Sales_District,
                                   T762518.GEOGRAPHY_DESCRIPTION as Sales_Geography,
                                   T762518.MULTI_AREA_DESCRIPTION as Sales_Multi_Area,
                                   T762518.REGION_DESCRIPTION as Sales_Region,
                                   case  when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.LINE_1_ADDRESS else 'Protected Party' end  as Ship_To_Customer_Address1,
                                   case  when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.LINE_2_ADDRESS else 'Protected Party' end  as Ship_To_Customer_Address2,
                                   case  when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.LINE_3_ADDRESS else 'Protected Party' end  as Ship_To_Customer_Address3,
                                   case  when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.LINE_4_ADDRESS else 'Protected Party' end  as Ship_To_Customer_Address4,
                                   case  when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.CITY_NAME else 'Protected Party' end  as Ship_To_Customer_City,
                                   T762725.ISO_COUNTRY_CODE as C66,
                                   T762725.PROTECTED_NAGP_NAME as Ship_To_Customer_NAGP,
                                   case  when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.POSTAL_CODE else 'Protected Party' end  as Ship_To_Customer_Postal_Code,
                                   T763529.COUNTRY_NAME as Ship_To_Country,
                                   T762769.PROTECTED_NAGP_NAME as Sold_To_Customer_NAGP,
                                   T765592.COUNTRY_NAME as Sold_To_Country,
                                   T762811.COMPANY_COMMON_ID as Partner_Company_CMAT_ID,
                                   T762811.PROTECTED_COMPANY_NAME as c70,
                                   T779975.PARTNER_LEVEL_NAME as Sold_To_Partner_Level,
                                   T762811.NAGP_COMMON_ID as Sold_To_Partner_NAGP_CMAT_ID,
                                   T762811.PROTECTED_NAGP_NAME as Sold_To_Partner_NAGP,
                                   T762853.NAGP_COMMON_ID as Value_Add_Partner_NAGP_CMAT_ID,
                                   T762853.PROTECTED_NAGP_NAME as Value_Add_Partner_NAGP,
                                   T762895.NAGP_NAME as c76,
                                   T762683.NAGP_NAME as c77,
                                   case  when T774496.USER_ACCESS_CODE = 'ALLOWED' then T762769.NAGP_NAME else 'Protected Party' end  as c78,
                                   T762811.NAGP_NAME as c79,
                                   T762853.NAGP_NAME as c80,
                                   T764428.COUNTRY_CODE as c81,
                                   T762518.DISTRICT_CODE as c82,
                                T762157. SHIPTO_CUSTOMER_AS_IS_KEY,
                                T762157. DISTRIBUTOR_AS_IS_KEY,
                                T762157.  BILLTO_CUSTOMER_AS_IS_KEY,
                                T762157.  SOLDTO_PARTNER_AS_IS_KEY,
                                T762157. PRODUCT_AS_IS_KEY,
                                T762157.SOLDTO_CUSTOMER_AS_IS_KEY,
                                T762157.SOURCE_TRANSACTION_DATE_KEY ,
                                T762157.AOO_CUSTOMER_AS_IS_KEY,
                                T762811.PROTECTED_DOMESTIC_PARENT_NAME as SOLD_TO_PARTNER_DP,
                                T762811.DP_COMMON_ID as Sold_To_Partner_DP_CMAT_ID,
                                T762683.PROTECTED_DOMESTIC_PARENT_NAME as DISTRIBUTOR_DP,
                                T762683.DP_COMMON_ID as Distributor_DP_CMAT_ID,
                                T762247.BK_SALES_CHANNEL_CODE AS Sales_Channel_Code ,
                                                                T765592.GEOGRAPHY_HIERARCHY_KEY as GEOGRAPHY_SOLD_HIERARCHY_KEY ,
                                                                T764428.GEOGRAPHY_HIERARCHY_KEY as GEOGRAPHY_BILL_HIERARCHY_KEY,
                                                                T763529.GEOGRAPHY_HIERARCHY_KEY as GEOGRAPHY_SHIP_HIERARCHY_KEY,
                                                                sysdate as ENTRY_DATE
                                                                   from
                                   DIMS.PARTNER_BUSINESS_MODEL T779975 /* PARTNER_BUSINESS_MODEL_STP */ ,
                                   DIMS.CALENDAR T763102 /* CALENDAR_FISCAL */ ,
                                   DIMS.SALES_TERR_HIER_AS_IS T762518,
                                   ORG_PART T762683 /* ORG_PARTY_DISTRIBUTOR */ ,
                                   ORG_PART T762853 /* ORG_PARTY_VALUE_ADD_PART */ ,
                                   ORG_PART T762811 /* ORG_PARTY_SOLD_TO_PART */ ,
                                   ORG_PART T762895 /* ORG_PARTY_AOO_CUST */ ,
                                   DIMS.DEAL_PATHWAY T765610,
                                   DIMS.SALES_CHANNEL T762247,
                                   DIMS.QUOTE T762071,
                                   DIMS.SALES_PARTICIPANT T762438,
                                   DIMS.SALES_ORDER_LINE T762300,
                                   DIMS.SALES_ORDER T762251 left outer join DIMS.SALES_ORDER_EXT T765517 on T762251.SALES_ORDER_KEY = T765517.SALES_ORDER_KEY,
                                   DIMS.ISO_CURRENCY T763622 /* ISO_CURRENCY_MISC1 */ ,
                                   DIMS.OPPORTUNITY T761496,
                                   DIMS.ENTERPRISE_LIST_OF_VALUES T705584 /* LOV_CORP_FLAG */ ,
                                   ORG_PART T762769 /* ORG_PARTY_SOLD_TO_CUST */ ,
                                   DIMS.CUSTOMER_GEOGRAPHY_HIERARCHY T765592 /* CUSTOMER_GEO_SOLD_TO_HIERARCHY */ ,
                                   ORG_PART T762641 /* ORG_PARTY_BILL_TO_CUST */ ,
                                   DIMS.CUSTOMER_GEOGRAPHY_HIERARCHY T764428 /* CUSTOMER_GEO_BILL_TO_HIERARCHY */ ,
                                   ORG_PART T762725 /* ORG_PARTY_SHIP_TO_CUST */ ,
                                   DIMS.CUSTOMER_GEOGRAPHY_HIERARCHY T763529 /* CUSTOMER_GEO_SHIP_TO_HIERARCHY */ ,
                                   DIMS.PRODUCT T761675,
                                   FACTS.SALES_BOOKING T762157,
                                   DIMS.CHANNEL_TYPE T760943,
                                   DATASEC.USER_ACCESS_LIST T774496 /* USER_ACCESS_SOLD_TO_CUST */ ,
                                   DIMS.SALES_ORDER_LINE_DD T769152,
                                   DIMS.DERIVED_PRODUCT T770644,
                                   DIMS.ENTERPRISE_LIST_OF_VALUES T760552 /* LOV_ORD_CONFIG_ELA_ESA_FLAG */ ,
                                   DIMS.ENTERPRISE_LIST_OF_VALUES T760559 /* LOV_ORD_LINE_ELA_ESA_CODE */
                              where  ( T762157.SOURCE_TRANSACTION_DATE_KEY = T763102.DATE_KEY
and T762157.DISTRIBUTOR_AS_IS_KEY = T762683.ORGANIZATION_PARTY_KEY
and T762157.VALUE_ADD_PARTNER_AS_IS_KEY = T762853.ORGANIZATION_PARTY_KEY
and T762157.SOLDTO_PARTNER_AS_IS_KEY = T762811.ORGANIZATION_PARTY_KEY
and T762157.AOO_CUSTOMER_AS_IS_KEY = T762895.ORGANIZATION_PARTY_KEY
and T762157.PATHWAY_KEY = T765610.PATHWAY_KEY and T762157.SALES_CHANNEL_CODE_KEY = T762247.SALES_CHANNEL_CODE_KEY
and T762071.QUOTE_KEY = T762157.QUOTE_KEY and T762157.SALES_REP_AS_IS_KEY = T762438.SALES_PARTICIPANT_KEY
and T762157.SLED_TERRITORY_AS_IS_KEY = T762518.TERRITORY_KEY and T762157.TC_TRX_CURR_CODE_KEY = T763622.ISO_CURRENCY_KEY
and T761496.OPPORTUNITY_KEY = T762157.OPPORTUNITY_KEY and T705584.ATTRIBUTE_CODE = 'CORP FLAG'
and T705584.ATTRIBUTE_CODE_VALUE = T762157.CORP_FLAG
and T762157.SALES_ORDER_LINE_KEY = T762300.SALES_ORDER_LINE_KEY
and T762157.SOLDTO_PRTNR_MODEL_AS_IS_KEY = T779975.PARTNER_BUSINESS_MODEL_KEY
and T762157.BILLTO_CUSTOMER_AS_IS_KEY = T762641.ORGANIZATION_PARTY_KEY
and T762300.SALES_ORDER_LINE_KEY = T769152.SALES_ORDER_LINE_KEY
and T762157.SHIPTO_CUSTOMER_AS_IS_KEY = T762725.ORGANIZATION_PARTY_KEY
and T762641.GEOGRAPHY_HIERARCHY_KEY = T764428.GEOGRAPHY_HIERARCHY_KEY
and T761675.PRODUCT_KEY = T762157.PRODUCT_AS_IS_KEY and T762157.SALES_ORDER_KEY = T762251.SALES_ORDER_KEY
and T760943.SALES_CHANNEL_TYPE_KEY = T762251.SALES_CHANNEL_TYPE_KEY
and T762725.GEOGRAPHY_HIERARCHY_KEY = T763529.GEOGRAPHY_HIERARCHY_KEY
and T760552.ATTRIBUTE_CODE = 'ORD_CONFIG_ELA_ESA_FLAG'
and T760552.ATTRIBUTE_CODE_VALUE = T769152.ORD_CONFIG_ELA_ESA_FLAG
and T760559.ATTRIBUTE_CODE = 'ORD_LINE_ELA_ESA_CODE'
and T760559.ATTRIBUTE_CODE_VALUE = T769152.ORD_LINE_ELA_ESA_CODE
and T762157.SOLDTO_CUSTOMER_AS_IS_KEY = T762769.ORGANIZATION_PARTY_KEY
and T762769.GEOGRAPHY_HIERARCHY_KEY = T765592.GEOGRAPHY_HIERARCHY_KEY
and T762769.PROTECTION_SUBCLASS_KEY = T774496.PROTECTION_SUBCLASS_KEY
and T705584.ATTRIBUTE_CODE_DESCRIPTION = 'Y'
and T763102.FISCAL_YEAR_QTR_TEXT IN ("""+ param_fiscal_year_quarter +""")
and T763102.FISCAL_QTR_MONTH_TEXT IN ("""+ param_fiscal_month +""")
and T769152.DERIVED_PRODUCT_KEY = T770644.DERIVED_PRODUCT_KEY
and T774496.USER_ID = 'adari'

)
group by T705584.ATTRIBUTE_CODE_DESCRIPTION, T760552.ATTRIBUTE_CODE_DESCRIPTION, T760559.ATTRIBUTE_CODE_DESCRIPTION, T760943.SALES_CHANNEL_TYPE_DESCRIPTION, T761496.OPTY_END_CUSTOMER_NAME, T761496.SFDC_DISTRIBUTOR_NAGP_ID, T761496.SFDC_DISTRIBUTOR_NAME, T761496.SFDC_RESELLING_PARTNER, T761496.SFDC_RESELLING_PARTNER_CMAT_ID, T761675.CE_PART_CATEGORY_TYPE_CODE, T761675.PRODUCT_CATEGORY_NAME, T761675.PRODUCT_FAMILY_NAME, T761675.PRODUCT_LINE_NAME, T761675.PRODUCT_TYPE_NAME, T762071.QUOTE_NUMBER, T762071.SOURCE_CREATED_DATE, T762071.STATUS_CODE, T762071.PVR_LAST_ACTION_FOR_ROLE_NAME, T762071.PVR_APPROVED_DATE, T762247.SALES_CHANNEL_GROUPING_NAME, T762251.BK_SALES_ORDER_NUMBER, T762251.BOOKED_DATE, T762251.ORDER_SUB_TYPE_CODE, T762251.PURCHASE_ORDER_NUMBER, T762251.SALES_ORDER_TYPE_CODE, T762300.BK_SALES_ORDER_LINE_NUMBER, T762300.LINE_STATUS_CODE, T762300.SHIPPED_DATE, T762438.BK_SALES_REP_NUMBER, T762438.SALES_REP_NAME, T762518.AREA_CODE, T762518.AREA_DESCRIPTION, T762518.DISTRICT_CODE, T762518.DISTRICT_DESCRIPTION, T762518.GEOGRAPHY_CODE, T762518.GEOGRAPHY_DESCRIPTION, T762518.MULTI_AREA_CODE, T762518.MULTI_AREA_DESCRIPTION, T762518.REGION_CODE, T762518.REGION_DESCRIPTION, T762641.PROTECTED_NAGP_NAME, T762683.COMPANY_COMMON_ID, T762683.NAGP_NAME, T762683.NAGP_COMMON_ID, T762683.PROTECTED_COMPANY_NAME, T762683.PROTECTED_NAGP_NAME, T762725.ISO_COUNTRY_CODE, T762725.PROTECTED_NAGP_NAME, T762769.PROTECTED_NAGP_NAME, T762811.COMPANY_COMMON_ID, T762811.NAGP_NAME, T762811.NAGP_COMMON_ID, T762811.PROTECTED_COMPANY_NAME, T762811.PROTECTED_NAGP_NAME, T762853.NAGP_NAME, T762853.NAGP_COMMON_ID, T762853.PROTECTED_NAGP_NAME, T762895.NAGP_NAME, T762895.PROTECTED_NAGP_NAME, T763102.BK_CALENDAR_DATE, T763529.COUNTRY_NAME, T763622.BK_ISO_CURRENCY_CODE, T764428.COUNTRY_CODE, T764428.COUNTRY_NAME, T765517.PRIMARY_RESERVE_CODE, T765592.COUNTRY_NAME, T765610.BK_PATHWAY_TYPE_CODE, T770644.PRODUCT_PARENT_CODE, T770644.PRODUCT_GROUPING_CODE, T779975.PARTNER_LEVEL_NAME, case  when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.CITY_NAME else 'Protected Party' end , case  when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.LINE_1_ADDRESS else 'Protected Party' end , case  when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.LINE_2_ADDRESS else 'Protected Party' end , case  when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.LINE_3_ADDRESS else 'Protected Party' end , case  when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.LINE_4_ADDRESS else 'Protected Party' end , case  when T762725.PROTECTION_SUBCLASS_KEY = 1000 then T762725.POSTAL_CODE else 'Protected Party' end ,
                              case  when T774496.USER_ACCESS_CODE = 'ALLOWED' then T762769.NAGP_NAME else 'Protected Party' end ,
                                T762157. SHIPTO_CUSTOMER_AS_IS_KEY,
                                T762157. DISTRIBUTOR_AS_IS_KEY,
                                T762157.  BILLTO_CUSTOMER_AS_IS_KEY,
                                T762157.  SOLDTO_PARTNER_AS_IS_KEY,
                                T762157. PRODUCT_AS_IS_KEY,
                                T762157.SOLDTO_CUSTOMER_AS_IS_KEY,
                                T762157.SOURCE_TRANSACTION_DATE_KEY ,
                                T762157.AOO_CUSTOMER_AS_IS_KEY,
                                T762811.PROTECTED_DOMESTIC_PARENT_NAME,
                                T762811.DP_COMMON_ID,
                                T762683.PROTECTED_DOMESTIC_PARENT_NAME,
                                T762683.DP_COMMON_ID,
                                T762247.BK_SALES_CHANNEL_CODE ,
                                T765592.GEOGRAPHY_HIERARCHY_KEY  ,
                                T764428.GEOGRAPHY_HIERARCHY_KEY ,
                                T763529.GEOGRAPHY_HIERARCHY_KEY)
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
            param_fiscal_month = Ebi_read_write_obj.get_fiscal_month(db_schema,db_prop_key_extract)

            #SQL Query
            query = query_data(db_schema,param_fiscal_year_quarter,param_fiscal_month)
						
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


