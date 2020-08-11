#!/bin/bash

#***********************************************************************************************************
# Developed by : UST GLOBAL
# Version      : 1.0
# Programmer   : Vinay Kumbakonam
# Description  :
#	1. This bash scipt is mainly for 'INSTALLBASE_BOOKINGS' jobs.
#	2. And checks the run time arguments, if not receives means takes the default parameters.
#       
#
# Initial Creation:
#
# Date (YYYY-MM-DD)		Change Description
# -----------------		------------------
# 2018-10-30			Initial creation
#
#
#**************************************************************************************************************

# Env variables
path="$1"
truncate_stg_installedbase='JB_STG_INSTALLEDBASE_TRUNCATE'
load_stg_installedbase='JB_STG_INSTALLEDBASE'
truncate_installedbase_partition='JB_INSTALLEDBASE_PARTITION_TRUNCATE'
load_installedbase='JB_INSTALLEDBASE'
truncate_install_booking_lkp='JB_INSTALL_BOOKING_LKUP_TRUNCATE'
load_install_booking_lkp='JB_INSTALL_BOOKING_LKUP'
truncate_bookings_fiscal_qtr_so='JB_BOOKINGS_FISCAL_QTR_SO_TRUNCATE'
load_bookings_fiscal_qtr_so='JB_BOOKINGS_FISCAL_QTR_SO'
truncate_stg_installedbase_bookings='JB_STG_INSTALLEDBASE_BOOKINGS_TRUNCATE'
load_stg_installedbase_bookings='JB_STG_INSTALLEDBASE_BOOKINGS'
truncate_installedbase_bookings='JB_INSTALLEDBASE_BOOKINGS_TRUNCATE'
load_installedbase_bookings='JB_INSTALLEDBASE_BOOKINGS'

#===============================================================================================================

echo -e "\n This Script Will Trigger Following PySpark Jobs,
         1. truncate_stg_installedbase
         2. load_stg_installedbase
         3. truncate_installedbase_partition
         4. load_installedbase
         5. truncate_install_booking_lkp
         6. load_install_booking_lkp
         7. truncate_bookings_fiscal_qtr_so
         8. load_bookings_fiscal_qtr_so
         9. truncate_stg_installedbase_bookings
         10. load_stg_installedbase_bookings
         11. truncate_installedbase_bookings
         12. load_installedbase_bookings
"

# This functions is to run PySpark jobs in sequence manner.
spark_run_func(){
for job_name in ${truncate_stg_installedbase} ${load_stg_installedbase} ${truncate_installedbase_partition} ${load_installedbase} ${truncate_install_booking_lkp} ${load_install_booking_lkp} ${truncate_bookings_fiscal_qtr_so} ${load_bookings_fiscal_qtr_so} ${truncate_stg_installedbase_bookings} ${load_stg_installedbase_bookings} ${truncate_installedbase_bookings} ${load_installedbase_bookings} 
do
echo -e "\n	Running Job Name : $job_name	\n"
#job="spark-submit --packages com.crealytics:spark-excel_2.11:0.9.17 $job_name.py $path ${conf_file}"
job="spark-submit --py-files packages.zip --files configs/etl_config.json ${job_name}.py $path configs/etl_config.json"
eval $job #|& tee
echo $job
if [ $? -eq 0 ];then
echo -e "\n	PySpark Job $job_name Succeed	\n"
else echo -e "\n	[ERROR] : PySpark Job $job_name Failed	\n"
exit 1
fi
done
}

# This function is to check run time arguments
run_file_func(){
if [ $# -gt 0 ];then
conf_file=$path/db.properties
echo -e "	Running with ${conf_file} file "
export conf_file
spark_run_func
else
echo "	Run time argumets are not passed "
conf_file=/home/spark/v_kumbakonam/v_prod/db.properties
path=`pwd`
echo -e "\n	Running with ${conf_file} file "
spark_run_func
fi
}

# Calling functions
run_file_func $path

# Exit
exit 0
