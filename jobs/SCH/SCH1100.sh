#!/bin/bash

#***********************************************************************************************************
# Developed by : UST GLOBAL
# Version      : 1.0
# Programmer   : Vinay Kumbakonam
# Description  :
#	1. This bash scipt is mainly for 'BOOKINGS' jobs.
#	2. And checks the run time arguments, if not receives means takes the default parameters.
#       
#
# Initial Creation:
#
# Date (YYYY-MM-DD)		Change Description
# -----------------		------------------
# 2018-10-27			Initial creation
#
#
#**************************************************************************************************************

# Env variables
path="$1"
truncate_datarep='JB_WORK_INVOICE_DATAPREP_TRUNCATE'
load_datarep='JB_WORK_INVOICE_DATAPREP'
truncate_stg_invoice='JB_STG_INVOICE_TRUNCATE'
load_stg_invoice='JB_STG_INVOICE'
truncate_wrk_invoice='JB_WORK_INVOICE_TRUNCATE'
load_wrk_invoice='JB_WORK_INVOICE'
update_invoice='JB_UPD_INVOICE'
truncate_invoice_partition='JB_INVOICE_PARTITION_TRUNCATE'
load_edw_invoice='JB_WORK_TO_EDW_INVOICE'
truncate_invoice_tier_partition='JB_INVOICE_TIER_PARTITION_TRUNCATE'
load_invoice_tier='JB_INVOICE_TIER_LOAD'


echo -e "\n This Script Will Trigger Following PySpark Jobs,
		1. truncate_datarep
		2. load_datarep
		3. truncate_stg_invoice
		4. load_stg_invoice
		5. truncate_wrk_invoice
		6. load_wrk_invoice
		7. update_invoice
		8. truncate_invoice_partition
		9. load_edw_invoice
		10. truncate_invoice_tier_partition
		11. load_invoice_tier\n
"

# This functions is to run PySpark jobs in sequence manner.
spark_run_func(){
for job_name in ${truncate_datarep} ${load_datarep} ${truncate_stg_invoice} ${load_stg_invoice} ${truncate_wrk_invoice} ${load_wrk_invoice} ${update_invoice} ${truncate_invoice_partition} ${load_edw_invoice} ${truncate_invoice_tier_partition} ${load_invoice_tier}
do
echo -e "\n	Running Job Name : $job_name	\n"
job="spark-submit --packages com.crealytics:spark-excel_2.11:0.9.17 $job_name.py $path ${conf_file}"
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
