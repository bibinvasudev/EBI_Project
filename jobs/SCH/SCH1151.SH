#!/bin/bash

#*******************************************************************************************************************************
# Developed by : UST GLOBAL
# Version      : 1.0
# Programmer   : Vinay Kumbakonam
# Description  :
#	1. This bash scipt is mainly for 'PARTNER_PRODUCT' jobs.
#       2. And mainly checks the exit status of the 'JB_EVALUATE_ROWS_NUMBER' jobs and makes decisions for the remaining jobs.
#	3. Also checks the run time arguments, if not receives means takes the default parameters.
#       
#
# Initial Creation:
#
# Date (YYYY-MM-DD)		Change Description
# -----------------		------------------
# 2018-11-02			Initial creation
#
#
#*********************************************************************************************************************************

# Env variables
path="$1"
evaluate_rows_count="JB_EVALUATE_ROWS_NUMBER"
truncate_stg_debooking_pos='JB_STG_DEBOOKING_POS_TRUNCATE'
load_stg_debooking_pos='JB_STG_DEBOOKING_POS'
truncate_work_debooking='JB_WORK_DEBOOKING_TRUNCATE'
load_work_debooking='JB_WORK_DEBOOKING'
update_upd_bookings='JB_UPD_BOOKINGS'
truncate_stg_partner_product='JB_STG_PARTNER_PRODUCT_TRUNCATE'
load_stg_partner_product='JB_STG_PARTNER_PRODUCT'
truncate_partner_product='JB_PARTNER_PRODUCT_TRUNCATE'
load_partner_product='JB_PARTNER_PRODUCT'

#===============================================================================================================

echo -e "\n	This Script Initially Will Trigger ${evaluate_rows_count}.py"

# This functions is to run PySpark jobs in sequence manner.
spark_run_func(){
for job_name in ${evaluate_rows_count} ${truncate_stg_debooking_pos} ${load_stg_debooking_pos} ${truncate_work_debooking} ${load_work_debooking} ${update_upd_bookings} ${truncate_stg_partner_product} ${load_stg_partner_product} ${truncate_partner_product} ${load_partner_product} 
do
echo -e "        Running Job Name : ${evaluate_rows_count}    \n"
condition_job="spark-submit --py-files packages.zip --files configs/etl_config.json ${evaluate_rows_count}.py $path configs/etl_config.json"
eval ${condition_job} #|& tee
if [ $? -eq 0 ];then
echo -e "\n	${evaluate_rows_count}'s exit status is '1', So Triggering The Following Jobs
		1. ${truncate_stg_debooking_pos}
		2. ${load_stg_debooking_pos}
		3. ${truncate_work_debooking}
		4. ${load_work_debooking}
		5. ${update_upd_bookings}
		6. ${truncate_stg_partner_product}
		7. ${load_stg_partner_product}
		8. ${truncate_partner_product}
		9. ${load_partner_product}
"
echo -e "\n	Running Job Name : $job_name    \n"
job="spark-submit --py-files packages.zip --files configs/etl_config.json ${job_name} $path configs/etl_config.json" 
eval $job #|& tee
else
echo "\n	${evaluate_rows_count}'s Is Not Equals To '1', So No Jobs Will Trigger"
fi

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
