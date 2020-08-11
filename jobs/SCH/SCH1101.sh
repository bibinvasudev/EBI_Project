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
truncate_datarep='JB_WORK_BOOKINGS_DATAPREP_TRUNCATE'
load_datarep='JB_WORK_BOOKINGS_DATAPREP'
truncate_stg_bookings='JB_STG_BOOKINGS_TRUNCATE'
load_stg_bookings='JB_STG_BOOKINGS'
truncate_wrk_bookings='JB_WORK_BOOKINGS_TRUNCATE'
load_wrk_bookings='JB_WORK_BOOKINGS'
truncate_day_flag_temp='JB_BOOKINGS_DAY_FLAG_TEMP_TRUNCATE'
load_day_flag_temp='JB_BOOKINGS_DAY_FLAG_TEMP'
truncate_day_flag='JB_DAY_FLAG_BOOKINGS_TRUNCATE'
load_day_flag='JB_DAY_FLAG_BOOKINGS'
update_wrk_bookings='JB_UPD_WORK_BOOKINGS'
truncate_bookings_rej='JB_BOOKINGS_REJ_TRUNCATE'
truncate_bookings_partition='JB_BOOKINGS_PARTITION_TRUNCATE'
load_wrk_to_bookings='JB_WORK_TO_BOOKINGS'
load_wrk_to_booking_rej='JB_WORK_TO_BOOKINGS_REJ'
truncate_booking_tier_partition='JB_BOOKINGS_TIER_PARTITION_TRUNCATE'
load_booking_tier='JB_BOOKINGS_TIER'


echo -e "\n This Script Will Trigger Following PySpark Jobs,
         1. truncate_datarep.py
         2. load_datarep.py
         3. truncate_stg_bookings.py
         4. load_stg_bookings.py
         5. truncate_wrk_bookings.py
         6. load_wrk_bookings.py
         7. truncate_day_flag_temp.py
         8. load_day_flag_temp.py
         9. truncate_day_flag.py
         10. load_day_flag.py
         11. update_wrk_bookings.py
         12. truncate_bookings_rej.py
         13. truncate_bookings_partition.py
         14. load_wrk_bookings.py
         15. load_wrk_booking_rej.py
         16. truncate_booking_tier_partition.py
         17. load_booking_tier.py\n
"

# This functions is to run PySpark jobs in sequence manner.
spark_run_func(){
for job_name in ${truncate_datarep} ${load_datarep} ${truncate_stg_bookings} ${load_stg_bookings} ${truncate_wrk_bookings} ${load_wrk_bookings} ${truncate_day_flag_temp} ${load_day_flag_temp} ${truncate_day_flag} ${load_day_flag} ${update_wrk_bookings} ${truncate_bookings_rej} ${truncate_bookings_partition} ${load_wrk_bookings} ${load_wrk_booking_rej} ${truncate_booking_tier_partition} ${load_booking_tier}
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
