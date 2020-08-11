#!/bin/bash

#*********************************************************************************************************************************************************************
# Developed by : UST GLOBAL
# Version      : 1.0
# Programmer   : Vinay Kumbakonam
# Description  :
#       1. This bash scipt is mainly for 'JB_PARTNER_BUSINESS_MODEL_TRUNCATE', 'JB_PARTNER_BUSINESS_MODEL', 'JB_UPD_BUSINESS_MODEL', 'JB_UPD_BUSINESS_MODEL_LSD' jobs.
#       2. And checks the run time arguments, if not receives means takes the default parameters.
#
#
# Initial Creation:
#
# Date (YYYY-MM-DD)		Change Description
# -----------------		------------------
# 2018-09-27			Initial creation
#
#
#*********************************************************************************************************************************************************************

# Env Variables =======================================
path="$1"
truncate_job="JB_PARTNER_BUSINESS_MODEL_TRUNCATE"
insert_job="JB_PARTNER_BUSINESS_MODEL"
update_job="JB_UPD_BUSINESS_MODEL"
lsd_update_job="JB_UPD_BUSINESS_MODEL_LSD"

#======================================================

echo -e "\n This Script Will Trigger Following PySpark Jobs,
        1. $truncate_job.py
        2. $insert_job.py
        3. $update_job.py
        4. $lsd_update_job.py\n
"


# This functions is to run PySpark jobs in sequence manner.
spark_run_func(){
for job_name in $truncate_job $insert_job $update_job $lsd_update_job
do
echo -e  "\n	Running Job Name : $job_name	\n"
job="spark-submit --packages com.crealytics:spark-excel_2.11:0.9.17 $job_name.py $path ${conf_file}"
eval $job #|& tee
echo "done"
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
echo ${conf_file}
export conf_file
spark_run_func
else
echo "  Run time argumets are not passed "
conf_file=/home/spark/v_kumbakonam/v_prod/db.properties
path=`pwd`
echo ${conf_file}
spark_run_func
fi
}

# Calling functions
run_file_func $path

# Exit
exit 0
