#!/bin/bash

#***********************************************************************************************************
# Developed by : UST GLOBAL
# Version      : 1.0
# Programmer   : Vinay Kumbakonam
# Description  :
#	1. This bash scipt is mainly for 'JB_PARTNER_DETAILS', 'JB_UPD_PARTNER_DETAILS' jobs.
#	2. And checks the run time arguments, if not receives means takes the default parameters.
#       
#
# Initial Creation:
#
# Date (YYYY-MM-DD)		Change Description
# -----------------		------------------
# 2018-09-27			Initial creation
#
#
#**************************************************************************************************************

# Env variables
path="$1"
insert_job="JB_PARTNER_DETAILS"
update_job="JB_UPD_PARTNER_DETAILS"

echo -e "\n This Script Will Trigger Following PySpark Jobs,
        1.$insert_job.py
        2.$update_job.py\n
"

# This functions is to run PySpark jobs in sequence manner.
spark_run_func(){
for job_name in $insert_job $update_job
do
echo -e "\n	Running Job Name : $job_name	\n"
job="spark-submit --packages com.crealytics:spark-excel_2.11:0.9.17 $job_name.py $path ${conf_file}"
eval $job #|& tee
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
