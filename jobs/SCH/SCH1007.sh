#!/bin/bash

#***********************************************************************************************************************
# Developed by : UST GLOBAL
# Version      : 1.0
# Programmer   : Vinay Kumbakonam
# Description  :
#       1. This bash scipt is mainly for 'JB_PARTNER_SELLING_TERRITORY_TRUNCATE' and 'JB_PARTNER_SELLING_TERRITORY'
#       2. And checks the run time arguments, if not receives means takes the default parameters.
#
# Initial Creation:
#
# Date (YYYY-MM-DD)		Change Description
# -----------------		------------------
# 2018-09-27			Initial creation
#
#
#**********************************************************************************************************************

# Env Variables =====================================

path="$1"
truncate_job="JB_PARTNER_SELLING_TERRITORY_TRUNCATE"
insert_job="JB_PARTNER_SELLING_TERRITORY"

#=====================================================

echo -e "\n This Script Will Trigger Following PySpark Jobs,
        1. $truncate_job.py
        2. $insert_job.py\n
"

# This functions is to run PySpark jobs in sequence manner.
spark_run_func(){
for job_name in $truncate_job $insert_job
do
echo -e  "\n    Running Job Name : $job_name    \n"
job="spark-submit --packages com.crealytics:spark-excel_2.11:0.9.17 $job_name.py $path ${conf_file}"
eval $job #|& tee
echo "done"
if [ $? -eq 0 ];then
echo -e "\n     PySpark Job $job_name Succeed   \n"
else echo -e "\n        [ERROR] : PySpark Job $job_name Failed  \n"
exit 1
fi
done
}

# Checks for run time arguments
run_file_func(){
if [ $# -gt 0 ];then
conf_file=$path/db.properties
echo -e "\n     Running with ${conf_file} file "
export conf_file
spark_run_func
else
echo "File Not passed, Taking Default configuration file"
conf_file=/home/spark/v_kumbakonam/v_prod/db.properties
path=`pwd`
echo -e "\n     Running with ${conf_file} file "
spark_run_func
fi
}

# Calling functions
run_file_func $path

# Exit
exit 0
