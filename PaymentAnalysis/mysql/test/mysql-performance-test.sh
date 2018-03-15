#!/bin/bash
# 
# Runs MySQL performace tests on given data set. The results are appended to
# the given file location.
# 
# Usage: ./mysql-performance-test <input-directory> <result-file>
# 
# input-directory: the data to run the test on
# result-file: where you want to append the run time results
#------------------------------------------------------------------------------

PROJECT_HOME=$HOME/project/PaymentAnalysis
FUSEDFS_DIR=$HOME/mnt/hdfs/user/`whoami`
THIS_HOME=$PROJECT_HOME/mysql
script=$THIS_HOME/src/script
util=$THIS_HOME/src/util

if [ $# -ne 2 ]
then
	echo "Usage: $0 <input-directory> <result-file>"
	exit 1
fi

inputdirname=$1
resultfile=$2
inputdir=${FUSEDFS_DIR}/${inputdirname}


now=`date +%H%M%S`
log=${PROJECT_HOME}/logs/mysql-performance-test_${now}.log
#log=/dev/tty
#resultfile=/dev/tty

outputfile=$THIS_HOME/test/output/${inputdirname}_result_${now}

touch ${outputfile}
touch ${resultfile}

tmp_time="/tmp/time.$$"
TIME="/usr/bin/time --output=${tmp_time} --format=%e "

inputsize=`du -s -k ${inputdir} | awk '{print $1}'`

echo -------------------------------------------------------------------- >> ${log}
echo "Input directory: ${inputdir}" >> ${log}
echo "Input size (KB): ${inputsize}" >> ${log}
echo "Output file: ${outputfile}" >> ${log}
echo "Test results file: ${resultfile}" >> ${log}
echo "Log file: ${log}" >> ${log}
echo --------------------------------------------------------------------- >> ${log}

echo `date +%H:%M:%S`: Executing MySQL test. See ${log} for output.

echo `date +%H:%M:%S`: Loading payment schema... >> ${log}
mysql < ${script}/payment-schema.sql >> ${log} 2>&1

if [ "$?" -ne 0 ]
then
        echo "Failed to load schema. See ${log} for detailed log."
        exit 1
fi

echo `date +%H:%M:%S`: Generating load-data-payment.q...  >> ${log}
${util}/generate-load-data-script.sh ${inputdir} ${script} >> ${log} 2>&1

if [ "$?" -ne 0 ]
then
        echo "Failed to load data. See ${log} for detailed log."
        exit 1  
fi  


echo `date +%H:%M:%S`: Loading data from ${inputdir} to database... >> ${log}
mysql < ${script}/load-data-payment.sql >> ${log} 2>&1
if [ "$?" -ne 0 ]
then
        echo "Failed to load data. See ${log} for detailed log."
        exit 1  
fi 
count=`mysql < ${script}/account-count.sql | awk 'NR==2'`
echo $count accounts loaded. >> ${log}

echo `date +%H:%M:%S`: Executing MySQL query... >> ${log}
#runtest=mysql < ${script}/result.sql
${TIME} mysql < ${script}/result.sql >> ${log} 2>&1

if [ "$?" -ne 0 ]
then
        echo "MySQL test failed. See ${log} for detailed log."
        exit 1
fi

touch ${resultfile}

echo `date +%H:%M:%S`: Dumping query results to ${outputfile}... >> ${log}
mysql < ${script}/result-dump.sql >> ${outputfile}

if [ "$?" -ne 0 ]
then
        echo "Error dumping results to ${outputfile}. See ${log} for detailed log."
        exit 1  
fi  

echo `date +%H:%M:%S`: Writing test results to ${resultfile}... >> ${log}
runtime=`cat ${tmp_time} | awk '{print $1}'`
echo "mysql_${now}, -, ${inputsize}, ${runtime}" >> ${resultfile}

echo `date +%H:%M:%S`: MySQL test complete. Results appended to ${resultfile}.
echo See ${log} for detailed log.
