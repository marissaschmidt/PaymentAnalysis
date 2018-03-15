#!/bin/sh
# 
# Runs Hive performance test on given data set. The results are appended to 
# the given file location.
#
# Usage: ./hive-performance-test <input-directory> <result-file>
# 
# input-directory: the data to run the test on 
# result-file: where you want to append the run time results
#
# Before using this script, you MUST start a Hadoop cluster!! 
# Modify ${HADOOP_HOME}/run-hadoop-cluster.pbs as needed, then run the 
# following command:
# 
# qsub ${HADOOP_HOME}/run-hadoop-cluster.pbs
#------------------------------------------------------------------------------
HIVE_HOME=$HOME/hadoop-install/hive
HADOOP_HOME=$HOME/hadoop-install/hadoop
PROJECT_HOME=$HOME/project/PaymentAnalysis
FUSEDFS_DIR=$HOME/mnt/hdfs/user/`whoami`
THIS_HOME=$PROJECT_HOME/hive
script=$THIS_HOME/src/script
util=$THIS_HOME/src/util

hive=$HIVE_HOME/bin/hive
hadoop=$HADOOP_HOME/bin/hadoop

slavecount=`cat ${HADOOP_HOME}/conf/slaves | wc -l`

if [ $# -ne 2 ]
then
	echo "Usage: $0 <input-directory> <result-file>"
	exit 1
fi

inputdir=$1
resultfile=$2

now=`date +%H%M%S`
log=${PROJECT_HOME}/logs/hive-performance-test_${now}.log
#log=/dev/tty
#resultfile=/dev/tty

outputfile=$THIS_HOME/test/output/${inputdir}_result_${now}

tmp_time="/tmp/time.$$"
TIME="/usr/bin/time --output=${tmp_time} --format=%e "

inputsize=`${hadoop} dfs -dus ${inputdir} | awk '{print $2}'`

echo -------------------------------------------------------------------- >> ${log}
echo "Input directory: ${inputdir}" >> ${log}
echo "Input size (KB): ${inputsize}" >> ${log}
echo "Output file: ${outputfile}" >> ${log}
echo "Test results file: ${resultfile}" >> ${log}
echo "Log file: ${log}" >> ${log}
echo --------------------------------------------------------------------- >> ${log}

echo `date +%H:%M:%S`: Executing Hive test. See ${log} for output.

echo `date +%H:%M:%S`: Loading payment schema... >> ${log}
${hive} -f ${script}/payment-schema.q >> ${log} 2>&1
if [ "$?" -ne 0 ]
then
        echo "Failed to load schema. See ${log} for detailed log."
        exit 1
fi

echo `date +%H:%M:%S`: Generating load-data-payment.q... >> ${log}
${util}/generate-load-data-script.sh ${inputdir} ${script} >> ${log} 2>&1

echo `date +%H:%M:%S`: Loading data from ${inputdir} to database... >> ${log}
${hive} -f ${script}/load-data-payment.q >> ${log} 2>&1

if [ "$?" -ne 0 ]
then
        echo "Failed to load data. See ${log} for detailed log."
        exit 1
fi

echo `date +%H:%M:%S`: Executing Hive query... >> ${log}
${TIME} ${hive} -f ${script}/result.q >> ${log} 2>&1

if [ "$?" -ne 0 ]
then
        echo "Hive test failed. See ${log} for detailed log."
        exit 1
fi


echo `date +%H:%M:%S`: Dumping query results to ${outputfile}... >> ${log}
${hive} -f ${script}/result-dump.q >> ${outputfile} 2>> ${log}

if [ "$?" -ne 0 ]
then
        echo "Error dumping results to ${outputfile}. See ${log} for detailed log."
        exit 1
fi

echo `date +%H:%M:%S`: Writing test results to ${resultfile}... >> ${log}
runtime=`cat ${tmp_time} | awk '{print $1}'`
echo "hive_${now}, ${slavecount}, ${inputsize}, ${runtime}" >> ${resultfile}

echo `date +%H:%M:%S`: Hive test complete. Results appended to ${resultfile}.
echo See ${log} for detailed log.
