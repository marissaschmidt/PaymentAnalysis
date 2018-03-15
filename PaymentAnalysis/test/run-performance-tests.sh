#!/bin/sh
# 
# Runs MySQL, MapReduce and Hadoop performance tests on the same 
# set of generated data. 
# 
# Usage: ./run-performance-tests <num-accounts> <result-file>
#
# Before using this script, you MUST start a Hadoop cluster!! 
# Modify util/run-hadoop-cluster.pbs as needed, then run the 
# following command:
# 
# qsub run-hadoop-cluster.pbs
#------------------------------------------------------------------------------

HADOOP_HOME=$HOME/hadoop-install/hadoop
PROJECT_HOME=$HOME/project/PaymentAnalysis
HDFS_DIR=$HOME/mnt/hdfs/user/`whoami`
UTIL_DIR=$PROJECT_HOME/util
TEST_DIR=$PROJECT_HOME/test
hadoop=$HADOOP_HOME/bin/hadoop

mapreduce_test=$PROJECT_HOME/mapreduce/test/mapreduce-performance-test.sh
mysql_test=$PROJECT_HOME/mysql/test/mysql-performance-test.sh
hive_test=$PROJECT_HOME/hive/test/hive-performance-test.sh

if [ $# -ne 2 ]
then
	echo "Usage: $0 <num-accounts> <result-file>"
	exit 1
fi

accounts=$1
resultfile=$2

now=`date +%H%M%S`
log=$PROJECT_HOME/logs/run-performance-tests_${now}.log
#log=/dev/tty
#resultfile=/dev/tty

tmp_time="/tmp/time.$$"
TIME="/usr/bin/time --output=${tmp_time} --format=%e "

echo `date +%H:%M:%S`: Reporting HDFS status...  >> ${log}
${hadoop} dfsadmin -report >> ${log} 2>&1

histogram=${UTIL_DIR}/histogram.dat
if [ ! -e ${histogram} ]
then
	echo "Generating histogram from sample data."
	${UTIL_DIR}/generate-histogram.sh ${TEST_DIR}/data >> ${log} 2>&1
else
	echo "Histogram exists"
fi

gendata="accounts_${accounts}"
echo "Generating account data."
${UTIL_DIR}/generate-data-from-histogram.sh ${accounts} ${histogram} ${gendata} >> ${log} 2>&1

echo `date +%H:%M:%S`: Reporting HDFS status...  >> ${log}
${hadoop} dfsadmin -report >> ${log} 2>&1

datasize=`hadoop dfs -dus ${gendata} | awk '{print $2}'`
echo `date +%H:%M:%S`: Size of ${gendata}: ${datasize} >> ${log}

echo Number of accounts: ${accounts} >> ${resultfile} 
${mapreduce_test} ${gendata} ${resultfile}
${mysql_test} ${gendata} ${resultfile}
${hive_test} ${gendata} ${resultfile}
echo >> ${resultfile}

echo All tests complete!! Results are in ${resultfile}. >> ${log}
#echo Log files located at: $PROJECT_HOME/logs
