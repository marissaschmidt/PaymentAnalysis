#!/bin/sh
# 
# Generates the specified number of accounts. 
# 
# Usage: ./generate-test-data <num-accounts> <histogram> <result-file>
#
# Before using this script, you MUST start a Hadoop cluster!! 
# Modify util/run-hadoop-cluster.pbs as needed, then run the 
# following command:
# 
# qsub run-hadoop-cluster.pbs
#------------------------------------------------------------------------------

HADOOP_HOME=$HOME/hadoop-install/hadoop
PROJECT_HOME=$HOME/project/PaymentAnalysis

hadoop=$HADOOP_HOME/bin/hadoop

if [ $# -ne 3 ]
then
	echo "Usage: $0 <num-accounts> <histogram-file> <output-directory>"
	exit 1
fi

accounts=$1
histogram=$2
outputdir=$3

now=`date +%H%M%S`
log=$PROJECT_HOME/logs/generate-test-data_${now}.log

#log=/dev/tty
#resultfile=/dev/tty

tmp_time="/tmp/time.$$"
TIME="/usr/bin/time --output=${tmp_time} --format=%e "

cd ${HADOOP_HOME}

#echo `date +%H:%M:%S`: Starting Hadoop cluster... >> ${log}
#bin/start-all.sh >> ${log} 2>&1
#sleep 5

echo `date +%H:%M:%S`: Reporting HDFS status...  >> ${log}
${hadoop} dfsadmin -report >> ${log} 2>&1

echo `date +%H:%M:%S`: Adding ${histogram} to hdfs...
${hadoop} dfs -put ${histogram} histogram.dat >> ${log} 2>&1

if [ "$?" -ne 0 ]
then
        echo "Error adding ${histogram} to HDFS. See ${log} for detailed log."
        exit 1
fi


echo `date +%H:%M:%S`: Generating data for ${accounts} accounts...
accountgen=$PROJECT_HOME/mapreduce/build/*-accountgen.jar
${hadoop} jar ${accountgen} ${accounts} histogram.dat ${outputdir} >> ${log} 2>&1

if [ "$?" -ne 0 ]
then
        echo "Account generation failed. See ${log} for detailed log."
        exit 1
fi

${hadoop} dfs -rmr ${outputdir}/_logs >> ${log} 2>&1
${hadoop} dfs -rmr ${outputdir}/part* >> ${log} 2>&1

${hadoop} dfs -rmr histogram.dat >> ${log} 2>&1
if [ "$?" -ne 0 ]
then
        echo "Error cleaning up histogram from HDFS. See ${log} for detailed log."
	        exit 1  
fi


datasize=`hadoop dfs -dus ${outputdir} | awk '{print $2}'`
echo "`date +%H:%M:%S`: ${accounts} (${datasize} KB) generated. Output saved to ${outputdir}."
