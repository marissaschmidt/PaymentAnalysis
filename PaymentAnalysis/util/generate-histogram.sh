#!/bin/bash
# 
# Generates the histogram file based on the given sample data directory. 
# 
# Usage: ./generate-histogram <sample-data> [<histogram-file>]
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

if [ $# -ne 1 ]
then
	echo "Usage: $0 <sample-data> [<histogram-file>]"
	exit 1
fi
sampledata=$1

histogram=${PROJECT_HOME}/util/histogram.dat
if [ $# -eq 2 ]
then
	histogram=$2
fi

now=`date +%H%M%S`
log=$PROJECT_HOME/logs/generate-histogram_${now}.log

#exec >> >(tee ${log})
#exec 2>&1

#log=/dev/tty
#resultfile=/dev/tty

tmp_time="/tmp/time.$$"
TIME="/usr/bin/time --output=${tmp_time} --format=%e "

cd ${HADOOP_HOME}

#echo `date +%H:%M:%S`: Starting Hadoop cluster... 
#bin/start-all.sh 2>&1 >> ${log}
#sleep 5

echo `date +%H:%M:%S`: Reporting HDFS status... >> ${log}
${hadoop} dfsadmin -report >> ${log} 2>&1 

echo `date +%H:%M:%S`: Adding ${sampledata} directory to hdfs... 
${hadoop} dfs -rmr sampledata  >> ${log} 2>&1
${hadoop} dfs -put ${sampledata} sampledata >> ${log} 2>&1

if [ "$?" -ne 0 ]
then
	echo "Error adding ${sampledata} to HDFS. See ${log} for detailed log."
	exit 1
fi

echo `date +%H:%M:%S`: Generating histogram... 
histogen=${PROJECT_HOME}/mapreduce/build/*-histogen.jar
${hadoop} jar ${histogen} sampledata histogram  >> ${log} 2>&1

if [ "$?" -ne 0 ]
then
	echo "Histogram generation failed. See ${log} for detailed log."
	exit 1
fi

echo `date +%H:%M:%S`: Saving histogram file to ${histogram}...
${hadoop} dfs -get histogram/Histograms/part-00000 ${histogram} >> ${log}  2>&1

if [ "$?" -ne 0 ]
then
        echo "Error saving histogram file. See ${log} for detailed log."
        exit 1
fi

echo `date +%H:%M:%S`: Removing ${sampledata} from hdfs... >> ${log}
${hadoop} dfs -rmr sampledata >> ${log} 2>&1

if [ "$?" -ne 0 ]
then
        echo "Error cleaning up ${sampledata} from HDFS. See ${log} for detailed log."
	exit 1  
fi

echo `date +%H:%M:%S`: Cleaning up histogram from hdfs... >> ${log} 
${hadoop} dfs -rmr histogram  >> ${log} 2>&1

if [ "$?" -ne 0 ]
then
        echo "Error cleaning up histogram data from HDFS. See ${log} for detailed log."
	exit 1  
fi

