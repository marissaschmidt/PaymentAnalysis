#!/bin/sh
# 
# Runs MapReduce performance test on given data set. The results are 
# appended to the given file location.
#
# Usage: ./mapreduce-performance-test <input-directory> <result-file>
# 
# input-directory: the data to run the test on 
# result-file: where you want to append the run time results
#
# Before using this script, you MUST start a Hadoop cluster!! 
# Modify util/run-hadoop-cluster.pbs as needed, then run the 
# following command:
# 
# qsub run-hadoop-cluster.pbs
#------------------------------------------------------------------------------

HADOOP_HOME="$HOME/hadoop-install/hadoop"
PROJECT_HOME="$HOME/project/PaymentAnalysis"
MAPREDUCE_HOME=$PROJECT_HOME/mapreduce
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
log=${PROJECT_HOME}/logs/mapreduce-performance-test_${now}.log
#stdout=/dev/tty
#resultfile=/dev/tty

tmp_time="/tmp/time.$$"
TIME="/usr/bin/time --output=${tmp_time} --format=%e "

outputdir=${inputdir}_result_${now}
inputsize=`hadoop dfs -dus ${inputdir} | awk '{print $2}'`

echo -------------------------------------------------------------------- >> ${log}
echo "Input directory: ${inputdir}" >> ${log}
echo "Input size (KB): ${inputsize}" >> ${log}
echo "Output directory: ${outputdir}" >> ${log}
echo "Test results file: ${resultfile}" >> ${log}
echo "Log file: ${log}" >> ${log}
echo --------------------------------------------------------------------- >> ${log}
echo `date +%H:%M:%S`: Checking HDFS status... >> ${log}
${hadoop} dfsadmin -report >> ${log} 2>&1

echo  `date +%H:%M:%S`: Executing mapreduce test. See ${log} for ouput.
runtest="${hadoop} jar ${MAPREDUCE_HOME}/build/*-main.jar ${inputdir} ${outputdir}"
${TIME} ${runtest} >> ${log} 2>&1

if [ "$?" -ne 0 ]
then 
	echo "MapReduce test failed. See ${log} for detailed log."
	exit 1
fi

touch ${resultfile}

echo `date +%H:%M:%S`: Writing test results to ${resultfile}... >> ${log}
runtime=`cat ${tmp_time} | awk '{print $1}'`
echo "hadoop_${now}, ${slavecount}, ${inputsize}, ${runtime}" >> ${resultfile}

echo `date +%H:%M:%S`: MapReduce test complete. Results appended to ${resultfile}.
echo "See ${log} for detailed log."
