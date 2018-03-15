#!/bin/bash -x

if test "$PBS_NODEFILE" = ""
then
	echo "You must allocate nodes via PBS before using hadoop!"
	exit 1
fi

echo
echo "Using nodes:"
echo "*************"
cat $PBS_NODEFILE
echo "*************"
echo

if [ "$HADOOP_HOME" = "" ]; then
export HADOOP_HOME=$HOME/hadoop-install/hadoop
fi

echo "Starting the Hadoop runtime system on all nodes"
cd ${HADOOP_HOME} 
bin/start-all.sh 
bin/hadoop dfsadmin -safemode leave
