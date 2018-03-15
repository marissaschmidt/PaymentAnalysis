#!/bin/bash -x

#if [ $# -ne 1 ]
#then
#        echo "Usage $0: <replication factor>"
#        exit 1
#fi

if test "$PBS_NODEFILE" = ""
then
	echo "You must allocate nodes via PBS before using hadoop!"
	exit 1
fi

if [ "$HADOOP_HOME" = "" ]; then
export HADOOP_HOME=$HOME/hadoop-install/hadoop
fi
HADOOP_CONF=${HADOOP_HOME}/conf

echo
echo "Using nodes:"
echo "*************"
cat $PBS_NODEFILE
echo "*************"
echo

count=`cat $PBS_NODEFILE | wc -l`
count=$[count-1]

echo "Using $count worker nodes"
cat $PBS_NODEFILE | tail -$count > ${HADOOP_CONF}/slaves

# Set the replication factor using the script in HADOOP_HOME/conf 
#replication=$1
#echo `${HADOOP_CONF}/set_replication.sh ${replication}`
#echo "Replication Factor: ${replication}"

# Move to HADOOP_HOME
cd ${HADOOP_HOME}
/bin/rm -fr logs pids
mkdir {logs,pids}

pdsh -w - < $PBS_NODEFILE  mkdir /tmp/hadoop-`whoami`
pdsh -w - < $PBS_NODEFILE  chmod 700 /tmp/hadoop-`whoami`

for node in $(cat ${HADOOP_CONF}/slaves)
do
	mkdir pids/$node
done

mkdir pids/`hostname`

echo "Formatting the DFS filesystem"
bin/hadoop namenode -format

echo "Starting the Hadoop runtime system on all nodes"
bin/start-all.sh

touch ~/mnt/hdfs/

echo "Starting the fuse module" 
fuse-dfs/fuse_dfs_wrapper.sh dfs://node00:9000 ~/mnt/hdfs/ -obig_writes 
