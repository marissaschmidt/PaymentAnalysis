#!/bin/sh

if [ "$HADOOP_HOME" = "" ]; then
export HADOOP_HOME=$HOME/hadoop-install/hadoop
fi

echo -n "Warning: stopping adhoc cluster and deleting the hadoop filesystem!! Are you sure (y/n) "
read response
if test "$response" = "y"
then
	cd ${HADOOP_HOME}
	bin/stop-all.sh
	/bin/rm -fr logs pids
	echo
	echo "Removing hadoop filesystem directories"
	pdsh -a /bin/rm -fr /tmp/hadoop-`whoami`
	rm -fr filesystem
	rm -fr /tmp/hadoop-`whoami`
        echo "Unmounting fuse-dfs"
        fusermount -u ~/mnt/hdfs
	echo
fi
