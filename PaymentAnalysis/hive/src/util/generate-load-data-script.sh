#!/bin/bash

FUSEDFS_DIR=$HOME/mnt/hdfs/user/`whoami`

if [ $# -ne 2 ]
then
        echo "Usage: $0 <input-dir> <output-dir>"
        exit 1
fi

inputdir=$1
outfile="$2/load-data-payment.q"

rm ${outfile}
touch ${outfile}

data=${FUSEDFS_DIR}/${inputdir}

for f in ${data}/*
do
	file=${f##*/}
	echo "load data inpath '$1/$file'" >> ${outfile}
	ind=[`expr index "$f" '-'`-1]
	base=${file%%-*}
	echo "into table $base;" >> ${outfile}
	echo "" >> ${outfile}
done
