#!/bin/bash

if [ $# -ne 2 ]
then
        echo "Usage: $0 <input-dir> <output-dir>"
        exit 1
fi

inputdir=$1
output="$2/load-data-payment.sql"

rm -rf ${inputdir}/_logs
rm -rf ${inputdir}/part*

echo "use \`payment\`;" > ${output}

for f in ${inputdir}/*
do
	echo "load data local infile '$f'" >> ${output}
	ind=[`expr index "$f" '-'`-1]
	file=${f##*/}
	base=${file%%-*}
	echo "into table $base" >> ${output}
	echo "fields terminated by ','" >> ${output}
        echo "lines terminated by '\n';" >> ${output}
	echo "" >> ${output}
done
