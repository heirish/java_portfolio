#!/bin/bash

filesystem=$1
level=$2

#set -x

i=1
while [ $i -le $level ];do
    if [ "$filesystem" == "hdfs" ]; then
        hadoop fs -cat hdfs://localhost:9000/dataout/patternrefined-$i/* | wc -l
    else
        wc -l dataout/patternrefined-$i/part*
    fi
    i=`expr $i + 1`
done
