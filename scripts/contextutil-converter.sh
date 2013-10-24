#!/bin/sh

# Script to replace getConfiguration calls to use ContextUtil in order to improve Hadoop compatibility
#TODO import parquet.hadoop.util.ContextUtil
for file in `find $1 -name "*.java" -print0 | xargs -0 grep -l "\.getConfiguration("`;
do
	sed -i~ 's@\([A-Za-z]*\)\.getConfiguration([\s]*)@\ContextUtil.getConfiguration\(\1\)@g' $file
	mv $file~ $file
done

