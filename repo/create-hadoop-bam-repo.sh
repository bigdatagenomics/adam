#!/usr/bin/env bash

HADOOP_BAM_PATH="/workspace/hadoop-bam-5.1/"
HADOOP_BAM_VERSION="5.1"

HADOOP_BAM_JAR="$HADOOP_BAM_PATH/hadoop-bam-$HADOOP_BAM_VERSION.jar"

mvn install:install-file -DlocalRepositoryPath=. -DcreateChecksum=true -Dpackaging=jar \
-Dfile=$HADOOP_BAM_JAR -DgroupId=adam  -DartifactId=hadoop-bam  -Dversion=$HADOOP_BAM_VERSION

mvn install:install-file -DlocalRepositoryPath=. -DcreateChecksum=true -Dpackaging=jar \
-Dfile="$HADOOP_BAM_PATH/picard-1.76.jar" -DgroupId=adam  -DartifactId=picard  -Dversion=1.76

mvn install:install-file -DlocalRepositoryPath=. -DcreateChecksum=true -Dpackaging=jar \
-Dfile="$HADOOP_BAM_PATH/sam-1.76.jar" -DgroupId=adam  -DartifactId=samtools  -Dversion=1.76
