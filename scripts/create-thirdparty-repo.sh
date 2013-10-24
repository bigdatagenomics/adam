#!/usr/bin/env bash

HADOOP_BAM_PATH="/workspace/hadoop-bam-6.0/"
PICARD_VERSION="1.93"
SAMTOOLS_VERSION=$PICARD_VERSION

REPO_ID="adam-thirdparty"
OUTPUT_DIR="/tmp/repo/$REPO_ID"
# Delete any old artifacts
rm -rf $OUTPUT_DIR
OUTPUT_URL="file://$OUTPUT_DIR"

mvn deploy:deploy-file -Durl=$OUTPUT_URL -DrepositoryId=$REPO_ID -Dfile=$HADOOP_BAM_PATH/picard-$PICARD_VERSION.jar \
-DgroupId=picard -DartifactId=picard -Dversion=$PICARD_VERSION -DgeneratePom.description="Picard"

mvn deploy:deploy-file -Durl=$OUTPUT_URL -DrepositoryId=$REPO_ID -Dfile=$HADOOP_BAM_PATH/sam-$SAMTOOLS_VERSION.jar \
-DgroupId=samtools -DartifactId=samtools -Dversion=$SAMTOOLS_VERSION -DgeneratePom.description="Samtools"

pushd $OUTPUT_DIR
scp -r * massie@watson.millennium.berkeley.edu:public_html/maven
popd
echo "Removing the directory since it's been copied remotely. Verify the directory deletion below..."
rm -ir $OUTPUT_DIR
