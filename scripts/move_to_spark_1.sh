#!/bin/bash

set +x

grep -q "spark2" pom.xml
if [[ $? == 1 ]];
then
    echo "POM is already set up for Spark 1 (Spark 1/2 artifacts are missing -spark2 suffix in artifact names)."
    echo "Cowardly refusing to move to Spark 1 a second time..."

    exit 1
fi

find . -name "pom.xml" -exec sed \
    -e "s/-spark2//g" \
    -e "/spark.version/ s/2.1.0/1.6.3/g" \
    -i.spark1.bak '{}' \;
