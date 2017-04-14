#!/bin/bash

set +x

grep "<scala\.version>" pom.xml | grep -q 2.11
if [[ $? == 0 ]];
then
    echo "Scala version is already set to 2.11 (Scala artifacts have _2.11 version suffix in artifact name)."
    echo "Cowardly refusing to move to Scala 2.11 a second time..."

    exit 1
fi

find . -name "pom.xml" -exec sed -e "s/2.10.6/2.11.8/g" \
    -e "s/2.10/2.11/g" \
    -i.2.11.bak '{}' \;
# keep parquet-scala at parquet-scala_2.10
find . -name "pom.xml" -exec sed -e "s/parquet-scala_2.11/parquet-scala_2.10/g" -i.2.11.2.bak '{}' \;
# keep maven-javadoc-plugin at version 2.10.4
find . -name "pom.xml" -exec sed -e "s/2.11.4/2.10.4/g" -i.2.11.3.bak '{}' \;
find . -name "*.2.11.*bak" -exec rm -f {} \;
