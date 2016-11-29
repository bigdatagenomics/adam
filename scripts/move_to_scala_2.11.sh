#!/bin/bash

set +x

find . -name "pom.xml" -exec sed -e "s/2.10.6/2.11.8/g" -e "/bdg-utils.version/!s/2.10/2.11/g" -i.2.11.bak '{}' \;
# keep parquet-scala at parquet-scala_2.10
find . -name "pom.xml" -exec sed -e "s/parquet-scala_2.11/parquet-scala_2.10/g" -i.2.11.2.bak '{}' \;
# keep maven-javadoc-plugin at version 2.10.3
find . -name "pom.xml" -exec sed -e "s/2.11.3/2.10.3/g" -i.2.11.3.bak '{}' \;
find . -name "*.2.11.*bak" -exec rm {} \;
