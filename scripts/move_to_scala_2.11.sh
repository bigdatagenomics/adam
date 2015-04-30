#!/bin/bash

set +x

find . -name "pom.xml" -exec sed -e "s/2.10.4/2.11.4/g" -e "s/2.10/2.11/g" -i .2.11.bak '{}' \;
find . -name "pom.xml" -exec sed -e "s/parquet-scala_2.11/parquet-scala_2.10/g" -i .2.11.2.bak '{}' \;
find . -name "*.2.11.*bak" -exec rm {} \;
