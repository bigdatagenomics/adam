#!/bin/bash

set +x

find . -name "pom.xml" -exec sed \
    -e "s/-spark2//g" \
    -e "/spark.version/ s/2.0.0/1.6.3/g" \
    -i.spark1.bak '{}' \;
