#!/bin/bash

set +x

find . -name "pom.xml" -exec sed -e "/utils-/ s/_2.10/-spark2_2.10/g" \
    -e "/adam-/ s/_2.10/-spark2_2.10/g" \
    -e "/spark.version/ s/1.6.1/2.0.0/g" \
    -i .spark2.bak '{}' \;
