#!/bin/bash

set +x

find . -name "pom.xml" -exec sed -e "s/2.11.8/2.10.6/g" -e "s/2.11/2.10/g" -i .2.10.bak '{}' \;
find . -name "*.2.10.bak" -exec rm {} \;
