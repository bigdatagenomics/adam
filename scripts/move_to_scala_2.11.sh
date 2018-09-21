#!/bin/bash

set +x

grep "<scala\.version>" pom.xml | grep -q 2.11
if [[ $? == 0 ]];
then
    echo "Scala version is already set to 2.11 (Scala artifacts have _2.11 version suffix in artifact name)."
    echo "Cowardly refusing to move to Scala 2.11 a second time..."

    exit 1
fi

find . -name "pom.xml" -exec sed -e "s/2.12.8/2.11.12/g" \
    -e "s/2.12/2.11/g" \
    -i.2.11.bak '{}' \;
find . -name "*.2.11.*bak" -exec rm -f {} \;
