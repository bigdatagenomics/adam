#!/bin/bash

set +x

grep "<scala\.version>" pom.xml | grep -q 2.12
if [[ $? == 0 ]];
then
    echo "Scala version is already set to 2.12 (Scala artifacts have _2.12 version suffix in artifact name)."
    echo "Cowardly refusing to move to Scala 2.12 a second time..."

    exit 1
fi

find . -name "pom.xml" -exec sed -e "s/2.11.12/2.12.10/g" \
    -e "s/2.11/2.12/g" \
    -i.2.12.bak '{}' \;
find . -name "*.2.12.*bak" -exec rm -f {} \;
