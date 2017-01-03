#!/bin/bash

set +x

grep "<scala\.version>" pom.xml | grep -q 2.10
if [[ $? == 0 ]];
then
    echo "Scala version is already set to 2.10 (Scala artifacts have _2.10 version suffix in artifact name)."
    echo "Cowardly refusing to move to Scala 2.10 a second time..."

    exit 1
fi

find . -name "pom.xml" -exec sed -e "s/2.11.8/2.10.6/g" \
    -e "/bdg-utils.version/! s/2.11/2.10/g" -i.2.10.bak \
    -e "/no Scala/ s/Scala 2.10/Scala 2.11/g" -i.2.10.bak \
    '{}' \;
find . -name "*.2.10.bak" -exec rm -f {} \;
