#!/bin/bash

set +x

grep -q "spark2" pom.xml
if [[ $? == 0 ]];
then
    echo "POM is already set up for Spark 2 (Spark 2 artifacts have -spark2 suffix in artifact names)."
    echo "Cowardly refusing to move to Spark 2 a second time..."

    exit 1
fi

svp="\${scala.version.prefix}"
substitution_cmd="s/-spark3_$svp/-spark2_$svp/g"

find . -name "pom.xml" -exec sed \
    -e "/adam-/ s/-spark3_2\.1/-spark2_2\.1/" \
    -e "/adam-/ $substitution_cmd" \
    -e "/utils-/ s/-spark3_2\.1/-spark2_2\.1/" \
    -e "/utils-/ $substitution_cmd" \
    -e "/spark.version/ s/3.0.1/2.4.7/g" \
    -i.spark2.bak '{}' \;
