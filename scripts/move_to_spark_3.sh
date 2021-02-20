#!/bin/bash

set +x

grep -q "spark3" pom.xml
if [[ $? == 0 ]];
then
    echo "POM is already set up for Spark 3 (Spark 3 artifacts have -spark3 suffix in artifact names)."
    echo "Cowardly refusing to move to Spark 3 a second time..."

    exit 1
fi

svp="\${scala.version.prefix}"
substitution_cmd="s/-spark2_$svp/-spark3_$svp/g"

find . -name "pom.xml" -exec sed \
    -e "/adam-/ s/-spark2_2\.1/-spark3_2\.1/" \
    -e "/adam-/ $substitution_cmd" \
    -e "/utils-/ s/-spark2_2\.1/-spark3_2\.1/" \
    -e "/utils-/ $substitution_cmd" \
    -e "/spark.version/ s/2.4.7/3.0.2/g" \
    -i.spark3.bak '{}' \;
