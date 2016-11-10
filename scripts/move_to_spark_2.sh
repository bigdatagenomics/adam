#!/bin/bash

set +x

svp="\${scala\.version\.prefix}"
substitution_cmd="s/_$svp/-spark2_$svp/g"

find . -name "pom.xml" -exec sed \
    -e "/utils-/ s/_2\.10/-spark2_2.10/g" \
    -e "/adam-/ s/_2\.10/-spark2_2.10/g" \
    -e "/utils-/ $substitution_cmd" \
    -e "/adam-/ $substitution_cmd" \
    -e "/spark\.version/ s/1\.6\.1/2.0.0/g" \
    -i.spark2.bak '{}' \;
