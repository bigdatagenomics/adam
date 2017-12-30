#!/usr/bin/env bash
#
# Licensed to Big Data Genomics (BDG) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The BDG licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

SOURCE_DIR=$(dirname ${BASH_SOURCE[0]})
. ${SOURCE_DIR}/find-adam-home

# Find ADAM cli assembly jar
ADAM_CLI_JAR=
if [ -d "$ADAM_HOME/repo" ]; then
  ASSEMBLY_DIR="$ADAM_HOME/repo"
elif [ -d "$ADAM_HOME/jars" ]; then
  ASSEMBLY_DIR="$ADAM_HOME/jars"
else
  ASSEMBLY_DIR="$ADAM_HOME/adam-assembly/target"
fi

ASSEMBLY_JARS=$(ls -1 "$ASSEMBLY_DIR" | grep "^adam[0-9A-Za-z\.\_\-]*\.jar$" | grep -v javadoc | grep -v sources || true)
num_jars=$(echo ${ASSEMBLY_JARS} | wc -w)

if [ "$num_jars" -eq "0" ]; then
  echo "Failed to find ADAM cli assembly in $ASSEMBLY_DIR." 1>&2
  echo "You need to build ADAM before running this program." 1>&2
  exit 1
fi

if [ "$num_jars" -gt "1" ]; then
  echo "Found multiple ADAM cli assembly jars in $ASSEMBLY_DIR:" 1>&2
  echo "$ASSEMBLY_JARS" 1>&2
  echo "Please remove all but one jar." 1>&2
  exit 1
fi

echo "${ASSEMBLY_DIR}/${ASSEMBLY_JARS}"
