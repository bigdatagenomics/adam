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

SPARK_CMD=${1:-spark-submit}

# Find spark-submit script
if [ -z "$SPARK_HOME" ]; then
  SPARK_SUBMIT=$(which ${SPARK_CMD} || echo)
else
  SPARK_SUBMIT=${SPARK_HOME}/bin/${SPARK_CMD}
fi
if [ -z "$SPARK_SUBMIT" ]; then
  echo "SPARK_HOME not set and ${SPARK_CMD} not on PATH; Aborting." 1>&2
  exit 1
fi

echo ${SPARK_SUBMIT}
