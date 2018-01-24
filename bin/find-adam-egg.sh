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

# Find ADAM python egg
if [ -d "$ADAM_HOME/repo" ]; then
  DIST_DIR="$ADAM_HOME/repo"
else
  DIST_DIR="$ADAM_HOME/adam-python/dist"
fi

DIST_EGG=$(ls -1 "$DIST_DIR" | grep "^bdgenomics\.adam[0-9A-Za-z\.\_\-]*.egg$" || true)
num_egg=$(echo ${DIST_EGG} | wc -w)

if [ "$num_egg" -eq "0" ]; then
  echo "Failed to find ADAM egg in $DIST_DIR." 1>&2
  echo "You need to build ADAM before running this program." 1>&2
  exit 1
fi

if [ "$num_egg" -gt "1" ]; then
  echo "Found multiple ADAM eggs in $DIST_DIR:" 1>&2
  echo "$DIST_EGG" 1>&2
  echo "Please remove all but one egg." 1>&2
  exit 1
fi

echo "${DIST_DIR}/${DIST_EGG}"
