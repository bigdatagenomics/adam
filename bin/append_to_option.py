#!/usr/bin/env python
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

"""Append text to an option.

Usage:

    append_to_option.py DELIMITER OPTION APPEND_STRING OPTION_STRING

For example, running

    append_to_option.py , --jars myproject.jar --option1 value1 --jars otherproject.jar --option2 value2

will write to stdout

    --option1 value1 --jars otherproject.jar,myproject.jar --option2 value2
"""

import sys

delimiter = sys.argv[1]
target = sys.argv[2]
append = sys.argv[3]
original = sys.argv[4:]

if original.count(target) > 1:
    sys.stderr.write("Found multiple %s in the option list." % target)
    sys.exit(1)

if original.count(target) == 0:
    original.extend([target, append])
else:  # original.count(target) == 1
    idx = original.index(target)
    new_value = delimiter.join([original[idx + 1], append])
    original[idx + 1] = new_value
sys.stdout.write(' '.join(original))
