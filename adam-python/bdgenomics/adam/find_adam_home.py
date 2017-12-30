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

# This script is copied from Apache Spark, with minor modifications.
#
# This script attempt to determine the correct setting for ADAM_HOME given
# that ADAM may have been installed on the system with pip.

from __future__ import print_function
import os
import sys


def _find_adam_home():
    """Find the ADAM_HOME."""
    # If the enviroment has ADAM_HOME set trust it.
    if "ADAM_HOME" in os.environ:
        return os.environ["ADAM_HOME"]

    def is_adam_home(path):
        """Takes a path and returns true if the provided path could be a reasonable ADAM_HOME"""
        return (os.path.isfile(os.path.join(path, "bin/adam-submit")) and
                (os.path.isdir(os.path.join(path, "jars")) or
                 os.path.isdir(os.path.join(path, "adam-assembly/target"))))

    paths = [os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")]

    # Add the path of the ADAM module if it exists
    if sys.version < "3":
        import imp
        try:
            module_home = imp.find_module("bdgenomics")[1]
            paths.append(os.path.join(module_home, "adam"))
        except ImportError:
            # Not pip installed no worries
            pass
    else:
        from importlib.util import find_spec
        try:
            module_home = os.path.dirname(find_spec("bdgenomics").origin)
            paths.append(os.path.join(module_home, "adam"))
        except ImportError:
            # Not pip installed no worries
            pass

    # Normalize the paths
    print(repr(paths), file=sys.stderr)
    paths = [os.path.abspath(p) for p in paths]

    try:
        return next(path for path in paths if is_adam_home(path))
    except StopIteration:
        print("Could not find valid ADAM_HOME while searching {0}".format(paths), file=sys.stderr)
        exit(1)

if __name__ == "__main__":
    print(_find_adam_home())
