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

from __future__ import print_function
import glob
import os
import sys
from setuptools import find_packages, setup

from version import version as adam_version

if sys.version_info < (2, 7):
    print("Python versions prior to 2.7 are not supported for pip installed ADAM.",
          file=sys.stderr)
    exit(-1)

# Provide guidance about how to use setup.py
incorrect_invocation_message = """
If you are installing bdgenomics.adam from ADAM's source, you must first build ADAM and
run make develop.

    To build ADAM with maven you can run:
      ./build/mvn -DskipTests clean package
    Building the source dist is done in the Python directory:
      cd adam-python
      make sdist
      pip install dist/*.tar.gz"""

# Figure out where the jars are we need to package
ADAM_HOME = os.path.abspath("../")
TEMP_PATH = "deps"

ALL_JARS_PATH = glob.glob(os.path.join(ADAM_HOME, "adam-assembly/target/adam-assembly*.jar"))
JARS_PATH = list(filter(lambda x: (("sources" not in x) and ("javadoc" not in x)),
                   ALL_JARS_PATH))
JARS_TARGET = os.path.join(TEMP_PATH, "jars")

SCRIPTS_PATH = os.path.join(ADAM_HOME, "bin")
SCRIPTS_TARGET = os.path.join(TEMP_PATH, "bin")

if len(JARS_PATH) == 1:
    JARS_PATH = JARS_PATH[0]
elif len(JARS_PATH) > 1:
    print("Assembly jars exist for multiple Scala versions ({0}), please cleanup assembly/target".format(
        JARS_PATH), file=sys.stderr)
    sys.exit(-1)
elif len(JARS_PATH) == 0 and not os.path.exists(TEMP_PATH):
    print(incorrect_invocation_message, file=sys.stderr)
    sys.exit(-1)

in_adam = os.path.isfile("../adam-core/src/main/scala/org/bdgenomics/adam/rdd/ADAMContext.scala")

if in_adam:
    try:
        os.mkdir(TEMP_PATH)
    except:
        print("Temp path for symlink to parent already exists {0}".format(TEMP_PATH),
              file=sys.stderr)
        exit(-1)

try:
    if in_adam:
        os.mkdir(JARS_TARGET)
        os.symlink(JARS_PATH, os.path.join(JARS_TARGET, 'adam.jar'))
        os.symlink(SCRIPTS_PATH, SCRIPTS_TARGET)

    packages = find_packages(exclude=['*.test.*'])
    packages.extend(['bdgenomics.adam.jars',
                     'bdgenomics.adam.bin'])

    # Scripts directive requires a list of each script path and does not take wild cards.
    script_names = os.listdir(SCRIPTS_TARGET)
    scripts = list(map(lambda script: os.path.join(SCRIPTS_TARGET, script), script_names))

    # We add find_adam_home.py to the bin directory we install so that pip installed
    # bdgenomics.adam will search for ADAM_HOME with Python.
    scripts.append("bdgenomics/adam/find_adam_home.py")

    long_description = "!!!!! missing pandoc do not upload to PyPI !!!!"
    try:
        import pypandoc
        long_description = pypandoc.convert('README.md', 'rst')
    except ImportError:
        print("Could not import pypandoc - required to package bdgenomics.adam", file=sys.stderr)
    except OSError:
        print("Could not convert - pandoc is not installed", file=sys.stderr)

    setup(
        name='bdgenomics.adam',
        version=adam_version,
        description='A fast, scalable genome analysis system',
        long_description=long_description,
        author='Big Data Genomics',
        author_email='adam-developers@googlegroups.com',
        url="https://github.com/bdgenomics/adam",
        scripts=scripts,
        packages=packages,
        include_package_data=True,
        install_requires=['pyspark>=1.6.0'],
        package_dir={'bdgenomics.adam.jars': JARS_TARGET,
                     'bdgenomics.adam.bin': SCRIPTS_TARGET},
        package_data={'bdgenomics.adam.jars': ['adam.jar'],
                      'bdgenomics.adam.bin': ['*']},
        classifiers=[
            'License :: OSI Approved :: Apache Software License',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: Implementation :: CPython',
            'Programming Language :: Python :: Implementation :: PyPy',
            'Topic :: Scientific/Engineering :: Bio-Informatics'])

finally:
    if in_adam:
        os.remove(os.path.join(JARS_TARGET, 'adam.jar'))
        os.rmdir(JARS_TARGET)
        os.remove(SCRIPTS_TARGET)
        os.rmdir(TEMP_PATH)

