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

import xml.etree.ElementTree as ET

from setuptools import setup, find_packages

def readme():
    with open('README.md', 'r') as ip:
        return ip.read()

def version():
    tree = ET.parse('../pom.xml')
    root = tree.getroot()
    version = root.findall('{http://maven.apache.org/POM/4.0.0}version')
    if len(version) != 1:
        raise ValueError('Failed to pull out repo version from pom.xml')
    return version[0].text

setup(
    name='pyadam',
    version=version(),
    description='Python API to ADAM genomics project',
    long_description=readme(),
    author='Uri Laserson',
    author_email='uri.laserson@gmail.com',
    url='http://bdgenomics.org/',
    packages=find_packages(),
    keywords='genomics omics adam hadoop spark apache',
    license='Apache License, Version 2.0')
