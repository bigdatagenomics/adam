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


import os
import sys
import tempfile
import unittest

from pyspark.sql import SparkSession

class SparkTestCase(unittest.TestCase):


    def resourceFile(self, filename, module='adam-core'):

        adamRoot = os.path.dirname(os.getcwd())
        return os.path.join(os.path.join(adamRoot,
                                         "%s/src/test/resources" % module),
                            filename)


    def tmpFile(self):

        tempFile = tempfile.NamedTemporaryFile(delete=True)
        tempFile.close()
        return tempFile.name


    def checkFiles(self, file1, file2):

        f1 = open(file1)
        f2 = open(file2)

        try:
            self.assertEqual(f1.read(), f2.read())
        finally:
            f1.close()
            f2.close()


    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        self.ss = SparkSession.builder.master('local[4]').appName(class_name).getOrCreate()
        self.sc = self.ss.sparkContext

        
    def tearDown(self):
        self.sc.stop()
        sys.path = self._old_sys_path
