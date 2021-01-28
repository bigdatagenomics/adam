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


from bdgenomics.adam.adamContext import ADAMContext
from bdgenomics.adam.ds import CoverageDataset, FeatureDataset
from bdgenomics.adam.test import SparkTestCase
import os

class CoverageDatasetTest(SparkTestCase):

    def test_save(self):

        testFile = self.resourceFile("sorted.sam")
        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(testFile)
        coverage = reads.toCoverage()
        tmpPath = self.tmpFile() + ".coverage.adam"
        coverage.save(tmpPath,
                            asSingleFile=True,
                            disableFastConcat=True)
        assert(os.listdir(tmpPath) != [])


    def test_collapse(self):
        testFile = self.resourceFile("sorted.sam")
        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(testFile)
        coverage = reads.toCoverage()
        collapsed = coverage.collapse()
        self.assertEqual(collapsed.toDF().count(), coverage.toDF().count())

    def test_toFeatures(self):
        testFile = self.resourceFile("sorted.sam")
        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(testFile)
        coverage = reads.toCoverage()
        features = coverage.toFeatures()

        assert(isinstance(features, FeatureDataset))
        self.assertEquals(features.toDF().count(), coverage.toDF().count())

    def test_aggregatedCoverage(self):
        testFile = self.resourceFile("small.sam")
        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(testFile)
        coverage = reads.toCoverage()
        collapsed = coverage.aggregatedCoverage(10)
        self.assertEqual(collapsed.toDF().count(), 166)

    def test_flatten(self):
        testFile = self.resourceFile("small.sam")
        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(testFile)
        coverage = reads.toCoverage()
        flattened = coverage.flatten()
        self.assertEqual(flattened.toDF().count(), 1500)
