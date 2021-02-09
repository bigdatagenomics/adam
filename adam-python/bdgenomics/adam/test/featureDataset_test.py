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
from bdgenomics.adam.test import SparkTestCase


class FeatureDatasetTest(SparkTestCase):

    
    def test_round_trip_gtf(self):

        testFile = self.resourceFile("Homo_sapiens.GRCh37.75.trun20.gtf")
        ac = ADAMContext(self.ss)
        
        features = ac.loadFeatures(testFile)
        tmpPath = self.tmpFile() + ".gtf"
        features.save(tmpPath,
                      asSingleFile=True)

        savedFeatures = ac.loadFeatures(testFile)

        self.assertEqual(features._jvmDataset.jrdd().count(),
                          savedFeatures._jvmDataset.jrdd().count())


    def test_round_trip_bed(self):

        testFile = self.resourceFile("gencode.v7.annotation.trunc10.bed")
        ac = ADAMContext(self.ss)
        
        features = ac.loadFeatures(testFile)
        tmpPath = self.tmpFile() + ".bed"
        features.save(tmpPath,
                      asSingleFile=True)

        savedFeatures = ac.loadFeatures(testFile)

        self.assertEqual(features._jvmDataset.jrdd().count(),
                          savedFeatures._jvmDataset.jrdd().count())


    def test_round_trip_narrowPeak(self):

        testFile = self.resourceFile("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
        ac = ADAMContext(self.ss)
        
        features = ac.loadFeatures(testFile)
        tmpPath = self.tmpFile() + ".narrowPeak"
        features.save(tmpPath,
                      asSingleFile=True)

        savedFeatures = ac.loadFeatures(testFile)

        self.assertEqual(features._jvmDataset.jrdd().count(),
                          savedFeatures._jvmDataset.jrdd().count())


    def test_round_trip_interval_list(self):

        testFile = self.resourceFile("SeqCap_EZ_Exome_v3.hg19.interval_list")
        ac = ADAMContext(self.ss)
        
        features = ac.loadFeatures(testFile)
        tmpPath = self.tmpFile() + ".interval_list"
        features.save(tmpPath,
                      asSingleFile=True)

        savedFeatures = ac.loadFeatures(testFile)

        self.assertEqual(features._jvmDataset.jrdd().count(),
                          savedFeatures._jvmDataset.jrdd().count())


    def test_transform(self):

        featurePath = self.resourceFile("gencode.v7.annotation.trunc10.bed")
        ac = ADAMContext(self.ss)

        features = ac.loadFeatures(featurePath)

        transformedFeatures = features.transform(lambda x: x.filter(x.start < 12613))

        self.assertEqual(transformedFeatures.toDF().count(), 6)
