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


class AlignmentRecordRDDTest(SparkTestCase):

    
    def test_save_sorted_sam(self):

        testFile = self.resourceFile("sorted.sam")
        ac = ADAMContext(self.sc)
        
        reads = ac.loadAlignments(testFile)
        tmpPath = self.tmpFile() + ".sam"
        sortedReads = reads.sortReadsByReferencePosition()
        sortedReads.saveAsSam(tmpPath,
                              isSorted=True,
                              asSingleFile=True)
        
        self.checkFiles(testFile, tmpPath)


    def test_save_unordered_sam(self):

        testFile = self.resourceFile("unordered.sam")
        ac = ADAMContext(self.sc)
        
        reads = ac.loadAlignments(testFile)
        tmpPath = self.tmpFile() + ".sam"
        reads.saveAsSam(tmpPath,
                        asSingleFile=True)
        
        self.checkFiles(testFile, tmpPath)


    def test_union(self):

        testFile1 = self.resourceFile("sorted.sam")
        testFile2 = self.resourceFile("unordered.sam")
        ac = ADAMContext(self.sc)

        reads1 = ac.loadAlignments(testFile1)
        reads2 = ac.loadAlignments(testFile2)

        unionReads = reads1.union([reads2])

        self.assertEqual(unionReads.toDF().count(), 13)
        

    def test_save_as_bam(self):

        testFile = self.resourceFile("sorted.sam")
        ac = ADAMContext(self.sc)
        
        reads = ac.loadAlignments(testFile)
        tmpPath = self.tmpFile() + ".bam"
        reads.saveAsSam(tmpPath,
                        isSorted=True,
                        asSingleFile=True)

        bamReads = ac.loadAlignments(tmpPath)
        
        self.assertEquals(bamReads._jvmRdd.jrdd().count(),
                          reads._jvmRdd.jrdd().count())


    def test_count_kmers(self):

        testFile = self.resourceFile("small.sam")
        ac = ADAMContext(self.sc)
        
        reads = ac.loadAlignments(testFile)
        kmers = reads.countKmers(6)

        self.assertEquals(kmers.count(), 1040)
