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
from bdgenomics.adam.models import ReferenceRegion
from bdgenomics.adam.test import SparkTestCase


class ADAMContextTest(SparkTestCase):


    def test_load_alignments(self):
        
        testFile = self.resourceFile("small.sam")
        ac = ADAMContext(self.ss)
        
        reads = ac.loadAlignments(testFile)

        self.assertEqual(reads.toDF().count(), 20)
        self.assertEqual(reads._jvmDataset.jrdd().count(), 20)


    def test_load_indexed_bam(self):

        testFile = self.resourceFile("indexed_bams/sorted.bam")
        ac = ADAMContext(self.ss)

        reads = ac.loadIndexedBam(testFile,
                                  [ReferenceRegion("chr2", 100, 101),
                                   ReferenceRegion("3", 10, 17)])

        self.assertEqual(reads.toDF().count(), 2)
        

    def test_load_gtf(self):

        testFile = self.resourceFile("Homo_sapiens.GRCh37.75.trun20.gtf")
        ac = ADAMContext(self.ss)
        
        reads = ac.loadFeatures(testFile)

        self.assertEqual(reads.toDF().count(), 15)
        self.assertEqual(reads._jvmDataset.jrdd().count(), 15)


    def test_load_bed(self):

        testFile = self.resourceFile("gencode.v7.annotation.trunc10.bed")
        ac = ADAMContext(self.ss)
        
        reads = ac.loadFeatures(testFile)

        self.assertEqual(reads.toDF().count(), 10)
        self.assertEqual(reads._jvmDataset.jrdd().count(), 10)


    def test_load_narrowPeak(self):

        
        testFile = self.resourceFile("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
        ac = ADAMContext(self.ss)
        
        reads = ac.loadFeatures(testFile)

        self.assertEqual(reads.toDF().count(), 10)
        self.assertEqual(reads._jvmDataset.jrdd().count(), 10)


    def test_load_interval_list(self):

        testFile = self.resourceFile("SeqCap_EZ_Exome_v3.hg19.interval_list")
        ac = ADAMContext(self.ss)
        
        reads = ac.loadFeatures(testFile)

        self.assertEqual(reads.toDF().count(), 369)
        self.assertEqual(reads._jvmDataset.jrdd().count(), 369)


    def test_load_coverage(self):


        testFile = self.resourceFile("sample_coverage.bed")
        ac = ADAMContext(self.ss)

        coverage = ac.loadCoverage(testFile)

        self.assertEqual(coverage.toDF().count(), 3)
        

    def test_load_genotypes(self):

        
        testFile = self.resourceFile("small.vcf")
        ac = ADAMContext(self.ss)
        
        reads = ac.loadGenotypes(testFile)

        self.assertEqual(reads.toDF().count(), 18)
        self.assertEqual(reads._jvmDataset.jrdd().count(), 18)
        

    def test_load_variants(self):

        
        testFile = self.resourceFile("small.vcf")
        ac = ADAMContext(self.ss)
        
        reads = ac.loadVariants(testFile)

        self.assertEqual(reads.toDF().count(), 6)
        self.assertEqual(reads._jvmDataset.jrdd().count(), 6)


    def test_load_slices(self):


        testFile = self.resourceFile("HLA_DQB1_05_01_01_02.fa")
        ac = ADAMContext(self.ss)
        
        slices = ac.loadSlices(testFile, 10000)

        self.assertEqual(slices.toDF().count(), 1)
        self.assertEqual(slices._jvmDataset.jrdd().count(), 1)


    def test_load_dna_sequences(self):


        testFile = self.resourceFile("HLA_DQB1_05_01_01_02.fa")
        ac = ADAMContext(self.ss)

        sequences = ac.loadDnaSequences(testFile)

        self.assertEqual(sequences.toDF().count(), 1)
        self.assertEqual(sequences._jvmDataset.jrdd().count(), 1)
