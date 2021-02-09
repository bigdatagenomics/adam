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
from bdgenomics.adam.ds import AlignmentDataset, CoverageDataset
from bdgenomics.adam.test import SparkTestCase

from pyspark.sql.types import DoubleType
from  pyspark.storagelevel import StorageLevel

class AlignmentDatasetTest(SparkTestCase):


    def test_save_sorted_sam(self):

        testFile = self.resourceFile("sorted.sam")
        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(testFile)
        tmpPath = self.tmpFile() + ".sam"
        sortedReads = reads.sortByReferencePosition()
        sortedReads.saveAsSam(tmpPath,
                              isSorted=True,
                              asSingleFile=True)

        self.checkFiles(testFile, tmpPath)


    def test_save_unordered_sam(self):

        testFile = self.resourceFile("unordered.sam")
        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(testFile)
        tmpPath = self.tmpFile() + ".sam"
        reads.saveAsSam(tmpPath,
                        asSingleFile=True)

        self.checkFiles(testFile, tmpPath)


    def test_union(self):

        testFile1 = self.resourceFile("sorted.sam")
        testFile2 = self.resourceFile("unordered.sam")
        ac = ADAMContext(self.ss)

        reads1 = ac.loadAlignments(testFile1)
        reads2 = ac.loadAlignments(testFile2)

        unionReads = reads1.union([reads2])

        self.assertEqual(unionReads.toDF().count(), 13)


    def test_save_as_bam(self):

        testFile = self.resourceFile("sorted.sam")
        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(testFile)
        tmpPath = self.tmpFile() + ".bam"
        reads.saveAsSam(tmpPath,
                        isSorted=True,
                        asSingleFile=True)

        bamReads = ac.loadAlignments(tmpPath)

        self.assertEqual(bamReads._jvmDataset.jrdd().count(),
                          reads._jvmDataset.jrdd().count())


    def test_count_kmers(self):

        testFile = self.resourceFile("small.sam")
        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(testFile)
        kmers = reads.countKmers(6)

        self.assertEqual(kmers.count(), 1040)


    def test_pipe_as_sam(self):

        reads12Path = self.resourceFile("reads12.sam")
        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(reads12Path)

        pipedRdd = reads.pipe(["tee", "/dev/null"],
                              "org.bdgenomics.adam.ds.read.SAMInFormatter",
                              "org.bdgenomics.adam.ds.read.AnySAMOutFormatter",
                              "org.bdgenomics.adam.api.java.AlignmentsToAlignmentsConverter")

        self.assertEqual(reads.toDF().count(), pipedRdd.toDF().count())


    def test_transform(self):

        readsPath = self.resourceFile("unsorted.sam")
        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(readsPath)

        transformedReads = reads.transform(lambda x: x.filter(x.referenceName == "1"))

        self.assertEqual(transformedReads.toDF().count(), 1)


    def test_transmute_to_coverage(self):

        readsPath = self.resourceFile("unsorted.sam")
        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(readsPath)

        readsAsCoverage = reads.transmute(lambda x: x.select(x.referenceName,
                                                             x.start,
                                                             x.end,
                                                             x.mappingQuality.cast(DoubleType()).alias("count"),
                                                             x.readGroupSampleId.alias("optSampleId")),
                                        CoverageDataset)

        assert(isinstance(readsAsCoverage, CoverageDataset))
        self.assertEquals(readsAsCoverage.toDF().count(), 5)


    def test_to_coverage(self):

        readsPath = self.resourceFile("unsorted.sam")
        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(readsPath)

        coverage = reads.toCoverage()
        self.assertEqual(coverage.toDF().count(), 5)

        # 5 reads: contig 3 has 8 bp, chr2 (2 strands) has 20bp, contig 4 has 8bp, contig 1 has 10 bp
        # 8 + 20 + 8 + 10 = 46
        coverage = reads.toCoverage(collapse = False)
        self.assertEqual(coverage.toDF().count(), 46)


    def test_to_fragments(self):

        readsPath = self.resourceFile("unsorted.sam")
        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(readsPath)

        fragments = reads.toFragments()
        self.assertEqual(fragments.toDF().count(), 5)


    def test_load_indexed_bam(self):

        readsPath = self.resourceFile("indexed_bams/sorted.bam")
        ac = ADAMContext(self.ss)

        querys = [ReferenceRegion("chr2", 100, 101), ReferenceRegion("3", 10, 17)]

        reads = ac.loadIndexedBam(readsPath, querys)

        self.assertEqual(reads.toDF().count(), 2)


    def test_filterByOverlappingRegion(self):

        readsPath = self.resourceFile("unsorted.sam")
        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(readsPath)

        query = ReferenceRegion("chr2", 1, 400)

        filtered = reads.filterByOverlappingRegion(query)
        self.assertEqual(filtered.toDF().count(), 1)


    def test_filterByOverlappingRegions(self):

        readsPath = self.resourceFile("unsorted.sam")
        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(readsPath)

        querys = [ReferenceRegion("chr2", 1, 400),
                    ReferenceRegion("3", 1, 100)]

        filtered = reads.filterByOverlappingRegions(querys)
        self.assertEqual(filtered.toDF().count(), 2)


    def test_caching(self):

        readsPath = self.resourceFile("unsorted.sam")
        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(readsPath)

        cachedReads = reads.cache()
        cached = self.sc._jsc.getPersistentRDDs()
        self.assertEqual(cached.isEmpty(), False)

        cachedReads.unpersist()
        cached = self.sc._jsc.getPersistentRDDs()
        self.assertEqual(cached.isEmpty(), True)


    def test_persisting(self):

        readsPath = self.resourceFile("unsorted.sam")
        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(readsPath)

        persistedReads = reads.persist(StorageLevel.DISK_ONLY)
        cached = self.sc._jsc.getPersistentRDDs()
        self.assertEqual(cached.isEmpty(), False)

        persistedReads.unpersist()
        cached = self.sc._jsc.getPersistentRDDs()
        self.assertEqual(cached.isEmpty(), True)


    def test_broadcast_inner_join(self):

        readsPath = self.resourceFile("small.1.sam")
        targetsPath = self.resourceFile("small.1.bed")

        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(readsPath)
        targets = ac.loadFeatures(targetsPath)

        jRdd = reads.broadcastRegionJoin(targets)

        self.assertEqual(jRdd.toDF().count(), 5)


    def test_broadcast_right_outer_join(self):

        readsPath = self.resourceFile("small.1.sam")
        targetsPath = self.resourceFile("small.1.bed")

        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(readsPath)
        targets = ac.loadFeatures(targetsPath)

        jRdd = reads.rightOuterBroadcastRegionJoin(targets)

        self.assertEqual(jRdd.toDF().count(), 6)


    def test_shuffle_inner_join(self):

        readsPath = self.resourceFile("small.1.sam")
        targetsPath = self.resourceFile("small.1.bed")

        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(readsPath)
        targets = ac.loadFeatures(targetsPath)

        jRdd = reads.shuffleRegionJoin(targets)

        self.assertEqual(jRdd.toDF().count(), 5)


    def test_shuffle_right_outer_join(self):

        readsPath = self.resourceFile("small.1.sam")
        targetsPath = self.resourceFile("small.1.bed")

        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(readsPath)
        targets = ac.loadFeatures(targetsPath)

        jRdd = reads.rightOuterShuffleRegionJoin(targets)

        self.assertEqual(jRdd.toDF().count(), 6)


    def test_shuffle_left_outer_join(self):

        readsPath = self.resourceFile("small.1.sam")
        targetsPath = self.resourceFile("small.1.bed")

        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(readsPath)
        targets = ac.loadFeatures(targetsPath)

        jRdd = reads.leftOuterShuffleRegionJoin(targets)

        self.assertEqual(jRdd.toDF().count(), 20)


    def test_shuffle_full_outer_join(self):

        readsPath = self.resourceFile("small.1.sam")
        targetsPath = self.resourceFile("small.1.bed")

        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(readsPath)
        targets = ac.loadFeatures(targetsPath)

        jRdd = reads.fullOuterShuffleRegionJoin(targets)

        self.assertEqual(jRdd.toDF().count(), 21)


    def test_shuffle_inner_join_groupBy_left(self):

        readsPath = self.resourceFile("small.1.sam")
        targetsPath = self.resourceFile("small.1.bed")

        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(readsPath)
        targets = ac.loadFeatures(targetsPath)

        jRdd = reads.shuffleRegionJoinAndGroupByLeft(targets)

        self.assertEqual(jRdd.toDF().count(), 5)


    def test_shuffle_right_outer_join_groupBy_left(self):

        readsPath = self.resourceFile("small.1.sam")
        targetsPath = self.resourceFile("small.1.bed")

        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(readsPath)
        targets = ac.loadFeatures(targetsPath)

        jRdd = reads.rightOuterShuffleRegionJoinAndGroupByLeft(targets)

        self.assertEqual(jRdd.toDF().count(), 21)


    def test_realignIndels_reads(self):

        readsPath = self.resourceFile("small.1.sam")

        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(readsPath)
        realigned = reads.realignIndels()

        self.assertEqual(realigned.toDF().count(), 20)

    def test_realignIndels_known_indels(self):

        readsPath = self.resourceFile("small.1.sam")
        variantsPath = self.resourceFile("small.vcf")

        ac = ADAMContext(self.ss)

        reads = ac.loadAlignments(readsPath)
        knownIndels = ac.loadVariants(variantsPath)

        realigned = reads.realignIndelsFromKnownIndels(knownIndels)

        self.assertEqual(realigned.toDF().count(), 20)
