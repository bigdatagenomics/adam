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
library(bdgenomics.adam)

context("manipulating Alignments")

ac <- createADAMContext()

test_that("save sorted sam", {

    originalReads <- resourceFile("sorted.sam")
    reads <- loadAlignments(ac, originalReads)
    tmpPath <- tempfile(fileext = ".sam")
    sortedReads <- sortByReferencePosition(reads)
    saveAsSam(reads, tmpPath, isSorted = TRUE, asSingleFile = TRUE)

    expect_files_match(tmpPath, originalReads)
})

test_that("save unordered sam", {

    originalReads <- resourceFile("unordered.sam")
    reads <- loadAlignments(ac, originalReads)
    tmpPath <- tempfile(fileext = ".sam")
    sortedReads <- sortByReferencePosition(reads)
    saveAsSam(reads, tmpPath, asSingleFile = TRUE)

    expect_files_match(tmpPath, originalReads)
})

test_that("save as bam", {

    originalReads <- resourceFile("sorted.sam")
    reads <- loadAlignments(ac, originalReads)
    tmpPath <- tempfile(fileext = ".bam")
    sortedReads <- sortByReferencePosition(reads)
    saveAsSam(reads, tmpPath, isSorted = TRUE, asSingleFile = TRUE)

    bam <- loadAlignments(ac, tmpPath)
    readsDf <- toDF(reads)
    bamDf <- toDF(bam)

    expect_equal(count(readsDf), count(bamDf))
})

test_that("count k-mers", {

    originalReads <- resourceFile("small.sam")
    reads <- loadAlignments(ac, originalReads)
    kmers <- countKmers(reads, 6)

    expect_equal(count(kmers), 1040)
})

test_that("pipe as sam", {

    reads12Path <- resourceFile("reads12.sam")
    reads <- loadAlignments(ac, reads12Path)
    cmd <- list("tee", "/dev/null")

    pipedRdd <- pipe(reads,
                     cmd=cmd,
                     "org.bdgenomics.adam.ds.read.SAMInFormatter",
                     "org.bdgenomics.adam.ds.read.AnySAMOutFormatter",
                     "org.bdgenomics.adam.api.java.AlignmentsToAlignmentsConverter")

    expect_equal(count(toDF(reads)),
                 count(toDF(pipedRdd)))
})

test_that("transform dataframe of reads", {

    readsPath <- resourceFile("unsorted.sam")
    reads <- loadAlignments(ac, readsPath)

    transformedReads = transform(reads, function(df) {
        filter(df, df$referenceName == "1")
    })

    expect_equal(count(toDF(transformedReads)), 1)
})

test_that("transmute to coverage", {
    readsPath <- resourceFile("unsorted.sam")
    reads <- loadAlignments(ac, readsPath)

    readsAsCoverage = transmute(reads, function(df) {
        select(df, df$referenceName, df$start, df$end, alias(cast(df$mappingQuality, "double"), "count"), alias(cast(df$readGroupSampleId, "string"), "optSampleId"))
    }, "CoverageDataset")

    expect_true(is(readsAsCoverage, "CoverageDataset"))
    expect_equal(count(toDF(readsAsCoverage)), 5)
})

test_that("reads can cache", {

    readsPath <- resourceFile("unsorted.sam")
    reads <- loadAlignments(ac, readsPath)

    cache(reads)
    unpersist(reads)
})

test_that("reads can persist with storage level", {

    readsPath <- resourceFile("unsorted.sam")
    reads <- loadAlignments(ac, readsPath)
    storageLevel <- "MEMORY_AND_DISK"

    persist(reads, storageLevel)
    unpersist(reads)
})

test_that("broadcast inner join against targets", {

    readsPath <- resourceFile("small.1.sam")
    targetsPath <- resourceFile("small.1.bed")

    reads <- loadAlignments(ac, readsPath)
    targets <- loadFeatures(ac, targetsPath)

    jRdd <- broadcastRegionJoin(reads, targets)

    expect_equal(count(toDF(jRdd)), 5)
})

test_that("broadcast right outer join against targets", {

    readsPath <- resourceFile("small.1.sam")
    targetsPath <- resourceFile("small.1.bed")

    reads <- loadAlignments(ac, readsPath)
    targets <- loadFeatures(ac, targetsPath)

    jRdd <- rightOuterBroadcastRegionJoin(reads, targets)

    expect_equal(count(toDF(jRdd)), 6)
})

test_that("shuffle inner join against targets", {

    readsPath <- resourceFile("small.1.sam")
    targetsPath <- resourceFile("small.1.bed")

    reads <- loadAlignments(ac, readsPath)
    targets <- loadFeatures(ac, targetsPath)

    jRdd <- shuffleRegionJoin(reads, targets)

    expect_equal(count(toDF(jRdd)), 5)
})

test_that("shuffle right outer join against targets", {

    readsPath <- resourceFile("small.1.sam")
    targetsPath <- resourceFile("small.1.bed")

    reads <- loadAlignments(ac, readsPath)
    targets <- loadFeatures(ac, targetsPath)

    jRdd <- rightOuterShuffleRegionJoin(reads, targets)

    expect_equal(count(toDF(jRdd)), 6)
})

test_that("shuffle left outer join against targets", {

    readsPath <- resourceFile("small.1.sam")
    targetsPath <- resourceFile("small.1.bed")

    reads <- loadAlignments(ac, readsPath)
    targets <- loadFeatures(ac, targetsPath)

    jRdd <- leftOuterShuffleRegionJoin(reads, targets)

    expect_equal(count(toDF(jRdd)), 20)
})

test_that("shuffle full outer join against targets", {

    readsPath <- resourceFile("small.1.sam")
    targetsPath <- resourceFile("small.1.bed")

    reads <- loadAlignments(ac, readsPath)
    targets <- loadFeatures(ac, targetsPath)

    jRdd <- fullOuterShuffleRegionJoin(reads, targets)

    expect_equal(count(toDF(jRdd)), 21)
})

test_that("shuffle inner join and groupBy against targets", {

    readsPath <- resourceFile("small.1.sam")
    targetsPath <- resourceFile("small.1.bed")

    reads <- loadAlignments(ac, readsPath)
    targets <- loadFeatures(ac, targetsPath)

    jRdd <- shuffleRegionJoinAndGroupByLeft(reads, targets)

    expect_equal(count(toDF(jRdd)), 5)
})

test_that("shuffle right outer join and groupBy against targets", {

    readsPath <- resourceFile("small.1.sam")
    targetsPath <- resourceFile("small.1.bed")

    reads <- loadAlignments(ac, readsPath)
    targets <- loadFeatures(ac, targetsPath)

    jRdd <- rightOuterShuffleRegionJoinAndGroupByLeft(reads, targets)

    expect_equal(count(toDF(jRdd)), 21)
})
