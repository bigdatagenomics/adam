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
library(bdg.adam)

context("manipulating alignmentrecords")

sc <- sparkR.session()
ac <- ADAMContext(sc)

test_that("save sorted sam", {

    originalReads <- resourceFile("sorted.sam")
    reads <- loadAlignments(ac, originalReads)
    tmpPath <- tempfile(fileext = ".sam")
    sortedReads <- sortReadsByReferencePosition(reads)
    saveAsSam(reads, tmpPath, isSorted = TRUE, asSingleFile = TRUE)

    expect_files_match(tmpPath, originalReads)
})

test_that("save unordered sam", {

    originalReads <- resourceFile("unordered.sam")
    reads <- loadAlignments(ac, originalReads)
    tmpPath <- tempfile(fileext = ".sam")
    sortedReads <- sortReadsByReferencePosition(reads)
    saveAsSam(reads, tmpPath, asSingleFile = TRUE)

    expect_files_match(tmpPath, originalReads)
})

test_that("save as bam", {

    originalReads <- resourceFile("sorted.sam")
    reads <- loadAlignments(ac, originalReads)
    tmpPath <- tempfile(fileext = ".bam")
    sortedReads <- sortReadsByReferencePosition(reads)
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

    pipedRdd <- pipe(reads,
                     "tee /dev/null",
                     "org.bdgenomics.adam.rdd.read.SAMInFormatter",
                     "org.bdgenomics.adam.rdd.read.AnySAMOutFormatter",
                     "org.bdgenomics.adam.api.java.AlignmentRecordsToAlignmentRecordsConverter")

    expect_equal(count(toDF(reads)),
                 count(toDF(pipedRdd)))
})
