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

context("manipulating features")

ac <- createADAMContext()

test_that("round trip gtf", {
    testFile <- resourceFile("Homo_sapiens.GRCh37.75.trun20.gtf")
    features <- loadFeatures(ac, testFile)
    tmpPath <- tempfile(fileext = ".gtf")
    save(features, tmpPath, asSingleFile = TRUE)

    expect_equal(count(toDF(features)), count(toDF(loadFeatures(ac, tmpPath))))
})

test_that("round trip bed", {
    testFile <- resourceFile("gencode.v7.annotation.trunc10.bed")
    features <- loadFeatures(ac, testFile)
    tmpPath <- tempfile(fileext = ".bed")
    save(features, tmpPath, asSingleFile = TRUE)

    expect_equal(count(toDF(features)), count(toDF(loadFeatures(ac, tmpPath))))
})

test_that("round trip narrowpeak", {
    testFile <- resourceFile("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    features <- loadFeatures(ac, testFile)
    tmpPath <- tempfile(fileext = ".narrowPeak")
    save(features, tmpPath, asSingleFile = TRUE)

    expect_equal(count(toDF(features)), count(toDF(loadFeatures(ac, tmpPath))))
})

test_that("round trip interval list", {
    testFile <- resourceFile("SeqCap_EZ_Exome_v3.hg19.interval_list")
    features <- loadFeatures(ac, testFile)
    tmpPath <- tempfile(fileext = ".interval_list")
    save(features, tmpPath, asSingleFile = TRUE)

    expect_equal(count(toDF(features)), count(toDF(loadFeatures(ac, tmpPath))))
})
