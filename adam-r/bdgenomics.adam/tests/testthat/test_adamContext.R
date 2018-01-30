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
library(SparkR)

context("basic ADAM context functions")

ac <- createADAMContext()

test_that("load reads", {
    reads <- loadAlignments(ac, resourceFile("small.sam"))
    readDf <- toDF(reads)
    expect_equal(count(readDf), 20)
})

test_that("load features from GTF", {
    features <- loadFeatures(ac, resourceFile("Homo_sapiens.GRCh37.75.trun20.gtf"))
    featureDf <- toDF(features)
    expect_equal(count(featureDf), 15)
})

test_that("load features from BED", {
    features <- loadFeatures(ac, resourceFile("gencode.v7.annotation.trunc10.bed"))
    featureDf <- toDF(features)
    expect_equal(count(featureDf), 10)
})

test_that("load features from narrowpeak", {
    features <- loadFeatures(ac,
                             resourceFile("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak"))
    featureDf <- toDF(features)
    expect_equal(count(featureDf), 10)
})

test_that("load features from interval_list", {
    features <- loadFeatures(ac, resourceFile("SeqCap_EZ_Exome_v3.hg19.interval_list"))
    featureDf <- toDF(features)
    expect_equal(count(featureDf), 369)
})

test_that("load coverage", {
    coverage <- loadCoverage(ac, resourceFile("sample_coverage.bed"))
    coverageDf <- toDF(coverage)
    expect_equal(count(coverageDf), 3)
})

test_that("load genotypes from vcf", {
    genotypes <- loadGenotypes(ac, resourceFile("small.vcf"))
    genotypeDf <- toDF(genotypes)
    expect_equal(count(genotypeDf), 18)
})

test_that("load variants from vcf", {
    variants <- loadVariants(ac, resourceFile("small.vcf"))
    variantDf <- toDF(variants)
    expect_equal(count(variantDf), 6)
})

test_that("load fasta sequences", {
    sequences <- loadDnaSequences(ac, resourceFile("HLA_DQB1_05_01_01_02.fa"))
    sequencesDf <- toDF(sequences)
    expect_equal(count(sequencesDf), 1)
})

test_that("load fasta slices", {
    slices <- loadSlices(ac, resourceFile("HLA_DQB1_05_01_01_02.fa"), 10000L)
    slicesDf <- toDF(slices)
    expect_equal(count(slicesDf), 1)
})
