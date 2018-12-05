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

context("manipulating genotypes")

ac <- createADAMContext()

test_that("round trip vcf", {
    testFile <- resourceFile("small.vcf")
    genotypes <- loadGenotypes(ac, testFile)
    tmpPath <- tempfile(fileext = ".vcf")
    saveAsVcf(toVariantContexts(genotypes), tmpPath)

    expect_equal(count(toDF(genotypes)), count(toDF(loadGenotypes(ac, tmpPath))))
})

test_that("save sorted vcf", {

    testFile <- resourceFile("random.vcf")
    genotypes <- loadGenotypes(ac, testFile)
    tmpPath <- tempfile(fileext = ".vcf")
    saveAsVcf(sort(toVariantContexts(genotypes)), tmpPath, asSingleFile = TRUE)

    truthFile <- resourceFile("sorted.vcf", submodule="adam-cli")
    expect_files_match(tmpPath, truthFile)
})

test_that("save lex sorted vcf", {

    testFile <- resourceFile("random.vcf")
    genotypes <- loadGenotypes(ac, testFile)
    tmpPath <- tempfile(fileext = ".vcf")
    saveAsVcf(sortLexicographically(toVariantContexts(genotypes)),
              tmpPath,
              asSingleFile = TRUE)

    truthFile <- resourceFile("sorted.lex.vcf", submodule="adam-cli")
    expect_files_match(tmpPath, truthFile)
})

test_that("convert genotypes to variants", {

    testFile <- resourceFile("small.vcf")
    genotypes <- loadGenotypes(ac, testFile)

    variants <- toVariants(genotypes)
    expect_equal(count(toDF(variants)), 18)

    variants <- toVariants(genotypes, dedupe=TRUE)
    expect_equal(count(toDF(variants)), 6)
})
