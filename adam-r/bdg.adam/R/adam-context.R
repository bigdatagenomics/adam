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
library(SparkR)

setOldClass("jobj")

#' @title Class that represents an ADAMContext.
#' @description The ADAMContext provides helper methods for loading in genomic
#'              data into a Spark RDD/Dataframe.
#' @slot jac Java object reference to the backing JavaADAMContext.
#' @export
setClass("ADAMContext",
         slots = list(jac = "jobj"))

#' @export
ADAMContext <- function(ss) {
    ssc = sparkR.callJMethod(ss, "sparkContext")
    ac = sparkR.newJObject("org.bdgenomics.adam.rdd.ADAMContext", ssc)
    jac = sparkR.newJObject("org.bdgenomics.adam.api.java.JavaADAMContext", ac)
    
    new("ADAMContext", jac = jac)
}

#' Loads in an ADAM read file. This method can load SAM, BAM, and ADAM files.
#'
#' Loads path names ending in:
#' * .bam/.cram/.sam as BAM/CRAM/SAM format,
#' * .fa/.fasta as FASTA format,
#' * .fq/.fastq as FASTQ format, and
#' * .ifq as interleaved FASTQ format.
#'
#' If none of these match, fall back to Parquet + Avro.
#'
#' For FASTA, FASTQ, and interleaved FASTQ formats, compressed files are supported
#' through compression codecs configured in Hadoop, which by default include .gz and .bz2,
#' but can include more.
#'
#' @param ac The ADAMContext.
#' @param filePath The path to load the file from.
#' @return Returns an RDD containing reads.
#'
#' @export
setMethod("loadAlignments",
          signature(ac = "ADAMContext", filePath = "character"),
          function(ac, filePath) {
              jrdd <- sparkR.callJMethod(ac@jac, "loadAlignments", filePath)
              AlignmentRecordRDD(jrdd)
          })

#' Loads in sequence fragments.
#'
#' Can load from FASTA or from Parquet encoded NucleotideContigFragments.
#'
#' @param ac The ADAMContext.
#' @param filePath The path to load the file from.
#' @return Returns an RDD containing sequence fragments.
#'
#' @export
setMethod("loadContigFragments",
          signature(ac = "ADAMContext", filePath = "character"),
          function(ac, filePath) {
              jrdd <- sparkR.callJMethod(ac@jac, "loadContigFragments", filePath)
              NucleotideContigFragmentRDD(jrdd)
          })

#' Loads in read pairs as fragments.
#'
#' @param ac The ADAMContext.
#' @param filePath The path to load the file from.
#' @return Returns an RDD containing sequence fragments.
#'
#' @export
setMethod("loadFragments",
          signature(ac = "ADAMContext", filePath = "character"),
          function(ac, filePath) {
              jrdd <- sparkR.callJMethod(ac@jac, "loadFragments", filePath)
              FragmentRDD(jrdd)
          })

#' Loads in genomic features.
#'
#' @param ac The ADAMContext.
#' @param filePath The path to load the file from.
#' @return Returns an RDD containing features.
#'
#' @export
setMethod("loadFeatures",
          signature(ac = "ADAMContext", filePath = "character"),
          function(ac, filePath) {
              jrdd <- sparkR.callJMethod(ac@jac, "loadFeatures", filePath)
              FeatureRDD(jrdd)
          })

#' Loads in genomic features as coverage counts.
#'
#' @param ac The ADAMContext.
#' @param filePath The path to load the file from.
#' @return Returns an RDD containing coverage.
#'
#' @export
setMethod("loadCoverage",
          signature(ac = "ADAMContext", filePath = "character"),
          function(ac, filePath) {
              jrdd <- sparkR.callJMethod(ac@jac, "loadCoverage", filePath)
              CoverageRDD(jrdd)
          })

#' Loads in genotypes.
#'
#' @param ac The ADAMContext.
#' @param filePath The path to load the file from.
#' @return Returns an RDD containing genotypes.
#'
#' @export
setMethod("loadGenotypes",
          signature(ac = "ADAMContext", filePath = "character"),
          function(ac, filePath) {
              jrdd <- sparkR.callJMethod(ac@jac, "loadGenotypes", filePath)
              GenotypeRDD(jrdd)
          })

#' Loads in variants.
#'
#' @param ac The ADAMContext.
#' @param filePath The path to load the file from.
#' @return Returns an RDD containing variants.
#'
#' @export
setMethod("loadVariants",
          signature(ac = "ADAMContext", filePath = "character"),
          function(ac, filePath) {
              jrdd <- sparkR.callJMethod(ac@jac, "loadVariants", filePath)
              VariantRDD(jrdd)
          })
