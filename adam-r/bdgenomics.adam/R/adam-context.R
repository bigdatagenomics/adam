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

javaStringency <- function(stringency) {
    stringency <- sparkR.callJStatic("htsjdk.samtools.ValidationStringency",
                                     "valueOf",
                                     stringency)
}


#' Load alignment records into an AlignmentRecordRDD.
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
#' @param stringency The validation stringency to apply. Defaults to STRICT.
#' @return Returns an RDD containing reads.
#'
#' @export
setMethod("loadAlignments",
          signature(ac = "ADAMContext", filePath = "character"),
          function(ac, filePath, stringency = "STRICT") {
              jStringency <- javaStringency(stringency)
              jrdd <- sparkR.callJMethod(ac@jac,
                                         "loadAlignments",
                                         filePath, jStringency)
              AlignmentRecordRDD(jrdd)
          })

#' Load nucleotide contig fragments into a NucleotideContigFragmentRDD.
#'
#' If the path name has a .fa/.fasta extension, load as FASTA format.
#' Else, fall back to Parquet + Avro.
#'
#' For FASTA format, compressed files are supported through compression codecs configured
#' in Hadoop, which by default include .gz and .bz2, but can include more.
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

#' Load fragments into a FragmentRDD.
#'
#' Loads path names ending in:
#' * .bam/.cram/.sam as BAM/CRAM/SAM format and
#' * .ifq as interleaved FASTQ format.
#'
#' If none of these match, fall back to Parquet + Avro.
#'
#' For interleaved FASTQ format, compressed files are supported through compression codecs
#' configured in Hadoop, which by default include .gz and .bz2, but can include more.
#'
#' @param ac The ADAMContext.
#' @param filePath The path to load the file from.
#' @param stringency The validation stringency to apply. Defaults to STRICT.
#' @return Returns an RDD containing sequence fragments.
#'
#' @export
setMethod("loadFragments",
          signature(ac = "ADAMContext", filePath = "character"),
          function(ac, filePath, stringency) {
              jStringency <- javaStringency(stringency)
              jrdd <- sparkR.callJMethod(ac@jac,
                                         "loadFragments",
                                         filePath,
                                         jStringency)
              FragmentRDD(jrdd)
          })

#' Load features into a FeatureRDD.
#'
#' Loads path names ending in:
#' * .bed as BED6/12 format,
#' * .gff3 as GFF3 format,
#' * .gtf/.gff as GTF/GFF2 format,
#' * .narrow[pP]eak as NarrowPeak format, and
#' * .interval_list as IntervalList format.
#'
#' If none of these match, fall back to Parquet + Avro.
#'
#' For BED6/12, GFF3, GTF/GFF2, NarrowPeak, and IntervalList formats, compressed files
#' are supported through compression codecs configured in Hadoop, which by default include
#' .gz and .bz2, but can include more.
#'
#' @param ac The ADAMContext.
#' @param filePath The path to load the file from.
#' @param stringency The validation stringency to apply. Defaults to STRICT.
#' @return Returns an RDD containing features.
#'
#' @export
setMethod("loadFeatures",
          signature(ac = "ADAMContext", filePath = "character"),
          function(ac, filePath, stringency = "STRICT") {
              jStringency <- javaStringency(stringency)
              jrdd <- sparkR.callJMethod(ac@jac,
                                         "loadFeatures",
                                         filePath,
                                         jStringency)
              FeatureRDD(jrdd)
          })

#' Load features into a FeatureRDD and convert to a CoverageRDD.
#' Coverage is stored in the score field of Feature.
#'
#' Loads path names ending in:
#' * .bed as BED6/12 format,
#' * .gff3 as GFF3 format,
#' * .gtf/.gff as GTF/GFF2 format,
#' * .narrow[pP]eak as NarrowPeak format, and
#' * .interval_list as IntervalList format.
#'
#' If none of these match, fall back to Parquet + Avro.
#'
#' For BED6/12, GFF3, GTF/GFF2, NarrowPeak, and IntervalList formats, compressed files
#' are supported through compression codecs configured in Hadoop, which by default include
#' .gz and .bz2, but can include more.
#'
#' @param ac The ADAMContext.
#' @param filePath The path to load the file from.
#' @param stringency The validation stringency to apply. Defaults to STRICT.
#' @return Returns an RDD containing coverage.
#'
#' @export
setMethod("loadCoverage",
          signature(ac = "ADAMContext", filePath = "character"),
          function(ac, filePath, stringency = "STRICT") {
              jStringency <- javaStringency(stringency)
              jrdd <- sparkR.callJMethod(ac@jac,
                                         "loadCoverage",
                                         filePath,
                                         jStringency)
              CoverageRDD(jrdd)
          })

#' Load genotypes into a GenotypeRDD.
#'
#' If the path name has a .vcf/.vcf.gz/.vcf.bgz extension, load as VCF format.
#' Else, fall back to Parquet + Avro.
#'
#' @param ac The ADAMContext.
#' @param filePath The path to load the file from.
#' @param stringency The validation stringency to apply. Defaults to STRICT.
#' @return Returns an RDD containing genotypes.
#'
#' @export
setMethod("loadGenotypes",
          signature(ac = "ADAMContext", filePath = "character"),
          function(ac, filePath, stringency = "STRICT") {
              jStringency <- javaStringency(stringency)
              jrdd <- sparkR.callJMethod(ac@jac,
                                         "loadGenotypes",
                                         filePath,
                                         jStringency)
              GenotypeRDD(jrdd)
          })

#' Load variants into a VariantRDD.
#'
#' If the path name has a .vcf/.vcf.gz/.vcf.bgz extension, load as VCF format.
#' Else, fall back to Parquet + Avro.
#'
#' @param ac The ADAMContext.
#' @param filePath The path to load the file from.
#' @param stringency The validation stringency to apply. Defaults to STRICT.
#' @return Returns an RDD containing variants.
#'
#' @export
setMethod("loadVariants",
          signature(ac = "ADAMContext", filePath = "character"),
          function(ac, filePath, stringency = "STRICT") {
              jStringency <- javaStringency(stringency)
              jrdd <- sparkR.callJMethod(ac@jac,
                                         "loadVariants",
                                         filePath,
                                         jStringency)
              VariantRDD(jrdd)
          })
