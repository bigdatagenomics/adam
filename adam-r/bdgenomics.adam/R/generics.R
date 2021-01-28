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

#### ADAM Context operations ####

#' The ADAMContext provides functions on top of a SparkContext for loading genomic data.
#' 
#' @name ADAMContext
NULL

#' @rdname ADAMContext
#' @param ac The ADAMContext.
#' @param filePath The path to load the file from.
#' @param ... additional argument(s).
#' @export
setGeneric("loadAlignments",
           function(ac, filePath, ...) { standardGeneric("loadAlignments") })

#' @rdname ADAMContext
#' @export
setGeneric("loadDnaSequences",
           function(ac, filePath) { standardGeneric("loadDnaSequences") })

#' @rdname ADAMContext
#' @export
setGeneric("loadProteinSequences",
           function(ac, filePath) { standardGeneric("loadProteinSequences") })

#' @rdname ADAMContext
#' @export
setGeneric("loadRnaSequences",
           function(ac, filePath) { standardGeneric("loadRnaSequences") })

#' @rdname ADAMContext
#' @export
setGeneric("loadSlices",
           function(ac, filePath, ...) { standardGeneric("loadSlices") })

#' @rdname ADAMContext
#' @export
setGeneric("loadFragments",
           function(ac, filePath, ...) { standardGeneric("loadFragments") })

#' @rdname ADAMContext
#' @export
setGeneric("loadFeatures",
           function(ac, filePath, ...) { standardGeneric("loadFeatures") })

#' @rdname ADAMContext
#' @export
setGeneric("loadCoverage",
           function(ac, filePath, ...) { standardGeneric("loadCoverage") })

#' @rdname ADAMContext
#' @export
setGeneric("loadGenotypes",
           function(ac, filePath, ...) { standardGeneric("loadGenotypes") })

#' @rdname ADAMContext
#' @export
setGeneric("loadVariants",
           function(ac, filePath, ...) { standardGeneric("loadVariants") })

#### Genomic dataset operations ####

#' The GenomicDataset is the base class that all genomic datatypes extend from in ADAM.
#' 
#' @name GenomicDataset
NULL

#' @rdname GenomicDataset
#' @param cmd The command to run.
#' @param tFormatter The name of the ADAM in-formatter class to use.
#' @param xFormatter The name of the ADAM out-formatter class to use.
#' @param convFn The name of the ADAM GenomicDataset conversion class to
#'   use.
#' @param ... additional argument(s).
#' @return Returns a new genomic dataset where the input from the original genomic dataset has
#'   been piped through a command that runs locally on each executor.
#' @export
setGeneric("pipe",
           function(ardd, cmd, tFormatter, xFormatter, convFn, ...) { standardGeneric("pipe") })

#' @rdname GenomicDataset
#' @export
setGeneric("toDF",
           function(ardd) { standardGeneric("toDF") })

setGeneric("replaceRdd",
           function(ardd, rdd) { standardGeneric("replaceRdd") })

setGeneric("wrapTransformation",
           function(ardd, tFn) { standardGeneric("wrapTransformation") })

#' @rdname GenomicDataset
#' @export
setGeneric("transform",
           function(ardd, tFn) { standardGeneric("transform") })

setGeneric("inferConversionFn",
           function(ardd, destClass) { standardGeneric("inferConversionFn") })

setGeneric("destClassSuffix",
           function(destClass) { standardGeneric("destClassSuffix") })

#' @rdname GenomicDataset
#' @param tFn A function that transforms the underlying DataFrame as a DataFrame.
#' @param destClass The destination class of this transmutation.
#' @export
setGeneric("transmute",
           function(ardd, tFn, destClass, ...) { standardGeneric("transmute") })

#' @rdname GenomicDataset
#' @export
setGeneric("save",
           function(ardd, filePath, ...) { standardGeneric("save") })

#' @rdname GenomicDataset
#' @export
setGeneric("sort",
           function(ardd) { standardGeneric("sort") })

#' @rdname GenomicDataset
#' @export
setGeneric("sortLexicographically",
           function(ardd) { standardGeneric("sortLexicographically") })

#' Saves this genomic dataset to disk as Parquet.
#'
#' @param ardd The genomic dataset to apply this to.
#' @param filePath Path to save file to.
#'
#' @rdname GenomicDataset
#'
#' @export
setGeneric("saveAsParquet",
           function(ardd, filePath) { standardGeneric("saveAsParquet") })

#### Caching operations ####
#' @rdname GenomicDataset
#' @export
setGeneric("cache",
           function(ardd) { standardGeneric("cache") })

#' @rdname GenomicDataset
#' @export
setGeneric("persist",
          function(ardd, sl) { standardGeneric("persist") })

#' @rdname GenomicDataset
#' @param sl the StorageLevel to persist in.
#' @export
setGeneric("unpersist",
           function(ardd, sl) { standardGeneric("unpersist") })

#### Region joins ####

#' @rdname GenomicDataset
#' @param genomicRdd The dataset to join against.
#' @param ... additional argument(s).
#' @export
setGeneric("broadcastRegionJoin",
           function(ardd, genomicRdd, ...) { 
               standardGeneric("broadcastRegionJoin")
           })

#' @rdname GenomicDataset
#' @param genomicRdd The dataset to join against.
#' @param ... additional argument(s).
#' @export
setGeneric("rightOuterBroadcastRegionJoin",
           function(ardd, genomicRdd, ...) { 
               standardGeneric("rightOuterBroadcastRegionJoin")
           })

#' @rdname GenomicDataset
#' @param genomicRdd The dataset to join against.
#' @param ... additional argument(s).
#' @export
setGeneric("broadcastRegionJoinAndGroupByRight",
           function(ardd, genomicRdd, ...) { 
               standardGeneric("broadcastRegionJoinAndGroupByRight")
           })

#' @rdname GenomicDataset
#' @param genomicRdd The dataset to join against.
#' @param ... additional argument(s).
#' @export
setGeneric("rightOuterBroadcastRegionJoinAndGroupByRight",
           function(ardd, genomicRdd, ...) { 
               standardGeneric("rightOuterBroadcastRegionJoinAndGroupByRight")
           })

#' @rdname GenomicDataset
#' @param genomicRdd The dataset to join against.
#' @param ... additional argument(s).
#' @export
setGeneric("shuffleRegionJoin",
           function(ardd, genomicRdd, ...) { 
               standardGeneric("shuffleRegionJoin")
           })

#' @rdname GenomicDataset
#' @param genomicRdd The dataset to join against.
#' @param ... additional argument(s).
#' @export
setGeneric("rightOuterShuffleRegionJoin",
           function(ardd, genomicRdd, ...) { 
               standardGeneric("rightOuterShuffleRegionJoin")
           })

#' @rdname GenomicDataset
#' @param genomicRdd The dataset to join against.
#' @param ... additional argument(s).
#' @export
setGeneric("leftOuterShuffleRegionJoin",
           function(ardd, genomicRdd, ...) { 
               standardGeneric("leftOuterShuffleRegionJoin")
           })

#' @rdname GenomicDataset
#' @param genomicRdd The dataset to join against.
#' @param ... additional argument(s).
#' @export
setGeneric("leftOuterShuffleRegionJoinAndGroupByLeft",
           function(ardd, genomicRdd, ...) { 
               standardGeneric("leftOuterShuffleRegionJoinAndGroupByLeft")
           })

#' @rdname GenomicDataset
#' @param genomicRdd The dataset to join against.
#' @param ... additional argument(s).
#' @export
setGeneric("fullOuterShuffleRegionJoin",
           function(ardd, genomicRdd, ...) { 
               standardGeneric("fullOuterShuffleRegionJoin")
           })

#' @rdname GenomicDataset
#' @param genomicRdd The dataset to join against.
#' @param ... additional argument(s).
#' @export
setGeneric("rightOuterShuffleRegionJoinAndGroupByLeft",
           function(ardd, genomicRdd, ...) { 
               standardGeneric("rightOuterShuffleRegionJoinAndGroupByLeft")
           })

#' @rdname GenomicDataset
#' @param genomicRdd The dataset to join against.
#' @param ... additional argument(s).
#' @export
setGeneric("shuffleRegionJoinAndGroupByLeft",
           function(ardd, genomicRdd, ...) { 
               standardGeneric("shuffleRegionJoinAndGroupByLeft")
           })

#### Alignment operations ####

#' The AlignmentDataset is the class used to manipulate genomic read data.
#' 
#' @name AlignmentDataset
NULL

#' @rdname AlignmentDataset
#' @export
setGeneric("toFragments",
           function(ardd) { standardGeneric("toFragments") })

#' @rdname AlignmentDataset
#' @param ardd The genomic dataset to apply this to.
#' @param ... additional argument(s).
#' @export
setGeneric("toCoverage",
           function(ardd, ...) { standardGeneric("toCoverage") })

#' @rdname AlignmentDataset
#' @param kmerLength The value of _k_ to use for cutting _k_-mers.
#' @export
setGeneric("countKmers",
           function(ardd, kmerLength) { standardGeneric("countKmers") })

#' @rdname AlignmentDataset
#' @param filePath The path to save the file to.
#' @export
setGeneric("saveAsSam",
           function(ardd, filePath, ...) { standardGeneric("saveAsSam") })

#' @rdname AlignmentDataset
#' @export
setGeneric("sortByReadName",
           function(ardd) { standardGeneric("sortByReadName") })

#' @rdname AlignmentDataset
#' @export
setGeneric("sortByReferencePosition",
           function(ardd) { standardGeneric("sortByReferencePosition") })

#' @rdname AlignmentDataset
#' @export
setGeneric("sortByReferencePositionAndIndex",
           function(ardd) { standardGeneric("sortByReferencePositionAndIndex") })

#' @rdname AlignmentDataset
#' @export
setGeneric("markDuplicates",
           function(ardd) { standardGeneric("markDuplicates") })

#' @rdname AlignmentDataset
#' @param knownSnps A table of known SNPs to mask valid variants.
#' @param validationStringency The stringency to apply towards validating BQSR.
#' @export
setGeneric("recalibrateBaseQualities",
           function(ardd, knownSnps, validationStringency) {
             standardGeneric("recalibrateBaseQualities")
           })

#' @rdname AlignmentDataset
#' @export
setGeneric("realignIndels",
           function(ardd, ...) { standardGeneric("realignIndels") })

#### Coverage operations ####

#' The CoverageDataset class is used to manipulate read coverage counts.
#' 
#' @name CoverageDataset
NULL

#' @rdname CoverageDataset
#' @param ... additional argument(s).
#' @export
setGeneric("collapse",
           function(ardd, ...) { standardGeneric("collapse") })

#' @rdname CoverageDataset
#' @export
setGeneric("toFeatures",
           function(ardd) { standardGeneric("toFeatures") })

#' @rdname CoverageDataset
#' @export
setGeneric("coverage",
           function(ardd, ...) { standardGeneric("coverage") })

#' @rdname CoverageDataset
#' @export
#' @aliases aggregatedCoverage,CoverageDataset-method
setGeneric("aggregatedCoverage",
           function(ardd, ...) { standardGeneric("aggregatedCoverage") })

#' @rdname CoverageDataset
#' @export
setGeneric("flatten",
           function(ardd) { standardGeneric("flatten") })

#### Fragment operations ####

#' The FragmentDataset class is used to manipulate paired reads.
#' 
#' @name FragmentDataset
NULL

#' @rdname FragmentDataset
#' @param ardd The genomic dataset to apply this to.
#' @export
setGeneric("toAlignments",
           function(ardd) { standardGeneric("toAlignments") })

#### Genotype and Variant operations ####

#' Converts this genomic dataset to VariantContexts.
#'
#' @param ardd The genomic dataset to apply this to.
#' @return Returns this genomic dataset of Variants as VariantContexts.
#' @export
setGeneric("toVariantContexts",
           function(ardd) { standardGeneric("toVariantContexts") })


#' Converts this genomic dataset to Variants.
#'
#' @param ardd The genomic dataset to apply this to.
#' @param ... additional argument(s).
#' @return Returns this genomic dataset of Genotypes as Variants.
#' @export
setGeneric("toVariants",
           function(ardd, ...) { standardGeneric("toVariants") })

#### Slice operations ####

#' The SliceDataset class is used to manipulate slices.
#' 
#' @name SliceDataset
NULL

#' @rdname SliceDataset
#' @param ardd The RDD to apply this to.
#' @param flankLength The length to extend adjacent records by.
#' @export
setGeneric("flankAdjacentFragments",
           function(ardd, flankLength) {
             standardGeneric("flankAdjacentFragments")
           })

#### Variant operations ####

#' The VariantContextDataset class is used to manipulate VCF-styled data.
#'
#' Each element in a VariantContext genomic dataset corresponds to a VCF line. This
#' differs from the GenotypeDataset, where each element represents the genotype
#' of a single sample at a single site, or a VariantDataset, which represents
#' just the variant of interest.
#' 
#' @name VariantContextDataset
NULL

#' @rdname VariantContextDataset
#' @param ardd The genomic dataset to apply this to.
#' @param filePath Path to save VCF to.
#' @param ... additional argument(s).
#' @export
setGeneric("saveAsVcf",
           function(ardd, filePath, ...) { standardGeneric("saveAsVcf") })
