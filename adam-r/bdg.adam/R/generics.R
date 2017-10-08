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

# @rdname ADAMContext
# @export
setGeneric("loadAlignments",
           function(ac, filePath, ...) { standardGeneric("loadAlignments") })

# @rdname ADAMContext
# @export
setGeneric("loadContigFragments",
           function(ac, filePath) { standardGeneric("loadContigFragments") })

# @rdname ADAMContext
# @export
setGeneric("loadFragments",
           function(ac, filePath, ...) { standardGeneric("loadFragments") })

# @rdname ADAMContext
# @export
setGeneric("loadFeatures",
           function(ac, filePath, ...) { standardGeneric("loadFeatures") })

# @rdname ADAMContext
# @export
setGeneric("loadCoverage",
           function(ac, filePath, ...) { standardGeneric("loadCoverage") })

# @rdname ADAMContext
# @export
setGeneric("loadGenotypes",
           function(ac, filePath, ...) { standardGeneric("loadGenotypes") })

# @rdname ADAMContext
# @export
setGeneric("loadVariants",
           function(ac, filePath, ...) { standardGeneric("loadVariants") })

#### RDD operations ####

# @rdname GenomicRDD
# @export
setGeneric("pipe",
           function(ardd, cmd, tFormatter, xFormatter, convFn, ...) { standardGeneric("pipe") })

# @rdname GenomicRDD
# @export
setGeneric("toDF",
           function(ardd) { standardGeneric("toDF") })

setGeneric("replaceRdd",
           function(ardd, rdd) { standardGeneric("replaceRdd") })

setGeneric("wrapTransformation",
           function(ardd, tFn) { standardGeneric("wrapTransformation") })

# @rdname GenomicRDD
# @export
setGeneric("transform",
           function(ardd, tFn) { standardGeneric("transform") })

setGeneric("inferConversionFn",
           function(ardd, destClass) { standardGeneric("inferConversionFn") })

setGeneric("destClassSuffix",
           function(destClass) { standardGeneric("destClassSuffix") })

# @rdname GenomicRDD
# @export
setGeneric("transmute",
           function(ardd, tFn, destClass, ...) { standardGeneric("transmute") })

# @rdname GenomicRDD
# @export
setGeneric("save",
           function(ardd, filePath, ...) { standardGeneric("save") })

# @rdname GenomicRDD
# @export
setGeneric("sort",
           function(ardd) { standardGeneric("sort") })

# @rdname GenomicRDD
# @export
setGeneric("sortLexicographically",
           function(ardd) { standardGeneric("sortLexicographically") })

#### AlignmentRecord operations ####

# @rdname AlignmentRecordRDD
# @export
setGeneric("toFragments",
           function(ardd) { standardGeneric("toFragments") })

# @rdname AlignmentRecordRDD
# @export
setGeneric("toCoverage",
           function(ardd, ...) { standardGeneric("toCoverage") })

# @rdname AlignmentRecordRDD
# @export
setGeneric("countKmers",
           function(ardd, kmerLength) { standardGeneric("countKmers") })

# @rdname AlignmentRecordRDD
# @export
setGeneric("saveAsSam",
           function(ardd, filePath, ...) { standardGeneric("saveAsSam") })

# @rdname AlignmentRecordRDD-transforms
# @export
setGeneric("sortReadsByReferencePosition",
           function(ardd) { standardGeneric("sortReadsByReferencePosition") })

# @rdname AlignmentRecordRDD-transforms
# @export
setGeneric("sortReadsByReferencePositionAndIndex",
           function(ardd) { standardGeneric("sortReadsByReferencePositionAndIndex") })

# @rdname AlignmentRecordRDD-transforms
# @export
setGeneric("markDuplicates",
           function(ardd) { standardGeneric("markDuplicates") })

# @rdname AlignmentRecordRDD-transforms
# @export
setGeneric("recalibrateBaseQualities",
           function(ardd, knownSnps, validationStringency) {
             standardGeneric("recalibrateBaseQualities")
           })

# @rdname AlignmentRecordRDD-transforms
# @export
setGeneric("realignIndels",
           function(ardd, ...) { standardGeneric("realignIndels") })

# @rdname AlignmentRecordRDD-transforms
# @export
setGeneric("realignIndels",
           function(ardd, knownIndels, ...) { standardGeneric("realignIndels") })

#### Coverage operations ####

# @rdname CoverageRDD
# @export
setGeneric("collapse",
           function(ardd, ...) { standardGeneric("collapse") })

# @rdname CoverageRDD
# @export
setGeneric("toFeatures",
           function(ardd) { standardGeneric("toFeatures") })

# @rdname CoverageRDD
# @export
setGeneric("coverage",
           function(ardd, ...) { standardGeneric("coverage") })

# @rdname CoverageRDD
# @export
setGeneric("aggregatedCoverage",
           function(ardd, ...) { standardGeneric("aggregatedCoverage") })

# @rdname CoverageRDD
# @export
setGeneric("flatten",
           function(ardd) { standardGeneric("flatten") })

#### Fragment operations ####

# @rdname FragmentRDD
# @export
setGeneric("toReads",
           function(ardd) { standardGeneric("toReads") })

#### Genotype operations ####

# @rdname GenotypeRDD
# @export
setGeneric("toVariantContexts",
           function(ardd) { standardGeneric("toVariantContexts") })

# @rdname GenotypeRDD
# @export
setGeneric("saveAsParquet",
           function(ardd, filePath) { standardGeneric("saveAsParquet") })

#### NucleotideContigFragment operations ####

# @rdname NucleotideContigFragmentRDD
# @export
setGeneric("flankAdjacentFragments",
           function(ardd, flankLength) {
             standardGeneric("flankAdjacentFragments")
           })

#### Variant operations ####

# @rdname VariantRDD
# @export
setGeneric("toVariantContexts",
           function(ardd) { standardGeneric("toVariantContexts") })

# @rdname VariantRDD
# @export
setGeneric("saveAsParquet",
           function(ardd, filePath) { standardGeneric("saveAsParquet") })

# @rdname VariantContextRDD
# @export
setGeneric("saveAsVcf",
           function(ardd, filePath, ...) { standardGeneric("saveAsVcf") })
