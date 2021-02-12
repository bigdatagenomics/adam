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

#' A class that wraps an RDD, Dataframe, or Dataset of genomic data with helpful metadata.
#'
#' @rdname GenomicDataset
#' @slot jrdd The Java GenomicDataset that this class wraps.
#' 
#' @export
setClass("GenomicDataset",
         slots = list(jrdd = "jobj"))

#' A class that wraps a GenomicDataset with alignment metadata and functions.
#'
#' @rdname AlignmentDataset
#' @slot jrdd The Java AlignmentDataset that this class wraps.
#' 
#' @export
setClass("AlignmentDataset",
         slots = list(jrdd = "jobj"),
         contains = "GenomicDataset")

#' @importFrom methods new
GenomicDataset <- function(jrdd) {
    new("GenomicDataset", jrdd = jrdd)
}

#' @importFrom methods new
AlignmentDataset <- function(jrdd) {
    new("AlignmentDataset", jrdd = jrdd)
}

#' A class that wraps a GenomicDataset with Coverage metadata and functions.
#'
#' @rdname CoverageDataset
#' @slot jrdd The Java CoverageDataset that this class wraps.
#' 
#' @export
setClass("CoverageDataset",
         slots = list(jrdd = "jobj"),
         contains = "GenomicDataset")

#' @importFrom methods new
CoverageDataset <- function(jrdd) {
    new("CoverageDataset", jrdd = jrdd)
}

#' A class that wraps a GenomicDataset with Feature metadata and functions.
#'
#' @rdname FeatureDataset
#' @slot jrdd The Java FeatureDataset that this class wraps.
#' 
#' @export
setClass("FeatureDataset",
         slots = list(jrdd = "jobj"),
         contains = "GenomicDataset")

#' @importFrom methods new
FeatureDataset <- function(jrdd) {
    new("FeatureDataset", jrdd = jrdd)
}

#' A class that wraps a GenomicDataset with Fragment metadata and functions.
#'
#' @rdname FragmentDataset
#' @slot jrdd The Java FragmentDataset that this class wraps.
#' 
#' @export
setClass("FragmentDataset",
         slots = list(jrdd = "jobj"),
         contains = "GenomicDataset")

FragmentDataset <- function(jrdd) {
    new("FragmentDataset", jrdd = jrdd)
}

#' A class that wraps a GenomicDataset with Genotype metadata and functions.
#'
#' @rdname GenotypeDataset
#' @slot jrdd The Java GenotypeDataset that this class wraps.
#' 
#' @export
setClass("GenotypeDataset",
         slots = list(jrdd = "jobj"),
         contains = "GenomicDataset")

#' @importFrom methods new
GenotypeDataset <- function(jrdd) {
    new("GenotypeDataset", jrdd = jrdd)
}

#' A class that wraps a GenomicDataset with Read metadata and functions.
#'
#' @rdname ReadDataset
#' @slot jrdd The Java ReadDataset that this class wraps.
#'
#' @export
setClass("ReadDataset",
         slots = list(jrdd = "jobj"),
         contains = "GenomicDataset")

#' @importFrom methods new
ReadDataset <- function(jrdd) {
    new("ReadDataset", jrdd = jrdd)
}

#' A class that wraps a GenomicDataset with Sequence metadata and functions.
#'
#' @rdname SequenceDataset
#' @slot jrdd The Java SequenceDataset that this class wraps.
#' 
#' @export
setClass("SequenceDataset",
         slots = list(jrdd = "jobj"),
         contains = "GenomicDataset")

#' @importFrom methods new
SequenceDataset <- function(jrdd) {
    new("SequenceDataset", jrdd = jrdd)
}

#' A class that wraps a GenomicDataset with Slice metadata and functions.
#'
#' @rdname SliceDataset
#' @slot jrdd The Java SliceDataset that this class wraps.
#' 
#' @export
setClass("SliceDataset",
         slots = list(jrdd = "jobj"),
         contains = "GenomicDataset")

#' @importFrom methods new
SliceDataset <- function(jrdd) {
    new("SliceDataset", jrdd = jrdd)
}

#' A class that wraps a GenomicDataset with Variant metadata and functions.
#'
#' @rdname VariantDataset
#' @slot jrdd The Java VariantDataset that this class wraps.
#' 
#' @export
setClass("VariantDataset",
         slots = list(jrdd = "jobj"),
         contains = "GenomicDataset")

#' @importFrom methods new
VariantDataset <- function(jrdd) {
    new("VariantDataset", jrdd = jrdd)
}

#' A class that wraps a GenomicDataset with VariantContext metadata and functions.
#'
#' @rdname VariantContextDataset
#' @slot jrdd The Java VariantContextDataset that this class wraps.
#' 
#' @export
setClass("VariantContextDataset",
         slots = list(jrdd = "jobj"),
         contains = "GenomicDataset")

#' @importFrom methods new
VariantContextDataset <- function(jrdd) {
    new("VariantContextDataset", jrdd = jrdd)
}

#'
#' Pipes genomic data to a subprocess that runs in parallel using Spark.
#' 
#' Files are substituted in to the command with a $x syntax. E.g., to invoke
#' a command that uses the first file from the files Seq, use $0. To access
#' the path to the directory where the files are copied, use $root.
#' 
#' Pipes require the presence of an InFormatterCompanion and an OutFormatter
#' as implicit values. The InFormatterCompanion should be a singleton whose
#' apply method builds an InFormatter given a specific type of GenomicDataset.
#' The implicit InFormatterCompanion yields an InFormatter which is used to
#' format the input to the pipe, and the implicit OutFormatter is used to
#' parse the output from the pipe.
#'
#' @param ardd The genomic dataset to apply this to.
#' @param cmd The command to run.
#' @param tFormatter The name of the ADAM in-formatter class to use.
#' @param xFormatter The name of the ADAM out-formatter class to use.
#' @param convFn The name of the ADAM GenomicDataset conversion class to
#'   use.
#' @param files The files to copy locally onto all executors. Set to
#'   None (default) to omit.
#' @param environment The environment variables to set on the
#'   executor. Set to None (default) to omit.
#' @param flankSize The number of bases of flanking sequence to have
#'   around each partition. Defaults to 0.
#' @return Returns a new genomic dataset where the input from the original genomic dataset has
#'   been piped through a command that runs locally on each executor.
#'
#' @importFrom SparkR sparkR.callJStatic sparkR.callJMethod
#'
#' @export
setMethod("pipe",
          signature(ardd = "GenomicDataset",
                    tFormatter = "character",
                    xFormatter = "character",
                    convFn = "character"),
          function(ardd,
                   cmd,
                   tFormatter,
                   xFormatter,
                   convFn,
                   files = NA,
                   environment = NA,
                   flankSize = 0) {

              tFormatterClass = sparkR.callJStatic("java.lang.Class",
                                                   "forName",
                                                   tFormatter)
        
              xFormatterClass = sparkR.callJStatic("java.lang.Class",
                                                   "forName",
                                                   xFormatter)
              xFormatterInst = sparkR.callJMethod(xFormatterClass,
                                                  "newInstance")
              
              convFnClass = sparkR.callJStatic("java.lang.Class",
                                               "forName",
                                               convFn)
              convFnInst = sparkR.callJMethod(convFnClass,
                                              "newInstance")
              
              if (is.na(files)) {
                  files = list()
              }
              
              if (is.na(environment)) {
                  environment = new.env()
              }

              rdd = sparkR.callJMethod(ardd@jrdd,
                                       "pipe",
                                       cmd,
                                       files,
                                       environment,
                                       flankSize,
                                       tFormatterClass,
                                       xFormatterInst,
                                       convFnInst)
              replaceRdd(ardd, rdd)
          })


#' Caches the existing ardd
#'
#' @param ardd The genomic dataset to apply this to.
#' @return A new genomic dataset where the genomic dataset of genomic data has been replaced, but the
#'    metadata (reference sequence dictionary, and etc) is copied without modification.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("cache",
          signature(ardd = "GenomicDataset"),
          function(ardd) {
            replaceRdd(ardd, sparkR.callJMethod(ardd@jrdd, "cache"))
          })

#' Persists the existing ardd
#'
#' @param ardd The genomic dataset to apply this to.
#' @param sl the StorageLevel to persist in.
#' @return A new genomic dataset where the genomic dataset of genomic data has been replaced, but the
#'    metadata (reference sequence dictionary, and etc) is copied without modification.
#'
#' @importFrom SparkR sparkR.callJMethod sparkR.callJStatic
#'
#' @export
setMethod("persist",
          signature(ardd = "GenomicDataset",
                    sl = "character"),
          function(ardd, sl) {
              storageLevel <- sparkR.callJStatic("org.apache.spark.storage.StorageLevel", "fromString", sl)
              replaceRdd(ardd, sparkR.callJMethod(ardd@jrdd, "persist", storageLevel))
          })

#' Unpersists the existing ardd
#'
#' @param ardd The genomic dataset to apply this to.
#' @return A new genomic dataset where the genomic dataset of genomic data has been replaced, but the
#'    metadata (reference sequence dictionary, and etc) is copied without modification.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("unpersist",
          signature(ardd = "GenomicDataset"),
          function(ardd) {
              replaceRdd(ardd, sparkR.callJMethod(ardd@jrdd, "unpersist"))
          })

#' Sorts our genome aligned data by reference positions, with contigs ordered
#' by index.
#'
#' @param ardd The genomic dataset to apply this to.
#' @return Returns a new, sorted genomic dataset, of the implementing class type.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("sort",
          signature(ardd = "GenomicDataset"),
          function(ardd) {
              replaceRdd(ardd, sparkR.callJMethod(ardd@jrdd, "sort"))
          })

#' Sorts our genome aligned data by reference positions, with contigs ordered
#' lexicographically.
#'
#' @param ardd The genomic dataset to apply this to.
#' @return Returns a new, sorted genomic dataset, of the implementing class type.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("sortLexicographically",
          signature(ardd = "GenomicDataset"),
          function(ardd) {
              replaceRdd(ardd, sparkR.callJMethod(ardd@jrdd, "sortLexicographically"))
          })

#' Converts this GenomicDataset into a dataframe.
#'
#' @param ardd The genomic dataset to convert into a dataframe.
#' @return Returns a dataframe representing this genomic dataset.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("toDF",
          signature(ardd = "GenomicDataset"),
          function(ardd) {
              sdf = sparkR.callJMethod(ardd@jrdd, "toDF")
              new("SparkDataFrame", sdf, FALSE)
          })

#' @importFrom SparkR sparkR.callJStatic
setMethod("wrapTransformation",
          signature(ardd = "GenomicDataset",
                    tFn = "function"),
          function(ardd, tFn) {
              df = toDF(ardd)
              newDf = tFn(df)

              # should be <init> for ctr
              sparkR.callJStatic("org.bdgenomics.adam.api.python.DataFrameConversionWrapper",
                                 "<init>",
                                 newDf@sdf)
          })

#' Applies a function that transforms the underlying DataFrame into a new DataFrame
#' using the Spark SQL API.
#'
#' @param ardd The genomic dataset to apply this to.
#' @param tFn A function that transforms the underlying DataFrame as a DataFrame.
#' @return A new genomic dataset where the DataFrame of genomic data has been replaced, but the
#'    metadata (reference sequence dictionary, and etc) is copied without modification.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("transform",
          signature(ardd = "GenomicDataset",
                    tFn = "function"),
          function(ardd, tFn) {
              dfFn = wrapTransformation(ardd, tFn)
              
              replaceRdd(ardd,
                         sparkR.callJMethod(ardd@jrdd, "transformDataFrame", dfFn))
          })

setMethod("inferConversionFn",
          signature(ardd = "GenomicDataset",
                    destClass = "character"),
          function(ardd, destClass) {
              stop("This class does not implement conversion function inference.")
          })

setMethod("destClassSuffix",
          signature(destClass = "character"),
          function(destClass) {
              if (destClass == "CoverageDataset") {
                  "CoverageDatasetConverter"
              } else if (destClass == "FeatureDataset") {
                  "FeaturesDatasetConverter"
              } else if (destClass == "FragmentDataset") {
                  "FragmentDatasetConverter"
              } else if (destClass == "AlignmentDataset") {
                  "AlignmentDatasetConverter"
              } else if (destClass == "GenotypeDataset") {
                  "GenotypeDatasetConverter"
              } else if (destClass == "VariantDataset") {
                  "VariantDatasetConverter"
              } else if (destClass == "ReadDataset") {
                  "ReadDatasetConverter"
              } else if (destClass == "SequenceDataset") {
                  "SequenceDatasetConverter"
              } else if (destClass == "SliceDataset") {
                  "SliceDatasetConverter"
              } else {
                  stop(paste("No conversion method known for",
                             destClass))
              }
          })

#' Applies a function that transmutes the underlying DataFrame into a new DataFrame of a
#' different type.
#'
#' @param ardd The genomic dataset to apply this to.
#' @param tFn A function that transforms the underlying DataFrame as a DataFrame.
#' @param destClass The destination class of this transmutation.
#' @param convFn The name of the ADAM GenomicDatasetConversion class to use.
#' @return A new genomic dataset where the genomic dataset of genomic data has been replaced, but the
#'   metadata (reference sequence dictionary, and etc) is copied without modification.
#'
#' @importFrom SparkR sparkR.callJMethod sparkR.callJStatic
#'
#' @export
setMethod("transmute",
          signature(ardd = "GenomicDataset",
                    tFn = "function",
                    destClass = "character"),
          function(ardd, tFn, destClass, convFn = NA) {
              dfFn = wrapTransformation(ardd, tFn)

              # if no conversion function is provided, try to infer
              if (is.na(convFn)) {
                  convFn = inferConversionFn(ardd, destClass)
              }

              # create an instance of the conversion
              convFnInst = sparkR.callJStatic(convFn, "<init>")
              
              new(destClass,
                  jrdd = sparkR.callJMethod(ardd@jrdd, "transmuteDataFrame", dfFn, convFnInst))
          })

#' Performs a broadcast inner join between this genomic dataset and another genomic dataset.
#'
#' In a broadcast join, the left genomic dataset (this genomic dataset) is collected to the driver,
#' and broadcast to all the nodes in the cluster. The key equality function
#' used for this join is the reference region overlap function. Since this
#' is an inner join, all values who do not overlap a value from the other
#' genomic dataset are dropped.
#'
#' @param ardd The left genomic dataset in the join.
#' @param genomicRdd The right genomic dataset in the join.
#' @param flankSize Sets a flankSize for the distance between elements to be
#'   joined. If set to 0, an overlap is required to join two elements.
#' @return Returns a new genomic dataset containing all pairs of keys that
#'   overlapped in the genomic coordinate space.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("broadcastRegionJoin",
          signature(ardd = "GenomicDataset",
                    genomicRdd = "GenomicDataset"),
          function(ardd, genomicRdd, flankSize=0) {
              GenomicDataset(sparkR.callJMethod(ardd@jrdd,
                                                "broadcastRegionJoin",
                                                genomicRdd@jrdd,
                                                flankSize))
          })

#' Performs a broadcast right outer join between this genomic dataset and another genomic dataset.
#'
#' In a broadcast join, the left genomic dataset (this genomic dataset) is collected to the driver,
#' and broadcast to all the nodes in the cluster. The key equality function
#' used for this join is the reference region overlap function. Since this
#' is a right outer join, all values in the left genomic dataset that do not overlap a
#' value from the right genomic dataset are dropped. If a value from the right genomic dataset does
#' not overlap any values in the left genomic dataset, it will be paired with a `None`
#' in the product of the join.
#'
#' @param ardd The left genomic dataset in the join.
#' @param genomicRdd The right genomic dataset in the join.
#' @param flankSize Sets a flankSize for the distance between elements to be
#'   joined. If set to 0, an overlap is required to join two elements.
#' @return Returns a new genomic dataset containing all pairs of keys that
#'   overlapped in the genomic coordinate space, and all keys from the
#'   right genomic dataset that did not overlap a key in the left genomic dataset.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("rightOuterBroadcastRegionJoin",
          signature(ardd = "GenomicDataset",
                    genomicRdd = "GenomicDataset"),
          function(ardd, genomicRdd, flankSize=0) {
              GenomicDataset(sparkR.callJMethod(ardd@jrdd,
                                                "rightOuterBroadcastRegionJoin",
                                                genomicRdd@jrdd,
                                                flankSize))
          })

#' Performs a broadcast inner join between this genomic dataset and another genomic dataset.
#'
#' In a broadcast join, the left genomic dataset (this genomic dataset) is collected to the driver,
#' and broadcast to all the nodes in the cluster. The key equality function
#' used for this join is the reference region overlap function. Since this
#' is an inner join, all values who do not overlap a value from the other
#' genomic dataset are dropped.
#'
#' @param ardd The left genomic dataset in the join.
#' @param genomicRdd The right genomic dataset in the join.
#' @param flankSize Sets a flankSize for the distance between elements to be
#'   joined. If set to 0, an overlap is required to join two elements.
#' @return Returns a new genomic dataset containing all pairs of keys that
#'   overlapped in the genomic coordinate space.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("broadcastRegionJoinAndGroupByRight",
          signature(ardd = "GenomicDataset",
                    genomicRdd = "GenomicDataset"),
          function(ardd, genomicRdd, flankSize=0) {
              GenomicDataset(sparkR.callJMethod(ardd@jrdd,
                                                "broadcastRegionJoinAndGroupByRight",
                                                genomicRdd@jrdd,
                                                flankSize))
          })

#' Performs a broadcast right outer join between this genomic dataset and another genomic dataset.
#'
#' In a broadcast join, the left genomic dataset (this genomic dataset) is collected to the driver,
#' and broadcast to all the nodes in the cluster. The key equality function
#' used for this join is the reference region overlap function. Since this
#' is a right outer join, all values in the left genomic dataset that do not overlap a
#' value from the right genomic dataset are dropped. If a value from the right genomic dataset does
#' not overlap any values in the left genomic dataset, it will be paired with a `None`
#' in the product of the join.
#'
#' @param ardd The left genomic dataset in the join.
#' @param genomicRdd The right genomic dataset in the join.
#' @param flankSize Sets a flankSize for the distance between elements to be
#'   joined. If set to 0, an overlap is required to join two elements.
#' @return Returns a new genomic dataset containing all pairs of keys that
#'   overlapped in the genomic coordinate space, and all keys from the
#'   right genomic dataset that did not overlap a key in the left genomic dataset.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("rightOuterBroadcastRegionJoinAndGroupByRight",
          signature(ardd = "GenomicDataset",
                    genomicRdd = "GenomicDataset"),
          function(ardd, genomicRdd, flankSize=0) {
              GenomicDataset(sparkR.callJMethod(ardd@jrdd,
                                                "rightOuterBroadcastRegionJoinAndGroupByRight",
                                                genomicRdd@jrdd,
                                                flankSize))
          })

#' Performs a sort-merge inner join between this genomic dataset and another genomic dataset.
#'
#' In a sort-merge join, both genomic datasets are co-partitioned and sorted. The
#' partitions are then zipped, and we do a merge join on each partition.
#' The key equality function used for this join is the reference region
#' overlap function. Since this is an inner join, all values who do not
#' overlap a value from the other genomic dataset are dropped.
#'
#' @param ardd The left genomic dataset in the join.
#' @param genomicRdd The right genomic dataset in the join.
#' @param flankSize Sets a flankSize for the distance between elements to be
#'   joined. If set to 0, an overlap is required to join two elements.
#' @return Returns a new genomic dataset containing all pairs of keys that
#'   overlapped in the genomic coordinate space.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("shuffleRegionJoin",
          signature(ardd = "GenomicDataset",
                    genomicRdd = "GenomicDataset"),
          function(ardd, genomicRdd, flankSize=0) {
              GenomicDataset(sparkR.callJMethod(ardd@jrdd,
                                                "shuffleRegionJoin",
                                                genomicRdd@jrdd,
                                                flankSize))
          })

#' Performs a sort-merge right outer join between this genomic dataset and another genomic dataset.
#'
#' In a sort-merge join, both genomic datasets are co-partitioned and sorted. The
#' partitions are then zipped, and we do a merge join on each partition.
#' The key equality function used for this join is the reference region
#' overlap function. Since this is a right outer join, all values in the
#' left genomic dataset that do not overlap a value from the right genomic dataset are dropped.
#' If a value from the right genomic dataset does not overlap any values in the left
#' genomic dataset, it will be paired with a `None` in the product of the join.
#'
#' @param ardd The left genomic dataset in the join.
#' @param genomicRdd The right genomic dataset in the join.
#' @param flankSize Sets a flankSize for the distance between elements to be
#'   joined. If set to 0, an overlap is required to join two elements.
#' @return Returns a new genomic dataset containing all pairs of keys that
#'   overlapped in the genomic coordinate space, and all keys from the
#'   right genomic dataset that did not overlap a key in the left genomic dataset.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("rightOuterShuffleRegionJoin",
          signature(ardd = "GenomicDataset",
                    genomicRdd = "GenomicDataset"),
          function(ardd, genomicRdd, flankSize=0) {
              GenomicDataset(sparkR.callJMethod(ardd@jrdd,
                                                "rightOuterShuffleRegionJoin",
                                                genomicRdd@jrdd,
                                                flankSize))
          })

#' Performs a sort-merge left outer join between this genomic dataset and another genomic dataset.
#'
#' In a sort-merge join, both genomic datasets are co-partitioned and sorted. The
#' partitions are then zipped, and we do a merge join on each partition.
#' The key equality function used for this join is the reference region
#' overlap function. Since this is a left outer join, all values in the
#' right genomic dataset that do not overlap a value from the left genomic dataset are dropped.
#' If a value from the left genomic dataset does not overlap any values in the right
#' genomic dataset, it will be paired with a `None` in the product of the join.
#'
#' @param ardd The left genomic dataset in the join.
#' @param genomicRdd The right genomic dataset in the join.
#' @param flankSize Sets a flankSize for the distance between elements to be
#'   joined. If set to 0, an overlap is required to join two elements.
#' @return Returns a new genomic dataset containing all pairs of keys that
#'   overlapped in the genomic coordinate space, and all keys from the
#'    left genomic dataset that did not overlap a key in the left genomic dataset.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("leftOuterShuffleRegionJoin",
          signature(ardd = "GenomicDataset",
                    genomicRdd = "GenomicDataset"),
          function(ardd, genomicRdd, flankSize=0) {
              GenomicDataset(sparkR.callJMethod(ardd@jrdd,
                                                "leftOuterShuffleRegionJoin",
                                                genomicRdd@jrdd,
                                                flankSize))
          })

#' Performs a sort-merge left outer join between this genomic dataset and another genomic dataset,
#' followed by a groupBy on the left value.
#'
#' In a sort-merge join, both genomic datasets are co-partitioned and sorted. The
#' partitions are then zipped, and we do a merge join on each partition.
#' The key equality function used for this join is the reference region
#' overlap function. Since this is a left outer join, all values in the
#' right genomic dataset that do not overlap a value from the left genomic dataset are dropped.
#' If a value from the left genomic dataset does not overlap any values in the right
#' genomic dataset, it will be paired with an empty Iterable in the product of the join.
#'
#' @param ardd The left genomic dataset in the join.
#' @param genomicRdd The right genomic dataset in the join.
#' @param flankSize Sets a flankSize for the distance between elements to be
#'   joined. If set to 0, an overlap is required to join two elements.
#' @return Returns a new genomic dataset containing all pairs of keys that
#'   overlapped in the genomic coordinate space, and all keys from the
#'    left genomic dataset that did not overlap a key in the left genomic dataset.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("leftOuterShuffleRegionJoinAndGroupByLeft",
          signature(ardd = "GenomicDataset",
                    genomicRdd = "GenomicDataset"),
          function(ardd, genomicRdd, flankSize=0) {
              GenomicDataset(sparkR.callJMethod(ardd@jrdd,
                                                "leftOuterShuffleRegionJoinAndGroupByLeft",
                                                genomicRdd@jrdd,
                                                flankSize))
          })

#' Performs a sort-merge full outer join between this genomic dataset and another genomic dataset.
#'
#' In a sort-merge join, both genomic datasets are co-partitioned and sorted. The
#' partitions are then zipped, and we do a merge join on each partition.
#' The key equality function used for this join is the reference region
#' overlap function. Since this is a full outer join, if a value from either
#' genomic dataset does not overlap any values in the other genomic dataset, it will be paired with
#' a `None` in the product of the join.
#'
#' @param ardd The left genomic dataset in the join.
#' @param genomicRdd The right genomic dataset in the join.
#' @param flankSize Sets a flankSize for the distance between elements to be
#'   joined. If set to 0, an overlap is required to join two elements.
#' @return Returns a new genomic dataset containing all pairs of keys that
#'   overlapped in the genomic coordinate space, and values that did not
#'   overlap will be paired with a `None`.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("fullOuterShuffleRegionJoin",
          signature(ardd = "GenomicDataset",
                    genomicRdd = "GenomicDataset"),
          function(ardd, genomicRdd, flankSize=0) {
              GenomicDataset(sparkR.callJMethod(ardd@jrdd,
                                                "fullOuterShuffleRegionJoin",
                                                genomicRdd@jrdd,
                                                flankSize))
          })

#' Performs a sort-merge right outer join between this genomic dataset and another genomic dataset,
#' followed by a groupBy on the left value.
#'
#' In a sort-merge join, both genomic datasets are co-partitioned and sorted. The
#' partitions are then zipped, and we do a merge join on each partition.
#' The key equality function used for this join is the reference region
#' overlap function. Since this is a right outer join, all values from the
#' right genomic dataset who did not overlap a value from the left genomic dataset are placed into
#' a length-1 Iterable with a `None` key.
#'
#' @param ardd The left genomic dataset in the join.
#' @param genomicRdd The right genomic dataset in the join.
#' @param flankSize Sets a flankSize for the distance between elements to be
#'   joined. If set to 0, an overlap is required to join two elements.
#' @return Returns a new genomic dataset containing all pairs of keys that
#'   overlapped in the genomic coordinate space, and all values from the
#'   right genomic dataset that did not overlap an item in the left genomic dataset.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("rightOuterShuffleRegionJoinAndGroupByLeft",
          signature(ardd = "GenomicDataset",
                    genomicRdd = "GenomicDataset"),
          function(ardd, genomicRdd, flankSize=0) {
              GenomicDataset(sparkR.callJMethod(ardd@jrdd,
                                                "rightOuterShuffleRegionJoinAndGroupByLeft",
                                                genomicRdd@jrdd,
                                                flankSize))
          })

#' Performs a sort-merge inner join between this genomic dataset and another genomic dataset,
#' followed by a groupBy on the left value.
#'
#' In a sort-merge join, both genomic datasets are co-partitioned and sorted. The
#' partitions are then zipped, and we do a merge join on each partition.
#' The key equality function used for this join is the reference region
#' overlap function. In the same operation, we group all values by the left
#' item in the genomic dataset.
#'
#' @param ardd The left genomic dataset in the join.
#' @param genomicRdd The right genomic dataset in the join.
#' @param flankSize Sets a flankSize for the distance between elements to be
#'   joined. If set to 0, an overlap is required to join two elements.
#' @return Returns a new genomic dataset containing all pairs of keys that
#'   overlapped in the genomic coordinate space, grouped together by
#'   the value they overlapped in the left genomic dataset.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("shuffleRegionJoinAndGroupByLeft",
          signature(ardd = "GenomicDataset",
                    genomicRdd = "GenomicDataset"),
          function(ardd, genomicRdd, flankSize=0) {
              GenomicDataset(sparkR.callJMethod(ardd@jrdd,
                                                "shuffleRegionJoinAndGroupByLeft",
                                                genomicRdd@jrdd,
                                                flankSize))
          })

setMethod("replaceRdd",
          signature(ardd = "AlignmentDataset",
                    rdd = "jobj"),
          function(ardd, rdd) {
              AlignmentDataset(rdd)
          })

setMethod("inferConversionFn",
          signature(ardd = "AlignmentDataset",
                    destClass = "character"),
          function(ardd, destClass) {
              paste0("org.bdgenomics.adam.api.java.AlignmentsTo",
                     destClassSuffix(destClass))
          })

#' Convert this set of reads into fragments.
#'
#' @param ardd The genomic dataset to apply this to.
#' @return Returns a FragmentDataset where all reads have been grouped together by
#' the original sequence fragment they come from.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("toFragments",
          signature(ardd = "AlignmentDataset"),
          function(ardd) {
              FragmentDataset(sparkR.callJMethod(ardd@jrdd, "toFragments"))
          })

#' Saves this genomic dataset to disk as a SAM/BAM/CRAM file.
#'
#' @param ardd The genomic dataset to apply this to.
#' @param filePath The path to save the file to.
#' @param asType The type of file to save. Valid choices are SAM, BAM,
#'   CRAM, and NA. If None, the file type is inferred from the extension.
#' @param isSorted Whether the file is sorted or not.
#' @param asSingleFile Whether to save the file as a single merged
#'   file or as shards.
#'
#' @importFrom SparkR sparkR.callJMethod sparkR.callJStatic
#'
#' @export
setMethod("saveAsSam",
          signature(ardd = "AlignmentDataset", filePath = "character"),
          function(ardd,
                   filePath,
                   asType = NA,
                   isSorted = FALSE,
                   asSingleFile = FALSE) {

              if (is.na(asType)) {
                  fileType <- sparkR.callJStatic("org.seqdoop.hadoop_bam.SAMFormat",
                                                 "inferFromFilePath",
                                                 filePath)
              } else {
                  fileType <- sparkR.callJStatic("org.seqdoop.hadoop_bam.SAMFormat",
                                                 "valueOf",
                                                 asType)
              }

              invisible(sparkR.callJMethod(ardd@jrdd,
                                           "saveAsSam",
                                           filePath,
                                           fileType,
                                           asSingleFile,
                                           isSorted))
          })

#' Converts this set of reads into a corresponding CoverageDataset.
#'
#' @param ardd The genomic dataset to apply this to.
#' @param collapse Determines whether to merge adjacent coverage elements with
#'                 the same score to a single coverage observation.
#' @return Returns a genomic dataset with observed coverage.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("toCoverage",
          signature(ardd = "AlignmentDataset"),
          function(ardd, collapse = TRUE) {
              CoverageDataset(sparkR.callJMethod(ardd@jrdd, "toCoverage", collapse))
          })

#' Saves this genomic dataset to disk, with the type identified by the extension.
#'
#' @param ardd The genomic dataset to apply this to.
#' @param filePath The path to save the file to.
#' @param isSorted Whether the file is sorted or not.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("save",
          signature(ardd = "AlignmentDataset", filePath = "character"),
          function(ardd, filePath, isSorted = FALSE) {
              invisible(sparkR.callJMethod(ardd@jrdd, "save", filePath, isSorted))
          })

#' Cuts reads into _k_-mers, and then counts the occurrences of each _k_-mer.
#'
#' @param ardd The genomic dataset to apply this to.
#' @param kmerLength The value of _k_ to use for cutting _k_-mers.
#' @return Returns a DataFrame containing k-mer/count pairs.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("countKmers",
          signature(ardd = "AlignmentDataset", kmerLength = "numeric"),
          function(ardd, kmerLength) {
              new("SparkDataFrame",
                  sparkR.callJMethod(sparkR.callJMethod(ardd@jrdd,
                                                        "countKmersAsDataset",
                                                        as.integer(kmerLength)),
                                     "toDF"),
                  FALSE)
          })


#' Sorts our alignments by read name.
#'
#' @param ardd The genomic dataset to apply this to.
#' @return A new, sorted AlignmentDataset.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("sortByReadName",
          signature(ardd = "AlignmentDataset"),
          function(ardd) {
              AlignmentDataset(sparkR.callJMethod(ardd@jrdd, "sortByReadName"))
          })

#' Sorts our alignments by reference positions, with references ordered by name.
#'
#' Sorts alignments by the location where reads are aligned. Unaligned reads are
#' put at the end and sorted by read name. References are ordered lexicographically
#' by name.
#'
#' @param ardd The genomic dataset to apply this to.
#' @return A new, sorted AlignmentDataset.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("sortByReferencePosition",
          signature(ardd = "AlignmentDataset"),
          function(ardd) {
              AlignmentDataset(sparkR.callJMethod(ardd@jrdd, "sortByReferencePosition"))
          })

#' Sorts our alignments by reference positions, with references ordered by index.
#'
#' Sorts alignments by the location where reads are aligned. Unaligned reads are
#' put at the end and sorted by read name. References are ordered by index that
#' they are ordered in the reference sequence metadata.
#'
#' @param ardd The genomic dataset to apply this to.
#' @return A new, sorted AlignmentDataset.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("sortByReferencePositionAndIndex",
          signature(ardd = "AlignmentDataset"),
          function(ardd) {
              AlignmentDataset(sparkR.callJMethod(ardd@jrdd, "sortByReferencePositionAndIndex"))
          })

#' Marks reads as possible fragment duplicates.
#'
#' @param ardd The genomic dataset to apply this to.
#' @return A new genomic dataset where reads have the duplicate read flag set. Duplicate
#'          reads are NOT filtered out.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("markDuplicates",
          signature(ardd = "AlignmentDataset"),
          function(ardd) {
              AlignmentDataset(sparkR.callJMethod(ardd@jrdd, "markDuplicates"))
          })

#' Runs base quality score recalibration on a set of reads.
#'
#' Uses a table of known SNPs to mask true variation during the recalibration
#' process.
#'
#' @param ardd The genomic dataset to apply this to.
#' @param knownSnps A table of known SNPs to mask valid variants.
#' @param validationStringency The stringency to apply towards validating BQSR.
#'
#' @importFrom SparkR sparkR.callJMethod sparkR.callJStatic
#'
#' @export
setMethod("recalibrateBaseQualities",
          signature(ardd = "AlignmentDataset", knownSnps = "VariantDataset", validationStringency = "character"),
          function(ardd, knownSnps, validationStringency) {
              stringency <- sparkR.callJStatic("htsjdk.samtools.ValidationStringency", "valueOf", validationStringency)
              AlignmentDataset(sparkR.callJMethod(ardd@jrdd, "recalibrateBaseQualities", knownSnps@jrdd, stringency))
          })

#' Realigns indels using a consensus-based heuristic.
#'
#' If no known indels are provided, generates consensuses from reads. Else,
#' generates consensuses from previously seen variants.
#' 
#' @param ardd The genomic dataset to apply this to.
#' @param isSorted If the input data is sorted, setting this parameter to true
#'   avoids a second sort.
#' @param maxIndelSize The size of the largest indel to use for realignment.
#' @param maxConsensusNumber The maximum number of consensus sequences to
#'   realign against per target region.
#' @param lodThreshold Log-odds threshold to use when realigning; realignments
#'   are only finalized if the log-odds threshold is exceeded.
#' @param maxTargetSize The maximum width of a single target region for
#'   realignment.
#' @param maxReadsPerTarget Maximum number of reads per target.
#' @param unclipReads If true, unclips reads prior to realignment. Else,
#    omits clipped bases during realignment.
#' @param knownIndels A genomic dataset of previously called INDEL variants.
#' @return Returns a genomic dataset of mapped reads which have been realigned.
#'
#' @importFrom SparkR sparkR.callJMethod sparkR.callJStatic
#'
#' @export
setMethod("realignIndels",
          signature(ardd = "AlignmentDataset"),
          function(ardd, isSorted = FALSE, maxIndelSize = 500,
                   maxConsensusNumber = 30, lodThreshold = 5.0,
                   maxTargetSize = 3000, maxReadsPerTarget = 20000,
                   unclipReads = FALSE, knownIndels = NA) {

              if (is.na(knownIndels)) {
                  consensusModel <- sparkR.callJStatic("org.bdgenomics.adam.algorithms.consensus.ConsensusGenerator",
                                                       "fromKnowns", knownIndels@jrdd)
                  AlignmentDataset(sparkR.callJMethod(ardd@jrdd, "realignIndels",
                                                        consensusModel,
                                                        isSorted,
                                                        maxIndelSize,
                                                        maxConsensusNumber,
                                                        lodThreshold,
                                                        maxTargetSize,
                                                        maxReadsPerTarget,
                                                        unclipReads))

              } else {
                  consensusModel <- sparkR.callJStatic("org.bdgenomics.adam.algorithms.consensus.ConsensusGenerator",
                                                       "fromReads")
                  AlignmentDataset(sparkR.callJMethod(ardd@jrdd, "realignIndels",
                                                        consensusModel,
                                                        isSorted,
                                                        maxIndelSize,
                                                        maxConsensusNumber,
                                                        lodThreshold,
                                                        maxTargetSize,
                                                        maxReadsPerTarget,
                                                        unclipReads))
              }
          })

#' Saves coverage as a feature file.
#'
#' @param ardd The genomic dataset to apply this to.
#' @param filePath The location to write the output.
#' @param asSingleFile If true, merges the sharded output into a single file.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("save",
          signature(ardd = "CoverageDataset", filePath = "character"),
          function(ardd, filePath, asSingleFile = FALSE) {
              invisible(sparkR.callJMethod(ardd@jrdd, "save", filePath, asSingleFile))
          })

#' Merges adjacent ReferenceRegions with the same coverage value.
#'
#' This reduces the loss of coverage information while reducing the number of
#' of records in the genomic dataset. For example, adjacent records Coverage("chr1", 1, 10,
#' 3.0) and Coverage("chr1", 10, 20, 3.0) would be merged into one record
#' Coverage("chr1", 1, 20, 3.0).
#'
#' @param ardd The genomic dataset to apply this to.
#' @return A genomic dataset with merged tuples of adjacent sites with same coverage.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("collapse", signature(ardd = "CoverageDataset"),
          function(ardd) {
              CoverageDataset(sparkR.callJMethod(ardd@jrdd, "collapse"))
          })

#' Converts CoverageDataset to FeatureDataset.
#'
#' @param ardd The genomic dataset to apply this to.
#' @return Returns a FeatureDataset from a CoverageDataset.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("toFeatures", signature(ardd = "CoverageDataset"),
          function(ardd) {
              FeatureDataset(sparkR.callJMethod(ardd@jrdd, "toFeatures"))
          })

#' Gets coverage overlapping specified ReferenceRegion.
#'
#' For large ReferenceRegions, base pairs per bin (bpPerBin) can be specified to
#' bin together ReferenceRegions of equal size. The coverage of each bin is the
#' coverage of the first base pair in that bin.
#'
#' @param ardd The genomic dataset to apply this to.
#' @param bpPerBin Number of bases to combine to one bin.
#' @return Returns a sparsified CoverageDataset.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("coverage", signature(ardd = "CoverageDataset"),
          function(ardd, bpPerBin = 1) {
              CoverageDataset(sparkR.callJMethod(ardd@jrdd, "coverage", bpPerBin))
          })

#' Gets coverage overlapping specified ReferenceRegion.
#'
#' For large ReferenceRegions, base pairs per bin (bpPerBin) can be specified to
#' bin together ReferenceRegions of equal size. The coverage of each bin is the
#' average coverage of the bases in that bin.
#'
#' @param ardd The genomic dataset to apply this to.
#' @param bpPerBin Number of bases to combine to one bin.
#' @return Returns a sparsified CoverageDataset.
#'
#' @rdname CoverageDataset
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("aggregatedCoverage", signature(ardd = "CoverageDataset"),
          function(ardd, bpPerBin = 1) {
              CoverageDataset(sparkR.callJMethod(ardd@jrdd, "aggregatedCoverage", bpPerBin))
          })

#' Gets flattened genomic dataset of coverage, with coverage mapped to each base pair.
#'
#' The opposite operation of collapse.
#'
#' @param ardd The genomic dataset to apply this to.
#' @return New CoverageDataset of flattened coverage.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("flatten", signature(ardd = "CoverageDataset"),
          function(ardd) {
              CoverageDataset(sparkR.callJMethod(ardd@jrdd, "flatten"))
          })

setMethod("inferConversionFn",
          signature(ardd = "FeatureDataset",
                    destClass = "character"),
          function(ardd, destClass) {
              paste0("org.bdgenomics.adam.api.java.FeaturesTo",
                     destClassSuffix(destClass))
          })

setMethod("replaceRdd",
          signature(ardd = "FeatureDataset",
                    rdd = "jobj"),
          function(ardd, rdd) {
              FeatureDataset(rdd)
          })

#' Saves coverage, autodetecting the file type from the extension.
#'
#' Writes files ending in .bed as BED6/12, .gff3 as GFF3, .gtf/.gff as GTF/GFF2,
#' .narrow[pP]eak as NarrowPeak, and .interval_list as IntervalList. If none of
#' these match, we fall back to Parquet. These files are written as sharded text
#' files, which can be merged by passing asSingleFile = True.
#'
#' @param ardd The genomic dataset to apply this to.
#' @param filePath The location to write the output.
#' @param asSingleFile If true, merges the sharded output into a single file.
#' @param disableFastConcat If asSingleFile is true, disables the use of the
#'  fast concatenation engine for saving to HDFS.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("save",
          signature(ardd = "FeatureDataset", filePath = "character"),
          function(ardd, filePath,
                   asSingleFile = FALSE, disableFastConcat = FALSE) {
              invisible(sparkR.callJMethod(ardd@jrdd, "save", filePath,
                                           asSingleFile, disableFastConcat))
          })

#' Converts the FeatureDataset to a CoverageDataset.
#'
#' @param ardd The genomic dataset to apply this to.
#' @return Returns a new CoverageDataset.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("toCoverage", signature(ardd = "FeatureDataset"),
          function(ardd) {
              CoverageDataset(sparkR.callJMethod(ardd@jrdd, "toCoverage"))
          })

setMethod("inferConversionFn",
          signature(ardd = "FragmentDataset",
                    destClass = "character"),
          function(ardd, destClass) {
              paste0("org.bdgenomics.adam.api.java.FragmentsTo",
                     destClassSuffix(destClass))
          })

setMethod("replaceRdd",
          signature(ardd = "FragmentDataset",
                    rdd = "jobj"),
          function(ardd, rdd) {
              FragmentDataset(rdd)
          })

#' Splits up the reads in a Fragment back into alignments, and creates a
#' new genomic dataset.
#'
#' @param ardd The genomic dataset to apply this to.
#' @return Returns this genomic dataset converted back to alignments.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("toAlignments", signature(ardd = "FragmentDataset"),
          function(ardd) {
              AlignmentDataset(sparkR.callJMethod(ardd@jrdd, "toAlignments"))
          })

#' Marks reads as possible fragment duplicates.
#'
#' @param ardd The genomic dataset to apply this to.
#' @return A new genomic dataset where reads have the duplicate read flag set. Duplicate
#'   reads are NOT filtered out.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("markDuplicates", signature(ardd = "FragmentDataset"),
          function(ardd) {
              FragmentDataset(sparkR.callJMethod(ardd@jrdd, "markDuplicates"))
          })

#' Saves fragments to Parquet.
#'
#' @param ardd The genomic dataset to apply this to.
#' @param filePath Path to save fragments to.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("save", signature(ardd = "FragmentDataset", filePath = "character"),
          function(ardd, filePath) {
              invisible(sparkR.callJMethod(ardd@jrdd, "save", filePath))
          })

setMethod("inferConversionFn",
          signature(ardd = "GenotypeDataset",
                    destClass = "character"),
          function(ardd, destClass) {
              paste0("org.bdgenomics.adam.api.java.GenotypesTo",
                     destClassSuffix(destClass))
          })

setMethod("replaceRdd",
          signature(ardd = "GenotypeDataset",
                    rdd = "jobj"),
          function(ardd, rdd) {
              GenotypeDataset(rdd)
          })

#' Saves this genomic dataset of genotypes to disk as Parquet.
#'
#' @param ardd The genomic dataset to apply this to.
#' @param filePath Path to save file to.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("saveAsParquet", signature(ardd = "GenotypeDataset", filePath = "character"),
          function(ardd, filePath) {
              invisible(sparkR.callJMethod(ardd@jrdd, "saveAsParquet", filePath))
          })


#' Extracts the variants contained in this genomic dataset of genotypes.
#'
#' Does not perform any filtering looking at whether the variant was called or
#' not. By default, does not deduplicate variants.
#'
#' @param ardd The genomic dataset to apply this to.
#' @param dedupe If true, drops variants described in more than one genotype
#'   record.
#' @return Returns the variants described by this GenotypeDataset.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("toVariants", signature(ardd = "GenotypeDataset"),
          function(ardd, dedupe=FALSE) {
              VariantDataset(sparkR.callJMethod(ardd@jrdd, "toVariants", dedupe))
          })

#' Converts this genomic dataset of Genotypes to VariantContexts.
#'
#' @param ardd The genomic dataset to apply this to.
#' @return Returns this genomic dataset of Genotypes as VariantContexts.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("toVariantContexts", signature(ardd = "GenotypeDataset"),
          function(ardd) {
              VariantContextDataset(sparkR.callJMethod(ardd@jrdd, "toVariantContexts"))
          })

setMethod("inferConversionFn",
          signature(ardd = "SliceDataset",
                    destClass = "character"),
          function(ardd, destClass) {
              paste0("org.bdgenomics.adam.api.java.SlicesTo",
                     destClassSuffix(destClass))
          })

setMethod("replaceRdd",
          signature(ardd = "SliceDataset",
                    rdd = "jobj"),
          function(ardd, rdd) {
              SliceDataset(rdd)
          })

#' Save sequences as Parquet or FASTA.
#'
#' If filename ends in .fa or .fasta, saves as FASTA. If not, saves slices to
#' Parquet. Defaults to 60 character line length, if saving as FASTA.
#'
#' @param ardd The RDD to apply this to.
#' @param filePath Path to save to.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("save", signature(ardd = "SequenceDataset", filePath = "character"),
          function(ardd, filePath) {
              invisible(sparkR.callJMethod(ardd@jrdd, "save", filePath))
          })

#' Save slices as Parquet or FASTA.
#'
#' If filename ends in .fa or .fasta, saves as FASTA. If not, saves slices to
#' Parquet. Defaults to 60 character line length, if saving as FASTA.
#'
#' @param ardd The genomic dataset to apply this to.
#' @param filePath Path to save to.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("save", signature(ardd = "SliceDataset", filePath = "character"),
          function(ardd, filePath) {
              invisible(sparkR.callJMethod(ardd@jrdd, "save", filePath))
          })

#' For all adjacent records in the genomic dataset, we extend the records so that the
#' adjacent records now overlap by _n_ bases, where _n_ is the flank length.
#'
#' @param ardd The genomic dataset to apply this to.
#' @param flankLength The length to extend adjacent records by.
#' @return Returns the genomic dataset, with all adjacent fragments extended with flanking
#'   sequence.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("flankAdjacentFragments",
          signature(ardd = "SliceDataset", flankLength = "numeric"),
          function(ardd, flankLength) {
              SliceDataset(sparkR.callJMethod(ardd@jrdd,
                                          "flankAdjacentFragments",
                                          flankLength))
          })

setMethod("inferConversionFn",
          signature(ardd = "VariantDataset",
                    destClass = "character"),
          function(ardd, destClass) {
              paste0("org.bdgenomics.adam.api.java.VariantsTo",
                     destClassSuffix(destClass))
          })

setMethod("replaceRdd",
          signature(ardd = "VariantDataset",
                    rdd = "jobj"),
          function(ardd, rdd) {
              VariantDataset(rdd)
          })

#' Saves this genomic dataset of variants to disk as Parquet.
#'
#' @param ardd The genomic dataset to apply this to.
#' @param filePath Path to save file to.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("saveAsParquet", signature(ardd = "VariantDataset", filePath = "character"),
          function(ardd, filePath) {
              invisible(sparkR.callJMethod(ardd@jrdd, "saveAsParquet", filePath))
          })

#' Converts this genomic dataset of Variants to VariantContexts.
#'
#' @param ardd The genomic dataset to apply this to.
#' @return Returns this genomic dataset of Variants as VariantContexts.
#'
#' @importFrom SparkR sparkR.callJMethod
#'
#' @export
setMethod("toVariantContexts", signature(ardd = "VariantDataset"),
          function(ardd) {
              VariantContextDataset(sparkR.callJMethod(ardd@jrdd, "toVariantContexts"))
          })

setMethod("replaceRdd",
          signature(ardd = "VariantContextDataset",
                    rdd = "jobj"),
          function(ardd, rdd) {
              VariantContextDataset(rdd)
          })

#' Saves this genomic dataset of variant contexts to disk as VCF.
#'
#' @param ardd The genomic dataset to apply this to.
#' @param filePath Path to save VCF to.
#' @param asSingleFile If true, saves the output as a single file
#'   by merging the sharded output after saving.
#' @param deferMerging If true, saves the output as prepped for merging
#'   into a single file, but does not merge.
#' @param stringency The stringency to use when writing the VCF.
#' @param disableFastConcat If asSingleFile is true, disables the use
#'   of the fast concatenation engine for saving to HDFS.
#'
#' @importFrom SparkR sparkR.callJMethod sparkR.callJStatic
#'
#' @export
setMethod("saveAsVcf", signature(ardd = "VariantContextDataset", filePath = "character"),
          function(ardd,
                   filePath,
                   asSingleFile = TRUE,
                   deferMerging = FALSE,
                   stringency = "LENIENT",
                   disableFastConcat = FALSE) {

              stringency <- sparkR.callJStatic("htsjdk.samtools.ValidationStringency",
                                               "valueOf", stringency)
              
              invisible(sparkR.callJMethod(ardd@jrdd,
                                           "saveAsVcf",
                                           filePath,
                                           asSingleFile,
                                           deferMerging,
                                           disableFastConcat,
                                           stringency))
          })
