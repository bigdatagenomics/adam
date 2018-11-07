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
r"""
===
rdd
===
.. currentmodule:: bdgenomics.adam.rdd
.. autosummary::
   :toctree: _generate/

   GenomicDataset
   VCFSupportingGenomicDataset
   AlignmentRecordRDD
   CoverageRDD
   FeatureRDD
   FragmentRDD
   GenotypeRDD
   NucleotideContigFragmentRDD
   VariantRDD
   VariantContextRDD
"""

import logging

from py4j.java_gateway import get_java_class
from pyspark.rdd import RDD
from pyspark.sql import DataFrame, SQLContext

from bdgenomics.adam.stringency import LENIENT, _toJava

_log = logging.getLogger(__name__)

class GenomicDataset(object):
    """
    Wraps an RDD of genomic data with helpful metadata.
    """

    def __init__(self, jvmRdd, sc):
        """
        Constructs a Python GenomicDataset from a JVM GenomicDataset.
        Should not be called from user code; should only be called from
        implementing classes.

        :param jvmRdd: Py4j handle to the underlying JVM GenomicDataset.
        :param pyspark.context.SparkContext sc: Active Spark Context.
        """

        self._jvmRdd = jvmRdd
        self.sc = sc


    def cache(self):
        """
        Caches underlying RDD in memory.

        :return: Returns a new, cached RDD.
        """

        return self._replaceRdd(self._jvmRdd.cache())


    def persist(self, sl):
        """
        Persists underlying RDD in memory or disk.

        :param sl new StorageLevel
        :return: Returns a new, persisted RDD.
        """

        jsl = self.sc._jvm.org.apache.spark.api.java.StorageLevels.create(sl.useDisk,
                                                                          sl.useMemory,
                                                                          sl.useOffHeap,
                                                                          sl.deserialized,
                                                                          sl.replication)

        return self._replaceRdd(self._jvmRdd.persist(jsl))


    def unpersist(self):
        """
        Unpersists underlying RDD from memory or disk.

        :return: Returns a new, unpersisted RDD.
        """

        return self._replaceRdd(self._jvmRdd.unpersist())


    def sort(self):
        """
        Sorts our genome aligned data by reference positions, with contigs ordered
        by index.

        :return: Returns a new, sorted RDD, of the implementing class type.
        """

        return self._replaceRdd(self._jvmRdd.sort())


    def sortLexicographically(self):
        """
        Sorts our genome aligned data by reference positions, with contigs ordered
        lexicographically

        :return: Returns a new, sorted RDD, of the implementing class type.
        """

        return self._replaceRdd(self._jvmRdd.sortLexicographically())


    def filterByOverlappingRegion(self, query):
        """
        Runs a filter that selects data in the underlying RDD that overlaps a
        single genomic region.

        :param query The region to query for.
        :return Returns a new GenomicDataset containing only data that overlaps the
           query region.
        """

        # translate reference regions into jvm types
        javaRr = query._toJava(self.sc._jvm)

        return self._replaceRdd(self._jvmRdd.filterByOverlappingRegion(javaRr))

    def filterByOverlappingRegions(self, querys):
        """
        Runs a filter that selects data in the underlying RDD that overlaps a
        several genomic regions.

        :param list<ReferenceRegion> querys: List of ReferenceRegion to
        filter on.
        :return Returns a new GenomicDataset containing only data that overlaps the
           query regions.
        """

        # translate reference regions into iterator of jvm types
        javaRrs = iter([q._toJava(self.sc._jvm) for q in querys])

        return self._replaceRdd(self._jvmRdd.filterByOverlappingRegions(javaRrs))

    def union(self, rdds):
        """
        Unions together multiple RDDs.

        :param list rdds: The RDDs to union into this RDD.
        :return: Returns a new RDD containing the union of this RDD and the other RDDs.
        """


        return self._replaceRdd(self._jvmRdd.union(map(lambda x: x._jvmRdd,
                                                       rdds)))


    def _wrapTransformation(self,
                            tFn):

        # apply the lambda
        newDf = tFn(self.toDF())

        # wrap it in a conversion function
        jvm = self.sc._jvm
        return jvm.org.bdgenomics.adam.api.python.DataFrameConversionWrapper(newDf._jdf)


    def transform(self, tFn):
        """
        Applies a function that transforms the underlying DataFrame into a new DataFrame
        using the Spark SQL API.

        :param function tFn: A function that transforms the underlying RDD as a DataFrame.
        :return: A new RDD where the RDD of genomic data has been replaced, but the
        metadata (sequence dictionary, and etc) is copied without modification.
        """

        # apply the lambda to the underlying DF
        dfFn = self._wrapTransformation(tFn)

        return self._replaceRdd(self._jvmRdd.transformDataFrame(dfFn))


    def transmute(self, tFn, destClass, convFn=None):
        """
        Applies a function that transmutes the underlying DataFrame into a new RDD of a
        different type.

        :param function tFn: A function that transforms the underlying RDD as a DataFrame.
        :param str convFn: The name of the ADAM GenomicDatasetConversion class to
        use.
        :param class destClass: The destination class of this transmutation.
        :return: A new RDD where the RDD of genomic data has been replaced, but the
        metadata (sequence dictionary, and etc) is copied without modification.
        """

        # apply the lambda to the underlying DF
        dfFn = self._wrapTransformation(tFn)

        # if no conversion function is provided, try to infer
        if convFn is None:
            convFn = self._inferConversionFn(destClass)

        # create an instance of the conversion
        jvm = self.sc._jvm
        convFnInst = getattr(jvm, convFn)()

        return destClass(self._jvmRdd.transmuteDataFrame(dfFn, convFnInst), self.sc)


    def _inferConversionFn(self, destClass):

        raise NotImplementedError("This class does not implement conversion function inference.")


    def _destClassSuffix(self, destClass):

        if destClass is NucleotideContigFragmentRDD:
            return "ContigsDatasetConverter"
        elif destClass is CoverageRDD:
            return "CoverageDatasetConverter"
        elif destClass is FeatureRDD:
            return "FeaturesDatasetConverter"
        elif destClass is FragmentRDD:
            return "FragmentDatasetConverter"
        elif destClass is AlignmentRecordRDD:
            return "AlignmentRecordDatasetConverter"
        elif destClass is GenotypeRDD:
            return "GenotypeDatasetConverter"
        elif destClass is VariantRDD:
            return "VariantDatasetConverter"
        else:
            raise ValueError("No conversion method known for %s." % destClass)


    def pipe(self,
             cmd,
             tFormatter,
             xFormatter,
             convFn,
             files=None,
             environment=None,
             flankSize=0):
        """
        Pipes genomic data to a subprocess that runs in parallel using Spark.

        Files are substituted in to the command with a $x syntax. E.g., to invoke
        a command that uses the first file from the files Seq, use $0. To access
        the path to the directory where the files are copied, use $root.

        Pipes require the presence of an InFormatterCompanion and an OutFormatter
        as implicit values. The InFormatterCompanion should be a singleton whose
        apply method builds an InFormatter given a specific type of GenomicDataset.
        The implicit InFormatterCompanion yields an InFormatter which is used to
        format the input to the pipe, and the implicit OutFormatter is used to
        parse the output from the pipe.

        :param list cmd: The command to run.
        :param str tFormatter: The name of the ADAM in-formatter class to use.
        :param str xFormatter: The name of the ADAM out-formatter class to use.
        :param str convFn: The name of the ADAM GenomicDataset conversion class to
        use.
        :param list files: The files to copy locally onto all executors. Set to
        None (default) to omit.
        :param dict environment: The environment variables to set on the
        executor. Set to None (default) to omit.
        :param int flankSize: The number of bases of flanking sequence to have
        around each partition. Defaults to 0.
        :return: Returns a new RDD where the input from the original RDD has
        been piped through a command that runs locally on each executor.
        """

        jvm = self.sc._jvm

        tFormatterClass = get_java_class(getattr(jvm, tFormatter))

        xFormatterInst = getattr(jvm, xFormatter)()

        convFnInst = getattr(jvm, convFn)()

        if files is None:
            files = []

        if environment is None:
            environment = {}

        return self._replaceRdd(self._jvmRdd.pipe(cmd,
                                                  files,
                                                  environment,
                                                  flankSize,
                                                  tFormatterClass,
                                                  xFormatterInst,
                                                  convFnInst))


    def broadcastRegionJoin(self, genomicRdd, flankSize=0):
        """
        Performs a broadcast inner join between this RDD and another RDD.

        In a broadcast join, the left RDD (this RDD) is collected to the driver,
        and broadcast to all the nodes in the cluster. The key equality function
        used for this join is the reference region overlap function. Since this
        is an inner join, all values who do not overlap a value from the other
        RDD are dropped.

        :param GenomicDataset genomicRdd: The right RDD in the join.
        :param int flankSize: Sets a flankSize for the distance between elements to be
          joined. If set to 0, an overlap is required to join two elements.
        :return: Returns a new genomic RDD containing all pairs of keys that
          overlapped in the genomic coordinate space.
        """

        return GenomicDataset(self._jvmRdd.broadcastRegionJoin(genomicRdd._jvmRdd,
                                                               flankSize),
                              self.sc)


    def rightOuterBroadcastRegionJoin(self, genomicRdd, flankSize=0):
        """
        Performs a broadcast right outer join between this RDD and another RDD.

        In a broadcast join, the left RDD (this RDD) is collected to the driver,
        and broadcast to all the nodes in the cluster. The key equality function
        used for this join is the reference region overlap function. Since this
        is a right outer join, all values in the left RDD that do not overlap a
        value from the right RDD are dropped. If a value from the right RDD does
        not overlap any values in the left RDD, it will be paired with a `None`
        in the product of the join.

        :param GenomicDataset genomicRdd: The right RDD in the join.
        :param int flankSize: Sets a flankSize for the distance between elements to be
          joined. If set to 0, an overlap is required to join two elements.
        :return: Returns a new genomic RDD containing all pairs of keys that
          overlapped in the genomic coordinate space, and all keys from the
          right RDD that did not overlap a key in the left RDD.
        """

        return GenomicDataset(self._jvmRdd.rightOuterBroadcastRegionJoin(genomicRdd._jvmRdd,
                                                                         flankSize),
                              self.sc)


    def broadcastRegionJoinAndGroupByRight(self, genomicRdd, flankSize=0):
        """
        Performs a broadcast inner join between this RDD and another RDD.

        In a broadcast join, the left RDD (this RDD) is collected to the driver,
        and broadcast to all the nodes in the cluster. The key equality function
        used for this join is the reference region overlap function. Since this
        is an inner join, all values who do not overlap a value from the other
        RDD are dropped.

        :param GenomicDataset genomicRdd: The right RDD in the join.
        :param int flankSize: Sets a flankSize for the distance between elements to be
          joined. If set to 0, an overlap is required to join two elements.
        :return: Returns a new genomic RDD containing all pairs of keys that
          overlapped in the genomic coordinate space.
        """

        return GenomicDataset(self._jvmRdd.broadcastRegionJoinAndGroupByRight(genomicRdd._jvmRdd,
                                                                              flankSize),
                              self.sc)


    def rightOuterBroadcastRegionJoinAndGroupByRight(self, genomicRdd, flankSize=0):
        """
        Performs a broadcast right outer join between this RDD and another RDD.

        In a broadcast join, the left side of the join (broadcastTree) is broadcast to
        to all the nodes in the cluster. The key equality function
        used for this join is the reference region overlap function. Since this
        is a right outer join, all values in the left RDD that do not overlap a
        value from the right RDD are dropped. If a value from the right RDD does
        not overlap any values in the left RDD, it will be paired with a `None`
        in the product of the join.

        :param GenomicDataset genomicRdd: The right RDD in the join.
        :param int flankSize: Sets a flankSize for the distance between elements to be
          joined. If set to 0, an overlap is required to join two elements.
        :return: Returns a new genomic RDD containing all pairs of keys that
          overlapped in the genomic coordinate space, and all keys from the
          right RDD that did not overlap a key in the left RDD.
        """

        return GenomicDataset(self._jvmRdd.rightOuterBroadcastRegionJoinAndGroupByRight(genomicRdd._jvmRdd,
                                                                                        flankSize),
                              self.sc)


    def shuffleRegionJoin(self, genomicRdd, flankSize=0):
        """
        Performs a sort-merge inner join between this RDD and another RDD.

        In a sort-merge join, both RDDs are co-partitioned and sorted. The
        partitions are then zipped, and we do a merge join on each partition.
        The key equality function used for this join is the reference region
        overlap function. Since this is an inner join, all values who do not
        overlap a value from the other RDD are dropped.

        :param GenomicDataset genomicRdd: The right RDD in the join.
        :param int flankSize: Sets a flankSize for the distance between elements to be
          joined. If set to 0, an overlap is required to join two elements.
        :return: Returns a new genomic RDD containing all pairs of keys that
          overlapped in the genomic coordinate space.
        """

        return GenomicDataset(self._jvmRdd.shuffleRegionJoin(genomicRdd._jvmRdd, flankSize),
                              self.sc)


    def rightOuterShuffleRegionJoin(self, genomicRdd, flankSize=0):
        """
        Performs a sort-merge right outer join between this RDD and another RDD.

        In a sort-merge join, both RDDs are co-partitioned and sorted. The
        partitions are then zipped, and we do a merge join on each partition.
        The key equality function used for this join is the reference region
        overlap function. Since this is a right outer join, all values in the
        left RDD that do not overlap a value from the right RDD are dropped.
        If a value from the right RDD does not overlap any values in the left
        RDD, it will be paired with a `None` in the product of the join.

        :param GenomicDataset genomicRdd: The right RDD in the join.
        :param int flankSize: Sets a flankSize for the distance between elements to be
          joined. If set to 0, an overlap is required to join two elements.
        :return: Returns a new genomic RDD containing all pairs of keys that
          overlapped in the genomic coordinate space, and all keys from the
          right RDD that did not overlap a key in the left RDD.
        """

        return GenomicDataset(self._jvmRdd.rightOuterShuffleRegionJoin(genomicRdd._jvmRdd, flankSize),
                              self.sc)


    def leftOuterShuffleRegionJoin(self, genomicRdd, flankSize=0):
        """
        Performs a sort-merge left outer join between this RDD and another RDD.

        In a sort-merge join, both RDDs are co-partitioned and sorted. The
        partitions are then zipped, and we do a merge join on each partition.
        The key equality function used for this join is the reference region
        overlap function. Since this is a left outer join, all values in the
        right RDD that do not overlap a value from the left RDD are dropped.
        If a value from the left RDD does not overlap any values in the right
        RDD, it will be paired with a `None` in the product of the join.

        :param GenomicDataset genomicRdd: The right RDD in the join.
        :param int flankSize: Sets a flankSize for the distance between elements to be
          joined. If set to 0, an overlap is required to join two elements.
        :return: Returns a new genomic RDD containing all pairs of keys that
          overlapped in the genomic coordinate space, and all keys from the
          left RDD that did not overlap a key in the left RDD.
        """

        return GenomicDataset(self._jvmRdd.leftOuterShuffleRegionJoin(genomicRdd._jvmRdd, flankSize),
                              self.sc)


    def leftOuterShuffleRegionJoinAndGroupByLeft(self, genomicRdd, flankSize=0):
        """
        Performs a sort-merge left outer join between this RDD and another RDD,
        followed by a groupBy on the left value.

        In a sort-merge join, both RDDs are co-partitioned and sorted. The
        partitions are then zipped, and we do a merge join on each partition.
        The key equality function used for this join is the reference region
        overlap function. Since this is a left outer join, all values in the
        right RDD that do not overlap a value from the left RDD are dropped.
        If a value from the left RDD does not overlap any values in the right
        RDD, it will be paired with an empty Iterable in the product of the join.

        :param GenomicDataset genomicRdd: The right RDD in the join.
        :param int flankSize: Sets a flankSize for the distance between elements to be
          joined. If set to 0, an overlap is required to join two elements.
        :return: Returns a new genomic RDD containing all pairs of keys that
          overlapped in the genomic coordinate space, and all keys from the
          left RDD that did not overlap a key in the left RDD.
        """

        return GenomicDataset(self._jvmRdd.leftOuterShuffleRegionJoinAndGroupByLeft(genomicRdd._jvmRdd, flankSize),
                              self.sc)


    def fullOuterShuffleRegionJoin(self, genomicRdd, flankSize=0):
        """
        Performs a sort-merge full outer join between this RDD and another RDD.

        In a sort-merge join, both RDDs are co-partitioned and sorted. The
        partitions are then zipped, and we do a merge join on each partition.
        The key equality function used for this join is the reference region
        overlap function. Since this is a full outer join, if a value from either
        RDD does not overlap any values in the other RDD, it will be paired with
        a `None` in the product of the join.

        :param GenomicDataset genomicRdd: The right RDD in the join.
        :param int flankSize: Sets a flankSize for the distance between elements to be
          joined. If set to 0, an overlap is required to join two elements.
        :return: Returns a new genomic RDD containing all pairs of keys that
          overlapped in the genomic coordinate space, and values that did not
          overlap will be paired with a `None`.
        """

        return GenomicDataset(self._jvmRdd.fullOuterShuffleRegionJoin(genomicRdd._jvmRdd, flankSize),
                              self.sc)


    def rightOuterShuffleRegionJoinAndGroupByLeft(self, genomicRdd, flankSize=0):
        """
        Performs a sort-merge right outer join between this RDD and another RDD,
        followed by a groupBy on the left value, if not null.

        In a sort-merge join, both RDDs are co-partitioned and sorted. The
        partitions are then zipped, and we do a merge join on each partition.
        The key equality function used for this join is the reference region
        overlap function. In the same operation, we group all values by the left
        item in the RDD. Since this is a right outer join, all values from the
        right RDD who did not overlap a value from the left RDD are placed into
        a length-1 Iterable with a `None` key.

        :param GenomicDataset genomicRdd: The right RDD in the join.
        :param int flankSize: Sets a flankSize for the distance between elements to be
          joined. If set to 0, an overlap is required to join two elements.
        :return: Returns a new genomic RDD containing all pairs of keys that
          overlapped in the genomic coordinate space, grouped together by
          the value they overlapped in the left RDD, and all values from the
          right RDD that did not overlap an item in the left RDD.
        """

        return GenomicDataset(self._jvmRdd.rightOuterShuffleRegionJoinAndGroupByLeft(genomicRdd._jvmRdd, flankSize),
                              self.sc)


    def shuffleRegionJoinAndGroupByLeft(self, genomicRdd, flankSize=0):
        """
        Performs a sort-merge inner join between this RDD and another RDD,
        followed by a groupBy on the left value.

        In a sort-merge join, both RDDs are co-partitioned and sorted. The
        partitions are then zipped, and we do a merge join on each partition.
        The key equality function used for this join is the reference region
        overlap function. In the same operation, we group all values by the left
        item in the RDD.

        :param GenomicDataset genomicRdd: The right RDD in the join.
        :param int flankSize: Sets a flankSize for the distance between elements to be
          joined. If set to 0, an overlap is required to join two elements.
        :return: Returns a new genomic RDD containing all pairs of keys that
          overlapped in the genomic coordinate space, grouped together by
          the value they overlapped in the left RDD.
        """

        return GenomicDataset(self._jvmRdd.shuffleRegionJoinAndGroupByLeft(genomicRdd._jvmRdd, flankSize),
                              self.sc)


    def toDF(self):
        """
        Converts this GenomicDatset into a dataframe.
        :return: Returns a dataframe representing this RDD.
        """

        return DataFrame(self._jvmRdd.toDF(), SQLContext(self.sc))


class VCFSupportingGenomicDataset(GenomicDataset):
    """
    Wraps an GenomicDatset with VCF metadata.
    """

    def __init__(self, jvmRdd, sc):
        """
        Constructs a Python GenomicDataset from a JVM GenomicDataset.
        Should not be called from user code; should only be called from
        implementing classes.

        :param jvmRdd: Py4j handle to the underlying JVM GenomicDataset.
        :param pyspark.context.SparkContext sc: Active Spark Context.
        """

        GenomicDataset.__init__(self, jvmRdd, sc)


    def _javaType(self, lineType):
        """
        Converts a python type into a VCFHeaderLineType enum.

        :param lineType: A Python type.
        """

        jvm = self.sc._jvm

        if lineType == str:
            return jvm.htsjdk.variant.vcf.VCFHeaderLineType.String

        elif lineType == int:
            return jvm.htsjdk.variant.vcf.VCFHeaderLineType.Integer

        elif lineType == float:
            return jvm.htsjdk.variant.vcf.VCFHeaderLineType.Float

        elif lineType == chr:
            return jvm.htsjdk.variant.vcf.VCFHeaderLineType.Character

        elif lineType == bool:
            return jvm.htsjdk.variant.vcf.VCFHeaderLineType.Flag

        else:
            raise ValueError('Invalid type {}. Supported types are str, int, float, chr, bool'.format(lineType))


    def addFixedArrayFormatHeaderLine(self,
                                      name,
                                      count,
                                      description,
                                      lineType):
        """
        Adds a VCF header line describing an array format field, with fixed count.

        :param str name: The identifier for the field.
        :param int count: The number of elements in the array.
        :param str description: A description of the data stored in this format
        field.
        :param lineType: A Python primitive type corresponding to the type of
        data stored in the array. Supported types include str, int, float, and chr.
        :return: A new RDD with the new header line added.
        """

        return self._replaceRdd(self._jvmRdd.addFixedArrayFormatHeaderLine(name,
                                                                           count,
                                                                           self._javaType(lineType),
                                                                           description))


    def addScalarFormatHeaderLine(self,
                                  name,
                                  description,
                                  lineType):
        """
        Adds a VCF header line describing a scalar format field.

        :param str name: The identifier for the field.
        :param str description: A description of the data stored in this format
        field.
        :param lineType: A Python primitive type corresponding to the type of
        data stored in the array. Supported types include str, int, float, and chr.
        :return: A new RDD with the new header line added.
        """

        return self._replaceRdd(self._jvmRdd.addScalarFormatHeaderLine(name,
                                                                       description,
                                                                       self._javaType(lineType)))


    def addGenotypeArrayFormatHeaderLine(self,
                                         name,
                                         description,
                                         lineType):
        """
        Adds a VCF header line describing an 'G' array format field.

        This adds a format field that is an array whose length is equal to the
        number of genotypes for the genotype we are annotating.

        :param str name: The identifier for the field.
        :param str description: A description of the data stored in this format
        field.
        :param lineType: A Python primitive type corresponding to the type of
        data stored in the array. Supported types include str, int, float, and chr.
        :return: A new RDD with the new header line added.
        """

        return self._replaceRdd(self._jvmRdd.addGenotypeArrayFormatHeaderLine(name,
                                                                              description,
                                                                              self._javaType(lineType)))


    def addAlternateAlleleArrayFormatHeaderLine(self,
                                                name,
                                                description,
                                                lineType):
        """
        Adds a VCF header line describing an 'A' array format field.

        This adds a format field that is an array whose length is equal to the
        number of alternate alleles for the genotype we are annotating.

        :param str name: The identifier for the field.
        :param str description: A description of the data stored in this format
        field.
        :param lineType: A Python primitive type corresponding to the type of
        data stored in the array. Supported types include str, int, float, and chr.
        :return: A new RDD with the new header line added.
        """

        return self._replaceRdd(self._jvmRdd.addAlternateAlleleArrayFormatHeaderLine(name,
                                                                                     description,
                                                                                     self._javaType(lineType)))


    def addAllAlleleArrayFormatHeaderLine(self,
                                          name,
                                          description,
                                          lineType):
        """
        Adds a VCF header line describing an 'R' array format field.

        This adds a format field that is an array whose length is equal to the
        total number of alleles (including the reference allele) for the
        genotype we are annotating.

        :param str name: The identifier for the field.
        :param str description: A description of the data stored in this format
        field.
        :param lineType: A Python primitive type corresponding to the type of
        data stored in the array. Supported types include str, int, float, and chr.
        :return: A new RDD with the new header line added.
        """

        return self._replaceRdd(self._jvmRdd.addAllAlleleArrayFormatHeaderLine(name,
                                                                               description,
                                                                               self._javaType(lineType)))


    def addFixedArrayInfoHeaderLine(self,
                                      name,
                                      count,
                                      description,
                                      lineType):
        """
        Adds a VCF header line describing an array info field, with fixed count.

        :param str name: The identifier for the field.
        :param int count: The number of elements in the array.
        :param str description: A description of the data stored in this info
        field.
        :param lineType: A Python primitive type corresponding to the type of
        data stored in the array. Supported types include str, int, float, and chr.
        :return: A new RDD with the new header line added.
        """

        return self._replaceRdd(self._jvmRdd.addFixedArrayInfoHeaderLine(name,
                                                                         count,
                                                                         self._javaType(lineType),
                                                                         description))


    def addScalarInfoHeaderLine(self,
                                  name,
                                  description,
                                  lineType):
        """
        Adds a VCF header line describing a scalar info field.

        :param str name: The identifier for the field.
        :param str description: A description of the data stored in this info
        field.
        :param lineType: A Python primitive type corresponding to the type of
        data stored in the array. Supported types include str, int, float, and chr.
        :return: A new RDD with the new header line added.
        """

        return self._replaceRdd(self._jvmRdd.addScalarInfoHeaderLine(name,
                                                                     description,
                                                                     self._javaType(lineType)))


    def addAlternateAlleleArrayInfoHeaderLine(self,
                                                name,
                                                description,
                                                lineType):
        """
        Adds a VCF header line describing an 'A' array info field.

        This adds a info field that is an array whose length is equal to the
        number of alternate alleles for the genotype we are annotating.

        :param str name: The identifier for the field.
        :param str description: A description of the data stored in this info
        field.
        :param lineType: A Python primitive type corresponding to the type of
        data stored in the array. Supported types include str, int, float, and chr.
        :return: A new RDD with the new header line added.
        """

        return self._replaceRdd(self._jvmRdd.addAlternateAlleleArrayInfoHeaderLine(name,
                                                                                   description,
                                                                                   self._javaType(lineType)))


    def addAllAlleleArrayInfoHeaderLine(self,
                                          name,
                                          description,
                                          lineType):
        """
        Adds a VCF header line describing an 'R' array info field.

        This adds a info field that is an array whose length is equal to the
        total number of alleles (including the reference allele) for the
        genotype we are annotating.

        :param str name: The identifier for the field.
        :param str description: A description of the data stored in this info
        field.
        :param lineType: A Python primitive type corresponding to the type of
        data stored in the array. Supported types include str, int, float, and chr.
        :return: A new RDD with the new header line added.
        """

        return self._replaceRdd(self._jvmRdd.addAllAlleleArrayInfoHeaderLine(name,
                                                                             description,
                                                                             self._javaType(lineType)))


    def addFilterHeaderLine(self,
                            name,
                            description):
        """
        Adds a VCF header line describing a variant/genotype filter.

        :param str id: The identifier for the filter.
        :param str description: A description of the filter.
        :return: A new RDD with the new header line added.
        """

        return self._replaceRdd(self._jvmRdd.addFilterHeaderLine(name, description))


class AlignmentRecordRDD(GenomicDataset):
    """
    Wraps an GenomicDatset with Alignment Record metadata and functions.
    """

    def __init__(self, jvmRdd, sc):
        """
        Constructs a Python AlignmentRecordRDD from a JVM AlignmentRecordRDD.
        Should not be called from user code; instead, go through
        bdgenomics.adamContext.ADAMContext.

        :param jvmRdd: Py4j handle to the underlying JVM AlignmentRecordRDD.
        :param pyspark.context.SparkContext sc: Active Spark Context.
        """

        GenomicDataset.__init__(self, jvmRdd, sc)


    def _replaceRdd(self, newRdd):

        return AlignmentRecordRDD(newRdd, self.sc)


    def _inferConversionFn(self, destClass):

        return "org.bdgenomics.adam.api.java.AlignmentRecordsTo%s" % self._destClassSuffix(destClass)


    def toFragments(self):
        """
        Convert this set of reads into fragments.

        :return: Returns a FragmentRDD where all reads have been grouped
        together by the original sequence fragment they come from.
        :rtype: bdgenomics.adam.rdd.FragmentRDD
        """

        return FragmentRDD(self._jvmRdd.toFragments(), self.sc)


    def toCoverage(self, collapse = True):
        """
        Converts this set of reads into a corresponding CoverageRDD.

        :param bool collapse: Determines whether to merge adjacent coverage
        elements with the same score to a single coverage observation.
        :return: Returns an RDD with observed coverage.
        :rtype: bdgenomics.adam.rdd.CoverageRDD
        """

        coverageRDD = CoverageRDD(self._jvmRdd.toCoverage(), self.sc)
        if (collapse):
            return coverageRDD.collapse()
        else:
            return coverageRDD


    def save(self, filePath, isSorted = False):
        """
        Saves this RDD to disk, with the type identified by the extension.

        :param str filePath: The path to save the file to.
        :param bool isSorted: Whether the file is sorted or not.
        """

        self._jvmRdd.save(filePath, isSorted)


    def saveAsSam(self,
                  filePath,
                  asType=None,
                  isSorted=False,
                  asSingleFile=False):
        """
        Saves this RDD to disk as a SAM/BAM/CRAM file.

        :param str filePath: The path to save the file to.
        :param str asType: The type of file to save. Valid choices are SAM, BAM,
        CRAM, and None. If None, the file type is inferred from the extension.
        :param bool isSorted: Whether the file is sorted or not.
        :param bool asSingleFile: Whether to save the file as a single merged
        file or as shards.
        """

        if asType is None:

            fileType = self.sc._jvm.org.seqdoop.hadoop_bam.SAMFormat.inferFromFilePath(filePath)

        else:

            fileType = self.sc._jvm.org.seqdoop.hadoop_bam.SAMFormat.valueOf(asType)

        self._jvmRdd.saveAsSam(filePath, fileType, asSingleFile, isSorted)


    def saveAsSamString(self):
        """
        Converts an RDD into the SAM spec string it represents.

        This method converts an RDD of AlignmentRecords back to an RDD of
        SAMRecordWritables and a SAMFileHeader, and then maps this RDD into a
        string on the driver that represents this file in SAM.

        :return: A string on the driver representing this RDD of reads in SAM format.
        :rtype: str
        """

        return self._jvmRdd.saveAsSamString()


    def countKmers(self, kmerLength):
        """
        Cuts reads into _k_-mers, and then counts the number of occurrences of each _k_-mer.

        :param int kmerLength: The value of _k_ to use for cutting _k_-mers.
        :return: Returns an RDD containing k-mer/count pairs.
        :rtype: DataFrame containing "kmer" string and "count" long.
        """

        return DataFrame(self._jvmRdd.countKmersAsDataset(kmerLength).toDF(),
                         SQLContext(self.sc))


    def sortReadsByReferencePosition(self):
        """
        Sorts our read data by reference positions, with contigs ordered by name.

        Sorts reads by the location where they are aligned. Unaligned reads are
        put at the end and sorted by read name. Contigs are ordered
        lexicographically by name.

        :return: Returns a new RDD containing sorted reads.
        :rtype: bdgenomics.adam.rdd.AlignmentRecordRDD
        """

        return AlignmentRecordRDD(self._jvmRdd.sortReadsByReferencePosition(),
                                  self.sc)


    def sortReadsByReferencePositionAndIndex(self):
        """
        Sorts our read data by reference positions, with contigs ordered by index.

        Sorts reads by the location where they are aligned. Unaligned reads are
        put at the end and sorted by read name. Contigs are ordered by index
        that they are ordered in the sequence metadata.

        :return: Returns a new RDD containing sorted reads.
        :rtype: bdgenomics.adam.rdd.AlignmentRecordRDD
        """

        return AlignmentRecordRDD(self._jvmRdd.sortReadsByReferencePositionAndIndex(),
                                  self.sc)


    def markDuplicates(self):
        """
        Marks reads as possible fragment duplicates.

        :return: A new RDD where reads have the duplicate read flag set.
        Duplicate reads are NOT filtered out.
        :rtype: bdgenomics.adam.rdd.AlignmentRecordRDD
        """

        return AlignmentRecordRDD(self._jvmRdd.markDuplicates(),
                                  self.sc)


    def recalibrateBaseQualities(self,
                                 knownSnps,
                                 validationStringency = LENIENT):
        """
        Runs base quality score recalibration on a set of reads. Uses a table of
        known SNPs to mask true variation during the recalibration process.

        :param bdgenomics.adam.rdd.VariantRDD knownSnps: A table of known SNPs to mask valid variants.
        :param bdgenomics.adam.stringency validationStringency:
        """

        return AlignmentRecordRDD(self._jvmRdd.recalibrateBaseQualities(knownSnps._jvmRdd,
                                                                         _toJava(validationStringency, self.sc._jvm)))


    def realignIndels(self,
                      isSorted = False,
                      maxIndelSize = 500,
                      maxConsensusNumber = 30,
                      lodThreshold = 5.0,
                      maxTargetSize = 3000):
        """
        Realigns indels using a concensus-based heuristic.

        Generates consensuses from reads.

        :param bool isSorted: If the input data is sorted, setting this
        parameter to true avoids a second sort.
        :param int maxIndelSize: The size of the largest indel to use for
        realignment.
        :param int maxConsensusNumber: The maximum number of consensus sequences
        to realign against per target region.
        :param float lodThreshold: Log-odds threshold to use when realigning;
        realignments are only finalized if the log-odds threshold is exceeded.
        :param int maxTargetSize: The maximum width of a single target region
        for realignment.
        :return: Returns an RDD of mapped reads which have been realigned.
        :rtype: bdgenomics.adam.rdd.AlignmentRecordRDD
        """

        consensusModel = self.sc._jvm.org.bdgenomics.adam.algorithms.consensus.ConsensusGenerator.fromReads()
        return AlignmentRecordRDD(self._jvmRdd.realignIndels(consensusModel,
                                                             isSorted,
                                                             maxIndelSize,
                                                             maxConsensusNumber,
                                                             lodThreshold,
                                                             maxTargetSize),
                                  self.sc)


    def realignIndels(self,
                      knownIndels,
                      isSorted = False,
                      maxIndelSize = 500,
                      maxConsensusNumber = 30,
                      lodThreshold = 5.0,
                      maxTargetSize = 3000):
        """
        Realigns indels using a concensus-based heuristic.

        Generates consensuses from prior called INDELs.

        :param bdgenomics.adam.rdd.VariantRDD knownIndels: An RDD of previously
        called INDEL variants.
        :param bool isSorted: If the input data is sorted, setting this
        parameter to true avoids a second sort.
        :param int maxIndelSize: The size of the largest indel to use for
        realignment.
        :param int maxConsensusNumber: The maximum number of consensus sequences
        to realign against per target region.
        :param float lodThreshold: Log-odds threshold to use when realigning;
        realignments are only finalized if the log-odds threshold is exceeded.
        :param int maxTargetSize: The maximum width of a single target region
        for realignment.
        :return: Returns an RDD of mapped reads which have been realigned.
        :rtype: bdgenomics.adam.rdd.AlignmentRecordRDD
        """

        consensusModel = self.sc._jvm.org.bdgenomics.adam.algorithms.consensus.ConsensusGenerator.fromKnowns(knownIndels._jvmRdd)
        return AlignmentRecordRDD(self._jvmRdd.realignIndels(consensusModel,
                                                             isSorted,
                                                             maxIndelSize,
                                                             maxConsensusNumber,
                                                             lodThreshold,
                                                             maxTargetSize),
                                  self.sc)

    def flagStat(self):
        """
        Runs a quality control pass akin to the Samtools FlagStat tool.

        :return: Returns a tuple of (failedQualityMetrics, passedQualityMetrics)
        """

        return self._jvmRdd.flagStat()


    def saveAsPairedFastq(self,
                          fileName1,
                          fileName2,
                          persistLevel,
                          outputOriginalBaseQualities = False,
                          validationStringency = LENIENT):
        """
        Saves these AlignmentRecords to two FASTQ files.

        The files are one for the first mate in each pair, and the other for the
        second mate in the pair.

        :param str fileName1: Path at which to save a FASTQ file containing the
        first mate of each pair.
        :param str fileName2: Path at which to save a FASTQ file containing the
        second mate of each pair.
        :param bool outputOriginalBaseQualities: If true, writes out reads with
        the base qualities from the original qualities (SAM "OQ") field. If
        false, writes out reads with the base qualities from the qual field.
        Default is false.
        :param bdgenomics.adam.stringency validationStringency: If strict, throw
        an exception if any read in this RDD is not accompanied by its mate.
        :param pyspark.storagelevel.StorageLevel persistLevel: The persistance
        level to cache reads at between passes.
        """

        self._jvmRdd.saveAsPairedFastq(fileName1, fileName2,
                                        outputOriginalBaseQualities,
                                        _toJava(validationStringency, self.sc._jvm),
                                        persistLevel)


    def saveAsFastq(self,
                    fileName,
                    validationStringency = LENIENT,
                    sort = False,
                    outputOriginalBaseQualities = False):
        """
        Saves reads in FASTQ format.

        :param str fileName: Path to save files at.
        :param bdgenomics.adam.stringency validationStringency: If strict, throw
        an exception if any read in this RDD is not accompanied by its mate.
        :param bool sort: Whether to sort the FASTQ files by read name or not.
        Defaults to false. Sorting the output will recover pair order, if
        desired.
        :param bool outputOriginalBaseQualities: If true, writes out reads with
        the base qualities from the original qualities (SAM "OQ") field. If
        false, writes out reads with the base qualities from the qual field.
        Default is false.
        """

        self._jvmRdd.saveAsFastq(fileName,
                                  outputOriginalBaseQualities,
                                  sort,
                                  _toJava(validationStringency, self.sc._jvm))


    def reassembleReadPairs(self,
                            secondPairRdd,
                            validationStringency = LENIENT):
        """
        Reassembles read pairs from two sets of unpaired reads.

        The assumption is that the two sets were _originally_ paired together.
        The RDD that this is called on should be the RDD with the first read
        from the pair.

        :param pyspark.rdd.RDD secondPairRdd: The rdd containing the second read
        from the pairs.
        :param bdgenomics.adam.stringency validationStringency: How stringently
        to validate the reads.
        :return: Returns an RDD with the pair information recomputed.
        :rtype: bdgenomics.adam.rdd.AlignmentRecordRDD
        """

        return AlignmentRecordRDD(self._jvmRdd.reassembleReadPairs(rdd._jrdd,
                                                                    _toJava(validationStringency, self.sc._jvm)),
                                  self.sc)


class CoverageRDD(GenomicDataset):
    """
    Wraps an GenomicDatset with Coverage metadata and functions.
    """

    def _replaceRdd(self, newRdd):

        return CoverageRDD(newRdd, self.sc)


    def __init__(self, jvmRdd, sc):
        """
        Constructs a Python CoverageRDD from a JVM CoverageRDD.
        Should not be called from user code; instead, go through
        bdgenomics.adamContext.ADAMContext.

        :param jvmRdd: Py4j handle to the underlying JVM CoverageRDD.
        :param pyspark.context.SparkContext sc: Active Spark Context.
        """

        GenomicDataset.__init__(self, jvmRdd, sc)


    def save(self, filePath, asSingleFile = False, disableFastConcat = False):
        """
        Saves coverage as feature file.

        :param str filePath: The location to write the output.
        :param bool asSingleFile: If true, merges the sharded output into a
        single file.
        """

        self._jvmRdd.save(filePath, asSingleFile, disableFastConcat)


    def collapse(self):
        """
        Merges adjacent ReferenceRegions with the same coverage value.

        This reduces the loss of coverage information while reducing the number
        of records in the RDD. For example, adjacent records Coverage("chr1", 1,
        10, 3.0) and Coverage("chr1", 10, 20, 3.0) would be merged into one
        record Coverage("chr1", 1, 20, 3.0).

        :return: An RDD with merged tuples of adjacent sites with same coverage.
        :rtype: bdgenomics.adam.rdd.CoverageRDD
        """

        return CoverageRDD(self._jvmRdd.collapse(), self.sc)


    def toFeatures(self):
        """
        Converts CoverageRDD to FeatureRDD.

        :return: Returns a FeatureRDD from CoverageRDD.
        :rtype: bdgenomics.adam.rdd.FeatureRDD
        """

        return FeatureRDD(self._jvmRdd.toFeatures(), self.sc)


    def coverage(self, bpPerBin = 1):
        """
        Gets coverage overlapping specified ReferenceRegion.

        For large ReferenceRegions, base pairs per bin (bpPerBin) can be
        specified to bin together ReferenceRegions of equal size. The coverage
        of each bin is the coverage of the first base pair in that bin.

        :param int bpPerBin: Number of bases to combine to one bin.
        :return: Returns a sparsified CoverageRDD.
        :rtype: bdgenomics.adam.rdd.CoverageRDD
        """

        return CoverageRDD(self._jvmRdd.coverage(bpPerBin), self.sc)


    def aggregatedCoverage(self, bpPerBin = 1):
        """
        Gets coverage overlapping specified ReferenceRegion.

        For large ReferenceRegions, base pairs per bin (bpPerBin) can be
        specified to bin together ReferenceRegions of equal size. The coverage
        of each bin is the average coverage of the bases in that bin.

        :param int bpPerBin: Number of bases to combine to one bin.
        :return: Returns a sparsified CoverageRDD.
        :rtype: bdgenomics.adam.rdd.CoverageRDD
        """

        return CoverageRDD(self._jvmRdd.aggregatedCoverage(bpPerBin), self.sc)


    def flatten(self):
        """
        Gets flattened RDD of coverage, with coverage mapped to each base pair.

        The opposite operation of collapse.

        :return: New CoverageRDD of flattened coverage.
        :rtype: bdgenomics.adam.rdd.CoverageRDD
        """

        return CoverageRDD(self._jvmRdd.flatten(), self.sc)


    def _inferConversionFn(self, destClass):

        return "org.bdgenomics.adam.api.java.CoverageTo%s" % self._destClassSuffix(destClass)


class FeatureRDD(GenomicDataset):
    """
    Wraps an GenomicDatset with Feature metadata and functions.
    """

    def _replaceRdd(self, newRdd):

        return FeatureRDD(newRdd, self.sc)


    def __init__(self, jvmRdd, sc):
        """
        Constructs a Python FeatureRDD from a JVM FeatureRDD.
        Should not be called from user code; instead, go through
        bdgenomics.adamContext.ADAMContext.

        :param jvmRdd: Py4j handle to the underlying JVM FeatureRDD.
        :param pyspark.context.SparkContext sc: Active Spark Context.
        """

        GenomicDataset.__init__(self, jvmRdd, sc)


    def save(self, filePath, asSingleFile = False, disableFastConcat = False):
        """
        Saves coverage, autodetecting the file type from the extension.

        Writes files ending in .bed as BED6/12, .gff3 as GFF3, .gtf/.gff as
        GTF/GFF2, .narrow[pP]eak as NarrowPeak, and .interval_list as
        IntervalList. If none of these match, we fall back to Parquet.
        These files are written as sharded text files, which can be merged by
        passing asSingleFile = True.

        :param str filePath: The location to write the output.
        :param bool asSingleFile: If true, merges the sharded output into a
        single file.
        :param bool disableFastConcat: If asSingleFile is true, disables the use
        of the fast concatenation engine for saving to HDFS.
        """

        self._jvmRdd.save(filePath, asSingleFile, disableFastConcat)


    def toCoverage(self):
        """
        Converts the FeatureRDD to a CoverageRDD.

        :return: Returns a new CoverageRDD.
        :rtype: bdgenomics.adam.rdd.CoverageRDD.
        """

        return CoverageRDD(self._jvmRdd.toCoverage(), self.sc)


    def _inferConversionFn(self, destClass):

        return "org.bdgenomics.adam.api.java.FeaturesTo%s" % self._destClassSuffix(destClass)


class FragmentRDD(GenomicDataset):
    """
    Wraps an GenomicDatset with Fragment metadata and functions.
    """

    def _replaceRdd(self, newRdd):

        return FragmentRDD(newRdd, self.sc)


    def __init__(self, jvmRdd, sc):
        """
        Constructs a Python FragmentRDD from a JVM FragmentRDD.
        Should not be called from user code; instead, go through
        bdgenomics.adamContext.ADAMContext.

        :param jvmRdd: Py4j handle to the underlying JVM FragmentRDD.
        :param pyspark.context.SparkContext sc: Active Spark Context.
        """

        GenomicDataset.__init__(self, jvmRdd, sc)


    def toReads(self):
        """
        Splits up the reads in a Fragment, and creates a new RDD.

        :return: Returns this RDD converted back to reads.
        :rtype: bdgenomics.adam.rdd.AlignmentRecordRDD
        """

        return AlignmentRecordRDD(self._jvmRdd.toReads(), self.sc)


    def markDuplicates(self):
        """
        Marks reads as possible fragment duplicates.

        :return: A new RDD where reads have the duplicate read flag set.
        Duplicate reads are NOT filtered out.
        :rtype: bdgenomics.adam.rdd.FragmentRDD
        """

        return FragmentRDD(self._jvmRdd.markDuplicates(), self.sc)


    def save(self, filePath):
        """
        Saves fragments to Parquet.

        :param str filePath: Path to save fragments to.
        """

        self._jvmRdd.save(filePath)


    def _inferConversionFn(self, destClass):

        return "org.bdgenomics.adam.api.java.FragmentsTo%s" % self._destClassSuffix(destClass)


class GenotypeRDD(VCFSupportingGenomicDataset):
    """
    Wraps an GenomicDatset with Genotype metadata and functions.
    """

    def _replaceRdd(self, newRdd):

        return GenotypeRDD(newRdd, self.sc)


    def __init__(self, jvmRdd, sc):
        """
        Constructs a Python GenotypeRDD from a JVM GenotypeRDD.
        Should not be called from user code; instead, go through
        bdgenomics.adamContext.ADAMContext.

        :param jvmRdd: Py4j handle to the underlying JVM GenotypeRDD.
        :param pyspark.context.SparkContext sc: Active Spark Context.
        """

        GenomicDataset.__init__(self, jvmRdd, sc)


    def saveAsParquet(self, filePath):
        """
        Saves this RDD of genotypes to disk as Parquet.

        :param str filePath: Path to save file to.
        """

        self._jvmRdd.saveAsParquet(filePath)


    def toVariantContexts(self):
        """
        :return: These genotypes, converted to variant contexts.
        """

        vcs = self._jvmRdd.toVariantContexts()
        return VariantContextRDD(vcs, self.sc)


    def toVariants(self, dedupe=False):
        """
        Extracts the variants contained in this RDD of genotypes.

        Does not perform any filtering looking at whether the variant was called
        or not. By default, does not deduplicate variants.

        :param bool dedupe: If true, drops variants described in more than one
        genotype record.
        :return: Returns the variants described by this GenotypeRDD.
        """

        return VariantRDD(self._jvmRdd.toVariants(dedupe), self.sc)


    def _inferConversionFn(self, destClass):

        return "org.bdgenomics.adam.api.java.GenotypesTo%s" % self._destClassSuffix(destClass)


class NucleotideContigFragmentRDD(GenomicDataset):
    """
    Wraps an GenomicDatset with Nucleotide Contig Fragment metadata and functions.
    """

    def _replaceRdd(self, newRdd):

        return NucleotideContigFragmentRDD(newRdd, self.sc)


    def __init__(self, jvmRdd, sc):
        """
        Constructs a Python NucleotideContigFragmentRDD from a JVM
        NucleotideContigFragmentRDD. Should not be called from user code;
        instead, go through bdgenomics.adamContext.ADAMContext.

        :param jvmRdd: Py4j handle to the underlying JVM NucleotideContigFragmentRDD.
        :param pyspark.context.SparkContext sc: Active Spark Context.
        """

        GenomicDataset.__init__(self, jvmRdd, sc)


    def save(self, fileName):
        """
        Save nucleotide contig fragments as Parquet or FASTA.

        If filename ends in .fa or .fasta, saves as Fasta. If not, saves
        fragments to Parquet. Defaults to 60 character line length, if saving to
        FASTA.

        :param str fileName: Path to save to.
        """

        self._jvmRdd.save(fileName)


    def flankAdjacentFragments(self, flankLength):
        """
        For all adjacent records in the RDD, we extend the records so that the
        adjacent records now overlap by _n_ bases, where _n_ is the flank
        length.

        :param int flankLength: The length to extend adjacent records by.
        :return: Returns the RDD, with all adjacent fragments extended with
        flanking sequence.
        :rtype: bdgenomics.adam.rdd.NucleotideContigFragmentRDD
        """

        return NucleotideContigFragmentRDD(self._jvmRdd.flankAdjacentFragments(flankLength),
                                           self.sc)


    def countKmers(self, kmerLength):
        """
        Counts the k-mers contained in a FASTA contig.

        :param int kmerLength: The value of _k_ to use for cutting _k_-mers.
        :return: Returns an RDD containing k-mer/count pairs.
        :rtype: pyspark.rdd.RDD[str,long]
        """

        return RDD(self._jvmRdd.countKmers(kmerLength), self.sc)


    def _inferConversionFn(self, destClass):

        return "org.bdgenomics.adam.api.java.ContigsTo%s" % self._destClassSuffix(destClass)


class VariantRDD(VCFSupportingGenomicDataset):
    """
    Wraps an GenomicDatset with Variant metadata and functions.
    """

    def _replaceRdd(self, newRdd):

        return VariantRDD(newRdd, self.sc)


    def __init__(self, jvmRdd, sc):
        """
        Constructs a Python VariantRDD from a JVM VariantRDD.
        Should not be called from user code; instead, go through
        bdgenomics.adamContext.ADAMContext.

        :param jvmRdd: Py4j handle to the underlying JVM VariantRDD.
        :param pyspark.context.SparkContext sc: Active Spark Context.
        """

        GenomicDataset.__init__(self, jvmRdd, sc)


    def toVariantContexts(self):
        """
        :return: These variants, converted to variant contexts.
        """

        vcs = self._jvmRdd.toVariantContexts()
        return VariantContextRDD(vcs, self.sc)


    def saveAsParquet(self, filePath):
        """
        Saves this RDD of variants to disk as Parquet.

        :param str filePath: Path to save file to.
        """

        self._jvmRdd.saveAsParquet(filePath)


    def _inferConversionFn(self, destClass):

        return "org.bdgenomics.adam.api.java.VariantsTo%s" % self._destClassSuffix(destClass)


class VariantContextRDD(VCFSupportingGenomicDataset):
    """
    Wraps an GenomicDataset with Variant Context metadata and functions.
    """

    def _replaceRdd(self, newRdd):

        return VariantContextRDD(newRdd, self.sc)


    def __init__(self, jvmRdd, sc):
        """
        Constructs a Python VariantContextRDD from a JVM VariantContextRDD.
        Should not be called from user code; instead, go through
        bdgenomics.adamContext.ADAMContext.

        :param jvmRdd: Py4j handle to the underlying JVM VariantContextRDD.
        :param pyspark.context.SparkContext sc: Active Spark Context.
        """

        VCFSupportingGenomicDataset.__init__(self, jvmRdd, sc)


    def saveAsVcf(self,
                  filePath,
                  asSingleFile=True,
                  deferMerging=False,
                  stringency=LENIENT,
                  disableFastConcat=False):
        """
        Saves this RDD of variants to disk as VCF.

        :param str filePath: Path to save file to.
        :param bool asSingleFile: If true, saves the output as a single file
        by merging the sharded output after saving.
        :param bool deferMerging: If true, saves the output as prepped for merging
        into a single file, but does not merge.
        :param bdgenomics.adam.stringency stringency: The stringency to use
        when writing the VCF.
        :param bool disableFastConcat: If asSingleFile is true, disables the use
        of the fast concatenation engine for saving to HDFS.
        """

        self._jvmRdd.saveAsVcf(filePath,
                               asSingleFile,
                               deferMerging,
                               disableFastConcat,
                               _toJava(stringency, self.sc._jvm))
