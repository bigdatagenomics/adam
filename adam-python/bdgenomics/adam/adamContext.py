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
===========
adamContext
===========
.. currentmodule:: bdgenomics.adam.adamContext
.. autosummary::
   :toctree: _generate/

   ADAMContext
"""

from bdgenomics.adam.rdd import AlignmentRecordDataset, \
    CoverageDataset, \
    FeatureDataset, \
    FragmentDataset, \
    GenotypeDataset, \
    NucleotideContigFragmentDataset, \
    VariantDataset
from bdgenomics.adam.stringency import STRICT, _toJava


class ADAMContext(object):
    """
    The ADAMContext provides functions on top of a SparkContext for loading
    genomic data.
    """


    def __init__(self, ss):
        """
        Initializes an ADAMContext using a SparkSession.

        :param ss: The currently active pyspark.context.SparkContext.
        """

        self._sc = ss.sparkContext
        self._jvm = self._sc._jvm
        c = self._jvm.org.bdgenomics.adam.rdd.ADAMContext.ADAMContextFromSession(ss._jsparkSession)
        self.__jac = self._jvm.org.bdgenomics.adam.api.java.JavaADAMContext(c)


    def loadAlignments(self, filePath, stringency=STRICT):
        """
        Load alignment records into an AlignmentRecordDataset.

        Loads path names ending in:
        * .bam/.cram/.sam as BAM/CRAM/SAM format,
        * .fa/.fasta as FASTA format,
        * .fq/.fastq as FASTQ format, and
        * .ifq as interleaved FASTQ format.

        If none of these match, fall back to Parquet + Avro.

        For FASTA, FASTQ, and interleaved FASTQ formats, compressed files are supported
        through compression codecs configured in Hadoop, which by default include .gz and .bz2,
        but can include more.

        :param str filePath: The path to load the file from.
        :param stringency: The validation stringency to apply. Defaults to STRICT.
        :return: Returns a genomic dataset containing reads.
        :rtype: bdgenomics.adam.rdd.AlignmentRecordDataset
        """

        adamRdd = self.__jac.loadAlignments(filePath,
                                            _toJava(stringency, self._jvm))

        return AlignmentRecordDataset(adamRdd, self._sc)


    def loadIndexedBam(self,
                       filePath,
                       viewRegions,
                       stringency=STRICT):
        """
        Functions like loadAlignments, but uses BAM index files to look at fewer
        blocks, and only returns records within the specified ReferenceRegions.
        BAM index file required.

        :param str pathName: The path name to load indexed BAM formatted
        alignment records from. Globs/directories are supported.
        :param list<ReferenceRegion> viewRegions: List of ReferenceRegion to
        filter on.
        :param int stringency: The validation stringency to use when validating
        the BAM/CRAM/SAM format header. Defaults to ValidationStringency.STRICT.

        :return Returns an AlignmentRecordDataset which wraps the RDD of alignment
        records, sequence dictionary representing contigs the alignment records
        may be aligned to, and the read group dictionary for the alignment
        records if one is available.
        :rtype: bdgenomics.adam.rdd.AlignmentRecordDataset
        """

        # translate reference regions into jvm types
        javaRrs = [rr._toJava(self._jvm) for rr in viewRegions]

        adamRdd = self.__jac.loadIndexedBam(filePath,
                                            javaRrs,
                                            _toJava(stringency, self._jvm))
        return AlignmentRecordDataset(adamRdd, self._sc)


    def loadCoverage(self, filePath,
                     stringency=STRICT):
        """
        Load features into a FeatureDataset and convert to a CoverageDataset.
        Coverage is stored in the score field of Feature.

        Loads path names ending in:
        * .bed as BED6/12 format,
        * .gff3 as GFF3 format,
        * .gtf/.gff as GTF/GFF2 format,
        * .narrow[pP]eak as NarrowPeak format, and
        * .interval_list as IntervalList format.

        If none of these match, fall back to Parquet + Avro.

        For BED6/12, GFF3, GTF/GFF2, NarrowPeak, and IntervalList formats, compressed files
        are supported through compression codecs configured in Hadoop, which by default include
        .gz and .bz2, but can include more.

        :param str filePath: The path to load coverage data from.
        :param stringency: The validation stringency to apply. Defaults to STRICT.
        :return: Returns a genomic dataset containing coverage.
        :rtype: bdgenomics.adam.rdd.CoverageDataset
        """

        adamRdd = self.__jac.loadCoverage(filePath,
                                          _toJava(stringency, self._jvm))

        return CoverageDataset(adamRdd, self._sc)


    def loadContigFragments(self, filePath):
        """
        Load nucleotide contig fragments into a NucleotideContigFragmentDataset.

        If the path name has a .fa/.fasta extension, load as FASTA format.
        Else, fall back to Parquet + Avro.

        For FASTA format, compressed files are supported through compression codecs configured
        in Hadoop, which by default include .gz and .bz2, but can include more.

        :param str filePath: The path to load the file from.
        :return: Returns a genomic dataset containing sequence fragments.
        :rtype: bdgenomics.adam.rdd.NucleotideContigFragmentDataset
        """

        adamRdd = self.__jac.loadContigFragments(filePath)

        return NucleotideContigFragmentDataset(adamRdd, self._sc)


    def loadFragments(self, filePath, stringency=STRICT):
        """
        Load fragments into a FragmentDataset.

        Loads path names ending in:
        * .bam/.cram/.sam as BAM/CRAM/SAM format and
        * .ifq as interleaved FASTQ format.

        If none of these match, fall back to Parquet + Avro.
        For interleaved FASTQ format, compressed files are supported through compression codecs
        configured in Hadoop, which by default include .gz and .bz2, but can include more.

        :param str filePath: The path to load the file from.
        :param stringency: The validation stringency to apply. Defaults to STRICT.
        :return: Returns a genomic dataset containing sequenced fragments.
        :rtype: bdgenomics.adam.rdd.FragmentDataset
        """

        adamRdd = self.__jac.loadFragments(filePath, stringency)

        return FragmentDataset(adamRdd, self._sc)


    def loadFeatures(self, filePath, stringency=STRICT):
        """
        Load features into a FeatureDataset.

        Loads path names ending in:
        * .bed as BED6/12 format,
        * .gff3 as GFF3 format,
        * .gtf/.gff as GTF/GFF2 format,
        * .narrow[pP]eak as NarrowPeak format, and
        * .interval_list as IntervalList format.

        If none of these match, fall back to Parquet + Avro.

        For BED6/12, GFF3, GTF/GFF2, NarrowPeak, and IntervalList formats, compressed files
        are supported through compression codecs configured in Hadoop, which by default include
        .gz and .bz2, but can include more.

        :param str filePath: The path to load the file from.
        :param stringency: The validation stringency to apply. Defaults to STRICT.
        :return: Returns a genomic dataset containing features.
        :rtype: bdgenomics.adam.rdd.FeatureDataset
        """

        adamRdd = self.__jac.loadFeatures(filePath,
                                          _toJava(stringency, self._jvm))

        return FeatureDataset(adamRdd, self._sc)


    def loadGenotypes(self, filePath, stringency=STRICT):
        """
        Load genotypes into a GenotypeDataset.

        If the path name has a .vcf/.vcf.gz/.vcf.bgz extension, load as VCF format.
        Else, fall back to Parquet + Avro.

        :param str filePath: The path to load the file from.
        :param stringency: The validation stringency to apply. Defaults to STRICT.
        :return: Returns a genomic dataset containing genotypes.
        :rtype: bdgenomics.adam.rdd.GenotypeDataset
        """

        adamRdd = self.__jac.loadGenotypes(filePath,
                                           _toJava(stringency, self._jvm))

        return GenotypeDataset(adamRdd, self._sc)


    def loadVariants(self, filePath, stringency=STRICT):
        """
        Load variants into a VariantDataset.

        If the path name has a .vcf/.vcf.gz/.vcf.bgz extension, load as VCF format.
        Else, fall back to Parquet + Avro.

        :param str filePath: The path to load the file from.
        :param stringency: The validation stringency to apply. Defaults to STRICT.
        :return: Returns a genomic dataset containing variants.
        :rtype: bdgenomics.adam.rdd.VariantDataset
        """

        adamRdd = self.__jac.loadVariants(filePath,
                                          _toJava(stringency, self._jvm))

        return VariantDataset(adamRdd, self._sc)
