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


from pyspark.rdd import RDD
from pyspark.sql import DataFrame, SQLContext

from bdgenomics.adam.stringency import LENIENT, _toJava


class GenomicRDD(object):


    def __init__(self, jvmRdd, sc):
        """
        Constructs a Python GenomicRDD from a JVM GenomicRDD.
        Should not be called from user code; should only be called from
        implementing classes.

        :param jvmRdd: Py4j handle to the underlying JVM GenomicRDD.
        :param pyspark.context.SparkContext sc: Active Spark Context.
        """

        self._jvmRdd = jvmRdd
        self.sc = sc


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

        return self._replaceRdd(self._jvmRdd.sort())


    def union(self, rdds):
        """
        Unions together multiple RDDs.

        :param list rdds: The RDDs to union into this RDD.
        :return: Returns a new RDD containing the union of this RDD and the other RDDs.
        """
        

        return self._replaceRdd(self._jvmRdd.union(map(lambda x: x._jvmRdd,
                                                       rdds)))
    

class AlignmentRecordRDD(GenomicRDD):


    def __init__(self, jvmRdd, sc):
        """
        Constructs a Python AlignmentRecordRDD from a JVM AlignmentRecordRDD.
        Should not be called from user code; instead, go through
        bdgenomics.adamContext.ADAMContext.

        :param jvmRdd: Py4j handle to the underlying JVM AlignmentRecordRDD.
        :param pyspark.context.SparkContext sc: Active Spark Context.
        """

        GenomicRDD.__init__(self, jvmRdd, sc)


    def _replaceRdd(self, newRdd):

        return AlignmentRecordRDD(newRdd, self.sc)

    
    def toDF(self):
        """
        :return: Returns a dataframe representing this RDD.
        """
        
        return DataFrame(self._jvmRdd.toDF(), SQLContext(self.sc))


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

        return CoverageRDD(self._jvmRdd.toCoverage(collapse), self.sc)


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

            type = self.sc._jvm.org.seqdoop.hadoop_bam.SAMFormat.inferFromFilePath(filePath)

        else:

            type = self.sc._jvm.org.seqdoop.hadoop_bam.SAMFormat.valueOf(asType)

        self._jvmRdd.saveAsSam(filePath, type, asSingleFile, isSorted)

        
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


class CoverageRDD(GenomicRDD):


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

        GenomicRDD.__init__(self, jvmRdd, sc)


    def toDF(self):
        """
        :return: Returns a dataframe representing this RDD.
        """
        
        return DataFrame(self._jvmRdd.toDF(), SQLContext(self.sc))


    def save(self, filePath, asSingleFile = False):
        """
        Saves coverage as feature file.

        :param str filePath: The location to write the output.
        :param bool asSingleFile: If true, merges the sharded output into a
        single file.
        """

        self._jvmRdd.save(filePath, asSingleFile)


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

    
    def toFeatureRDD(self):
        """
        Converts CoverageRDD to FeatureRDD.

        :return: Returns a FeatureRDD from CoverageRDD.
        :rtype: bdgenomics.adam.rdd.FeatureRDD
        """
        
        return FeatureRDD(self._jvmRdd.toFeatureRDD(), self.sc)


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


class FeatureRDD(GenomicRDD):


    def __init__(self, jvmRdd, sc):
        """
        Constructs a Python FeatureRDD from a JVM FeatureRDD.
        Should not be called from user code; instead, go through
        bdgenomics.adamContext.ADAMContext.

        :param jvmRdd: Py4j handle to the underlying JVM FeatureRDD.
        :param pyspark.context.SparkContext sc: Active Spark Context.
        """

        GenomicRDD.__init__(self, jvmRdd, sc)


    def toDF(self):
        """
        :return: Returns a dataframe representing this RDD.
        """
        
        return DataFrame(self._jvmRdd.toDF(), SQLContext(self.sc))


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


class FragmentRDD(GenomicRDD):


    def _replaceRdd(self, newRdd):

        return FragmentRDD(newRdd, self.sc)


    def __init__(self, jvmRdd):
        """
        Constructs a Python FragmentRDD from a JVM FragmentRDD.
        Should not be called from user code; instead, go through
        bdgenomics.adamContext.ADAMContext.

        :param jvmRdd: Py4j handle to the underlying JVM FragmentRDD.
        :param pyspark.context.SparkContext sc: Active Spark Context.
        """

        GenomicRDD.__init__(self, jvmRdd, sc)

        
    def toDF(self):
        """
        :return: Returns a dataframe representing this RDD.
        """
        
        return DataFrame(self._jvmRdd.toDF(), SQLContext(self.sc))


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


class GenotypeRDD(GenomicRDD):


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

        GenomicRDD.__init__(self, jvmRdd, sc)


    def toDF(self):
        """
        :return: Returns a dataframe representing this RDD.
        """
        
        return DataFrame(self._jvmRdd.toDF(), SQLContext(self.sc))

        
    def save(self, filePath):
        """
        Saves this RDD of genotypes to disk.

        :param str filePath: Path to save file to. If ends in ".vcf", saves as
        VCF, else saves as Parquet.
        """

        if (filePath.endswith(".vcf")):
            self.saveAsVcf(filePath)
        else:
            self._jvmRdd.saveAsParquet(filePath)


    def saveAsVcf(self,
                  filePath,
                  asSingleFile=True,
                  deferMerging=False,
                  stringency=LENIENT,
                  sortOnSave=None,
                  disableFastConcat=False):
        """
        Saves this RDD of genotypes to disk as VCF.

        :param str filePath: Path to save file to.
        :param bool asSingleFile: If true, saves the output as a single file
        by merging the sharded output after saving.
        :param bool deferMerging: If true, saves the output as prepped for merging
        into a single file, but does not merge.
        :param bdgenomics.adam.stringency stringency: The stringency to use
        when writing the VCF.
        :param bool sortOnSave: Whether to sort when saving. If None, does not
        sort. If True, sorts by contig index. If "lexicographically", sorts by
        contig name.
        :param bool disableFastConcat: If asSingleFile is true, disables the use
        of the fast concatenation engine for saving to HDFS.
        """

        vcs = self._jvmRdd.toVariantContextRDD()

        if sortOnSave is None:
            finalVcs = vcs
        elif sortOnSave == "lexicographically":
            finalVcs = vcs.sortLexicographically()
        elif sortOnSave:
            finalVcs = vcs.sort()
        else:
            raise RuntimeError('sortOnSave = %s. Expected None, "lexicographically", or True.' % sortOnSave)

        finalVcs.saveAsVcf(filePath,
                           asSingleFile,
                           deferMerging,
                           disableFastConcat,
                           _toJava(stringency, self.sc._jvm))


class NucleotideContigFragmentRDD(GenomicRDD):


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

        GenomicRDD.__init__(self, jvmRdd, sc)


    def toDF(self):
        """
        :return: Returns a dataframe representing this RDD.
        """
        
        return DataFrame(self._jvmRdd.toDF(), SQLContext(self.sc))


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


class VariantRDD(GenomicRDD):


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

        GenomicRDD.__init__(self, jvmRdd, sc)


    def toDF(self):
        """
        :return: Returns a dataframe representing this RDD.
        """
        
        return DataFrame(self._jvmRdd.toDF(), SQLContext(self.sc))


    def saveAsVcf(self,
                  filePath,
                  asSingleFile=True,
                  deferMerging=False,
                  stringency=LENIENT,
                  sortOnSave=None,
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
        :param bool sortOnSave: Whether to sort when saving. If None, does not
        sort. If True, sorts by contig index. If "lexicographically", sorts by
        contig name.
        :param bool disableFastConcat: If asSingleFile is true, disables the use
        of the fast concatenation engine for saving to HDFS.
        """

        vcs = self._jvmRdd.toVariantContextRDD()

        if sortOnSave is None:
            finalVcs = vcs
        elif sortOnSave == "lexicographically":
            finalVcs = vcs.sortLexicographically()
        elif sortOnSave:
            finalVcs = vcs.sort()
        else:
            raise RuntimeError('sortOnSave = %s. Expected None, "lexicographically", or True.' % sortOnSave)

        finalVcs.saveAsVcf(filePath,
                           asSingleFile,
                           deferMerging,
                           disableFastConcat,
                           _toJava(stringency, self.sc._jvm))


    def save(self, filePath):
        """
        Saves this RDD of variants to disk.

        :param str filePath: Path to save file to. If ends in ".vcf", saves as
        VCF, else saves as Parquet.
        """

        if (filePath.endswith(".vcf")):
            self.saveAsVcf(filePath)
        else:
            self._jvmRdd.saveAsParquet(filePath)
