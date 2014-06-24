/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.rdd

import fi.tkk.ics.hadoop.bam.SAMRecordWritable
import java.util.logging.Level
import net.sf.samtools.SAMFileHeader
import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.io.LongWritable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.avro.{
  ADAMPileup,
  ADAMRecord,
  ADAMNucleotideContigFragment
}
import org.bdgenomics.adam.converters.ADAMRecordConverter
import org.bdgenomics.adam.models.{
  ADAMRod,
  RecordGroupDictionary,
  ReferencePosition,
  ReferenceRegion,
  SAMFileHeaderWritable,
  SequenceRecord,
  SequenceDictionary,
  SingleReadBucket,
  SnpTable
}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.recalibration.BaseQualityRecalibration
import org.bdgenomics.adam.rdd.correction.{ ErrorCorrection, TrimReads }
import org.bdgenomics.adam.rich.RichADAMRecord
import org.bdgenomics.adam.util.{
  HadoopUtil,
  MapTools,
  ParquetLogger,
  ADAMBAMOutputFormat,
  ADAMSAMOutputFormat
}
import parquet.avro.{ AvroParquetOutputFormat, AvroWriteSupport }
import parquet.hadoop.ParquetOutputFormat
import parquet.hadoop.metadata.CompressionCodecName
import parquet.hadoop.util.ContextUtil
import scala.math.max

class ADAMRDDFunctions[T <% SpecificRecord: Manifest](rdd: RDD[T]) extends Serializable {

  def adamSave(filePath: String, blockSize: Int = 128 * 1024 * 1024,
               pageSize: Int = 1 * 1024 * 1024, compressCodec: CompressionCodecName = CompressionCodecName.GZIP,
               disableDictionaryEncoding: Boolean = false): RDD[T] = {
    val job = HadoopUtil.newJob(rdd.context)
    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)
    ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport])
    ParquetOutputFormat.setCompression(job, compressCodec)
    ParquetOutputFormat.setEnableDictionary(job, !disableDictionaryEncoding)
    ParquetOutputFormat.setBlockSize(job, blockSize)
    ParquetOutputFormat.setPageSize(job, pageSize)
    AvroParquetOutputFormat.setSchema(job, manifest[T].runtimeClass.asInstanceOf[Class[T]].newInstance().getSchema)
    // Add the Void Key
    val recordToSave = rdd.map(p => (null, p))
    // Save the values to the ADAM/Parquet file
    recordToSave.saveAsNewAPIHadoopFile(filePath,
      classOf[java.lang.Void], manifest[T].runtimeClass.asInstanceOf[Class[T]], classOf[ParquetOutputFormat[T]],
      ContextUtil.getConfiguration(job))
    // Return the origin rdd
    rdd
  }

}

/**
 * A class that provides functions to recover a sequence dictionary from a generic RDD of records.
 *
 * @tparam T Type contained in this RDD.
 * @param rdd RDD over which aggregation is supported.
 */
abstract class ADAMSequenceDictionaryRDDAggregator[T](rdd: RDD[T]) extends Serializable with Logging {
  /**
   * For a single RDD element, returns 0+ sequence record elements.
   *
   * @param elem Element from which to extract sequence records.
   * @return A seq of sequence records.
   */
  def getSequenceRecordsFromElement(elem: T): scala.collection.Set[SequenceRecord]

  /**
   * Aggregates together a sequence dictionary from the different individual reference sequences
   * used in this dataset.
   *
   * @return A sequence dictionary describing the reference contigs in this dataset.
   */
  def adamGetSequenceDictionary(): SequenceDictionary = {
    def mergeRecords(l: List[SequenceRecord], rec: T): List[SequenceRecord] = {
      val recs = getSequenceRecordsFromElement(rec)

      recs.foldLeft(l)((li: List[SequenceRecord], r: SequenceRecord) => {
        if (!li.contains(r)) {
          r :: li
        } else {
          li
        }
      })
    }

    def foldIterator(iter: Iterator[T]): SequenceDictionary = {
      val recs = iter.foldLeft(List[SequenceRecord]())(mergeRecords)
      SequenceDictionary(recs: _*)
    }

    rdd.mapPartitions(iter => Iterator(foldIterator(iter)), true)
      .reduce(_ ++ _)
  }

}

/**
 * A class that provides functions to recover a sequence dictionary from a generic RDD of records
 * that are defined in Avro. This class assumes that the reference identification fields are
 * defined inside of the given type.
 *
 * @note Avro classes that have specific constraints around sequence dictionary contents should
 * not use this class. Examples include ADAMRecords and ADAMNucleotideContigs
 *
 * @tparam T A type defined in Avro that contains the reference identification fields.
 * @param rdd RDD over which aggregation is supported.
 */
class ADAMSpecificRecordSequenceDictionaryRDDAggregator[T <% SpecificRecord: Manifest](rdd: RDD[T])
    extends ADAMSequenceDictionaryRDDAggregator[T](rdd) {

  def getSequenceRecordsFromElement(elem: T): Set[SequenceRecord] = {
    Set(SequenceRecord.fromSpecificRecord(elem))
  }
}

class ADAMRecordRDDFunctions(rdd: RDD[ADAMRecord]) extends ADAMSequenceDictionaryRDDAggregator[ADAMRecord](rdd) {

  /**
   * Saves an RDD of ADAM read data into the SAM/BAM format.
   *
   * @param filePath Path to save files to.
   * @param asSam Selects whether to save as SAM or BAM. The default value is true (save in SAM format).
   */
  def adamSAMSave(filePath: String, asSam: Boolean = true) {

    // convert the records
    val (convertRecords: RDD[SAMRecordWritable], header: SAMFileHeader) = rdd.adamConvertToSAM()

    // add keys to our records
    val withKey = convertRecords.keyBy(v => new LongWritable(v.get.getAlignmentStart))

    // attach header to output format
    asSam match {
      case true  => ADAMSAMOutputFormat.addHeader(convertRecords.first.get.getHeader())
      case false => ADAMBAMOutputFormat.addHeader(convertRecords.first.get.getHeader())
    }

    // write file to disk
    val conf = rdd.context.hadoopConfiguration
    asSam match {
      case true  => withKey.saveAsNewAPIHadoopFile(filePath, classOf[LongWritable], classOf[SAMRecordWritable], classOf[ADAMSAMOutputFormat[LongWritable]], conf)
      case false => withKey.saveAsNewAPIHadoopFile(filePath, classOf[LongWritable], classOf[SAMRecordWritable], classOf[ADAMBAMOutputFormat[LongWritable]], conf)
    }
  }

  def getSequenceRecordsFromElement(elem: ADAMRecord): scala.collection.Set[SequenceRecord] = {
    SequenceRecord.fromADAMRecord(elem)
  }

  /**
   * Collects a dictionary summarizing the read groups in an RDD of ADAMRecords.
   *
   * @return A dictionary describing the read groups in this RDD.
   */
  def adamGetReadGroupDictionary(): RecordGroupDictionary = {
    val rgNames = rdd.flatMap(r => Option(r.getRecordGroupName))
      .map(_.toString)
      .distinct()
      .collect()
      .toSeq

    new RecordGroupDictionary(rgNames)
  }

  /**
   * Converts an RDD of ADAM read records into SAM records.
   *
   * @return Returns a SAM/BAM formatted RDD of reads, as well as the file header.
   */
  def adamConvertToSAM(): (RDD[SAMRecordWritable], SAMFileHeader) = {
    // collect global summary data
    val sd = rdd.adamGetSequenceDictionary()
    val rgd = rdd.adamGetReadGroupDictionary()

    // create conversion object
    val adamRecordConverter = new ADAMRecordConverter

    // create header
    val header = adamRecordConverter.createSAMHeader(sd, rgd)

    // broadcast for efficiency
    val hdrBcast = rdd.context.broadcast(SAMFileHeaderWritable(header))

    // map across RDD to perform conversion
    val convertedRDD: RDD[SAMRecordWritable] = rdd.map(r => {
      // must wrap record for serializability
      val srw = new SAMRecordWritable()
      srw.set(adamRecordConverter.convert(r, hdrBcast.value))
      srw
    })

    (convertedRDD, header)
  }

  /**
   * Cuts reads into _k_-mers, and then counts the number of occurrences of each _k_-mer.
   *
   * @param kmerLength The value of _k_ to use for cutting _k_-mers.
   * @return Returns an RDD containing k-mer/count pairs.
   *
   * @see adamCountQmers
   */
  def adamCountKmers(kmerLength: Int): RDD[(String, Long)] = {
    rdd.flatMap(r => {
      // cut each read into k-mers, and attach a count of 1L
      r.getSequence
        .toString
        .sliding(kmerLength)
        .map(k => (k, 1L))
    }).reduceByKey((k1: Long, k2: Long) => k1 + k2)
  }

  /**
   * Cuts reads into _q_-mers, and then finds the _q_-mer weight. Q-mers are described in:
   *
   * Kelley, David R., Michael C. Schatz, and Steven L. Salzberg. "Quake: quality-aware detection
   * and correction of sequencing errors." Genome Biol 11.11 (2010): R116.
   *
   * _Q_-mers are _k_-mers weighted by the quality score of the bases in the _k_-mer.
   *
   * @param qmerLength The value of _q_ to use for cutting _q_-mers.
   * @return Returns an RDD containing q-mer/weight pairs.
   *
   * @see adamCountKmers
   */
  def adamCountQmers(qmerLength: Int): RDD[(String, Double)] = {
    ErrorCorrection.countQmers(rdd, qmerLength)
  }

  def adamSortReadsByReferencePosition(): RDD[ADAMRecord] = {
    log.info("Sorting reads by reference position")

    // NOTE: In order to keep unmapped reads from swamping a single partition
    // we place them in a range of referenceIds at the end of the file.
    // The referenceId is an Int and typical only a few dozen values are even used.
    // These referenceId values are not stored; they are only used during sorting.
    val unmappedReferenceNames = new Iterator[String] with Serializable {
      var currentOffsetFromEnd = 0

      def hasNext: Boolean = true

      def next(): String = {
        currentOffsetFromEnd += 1
        if (currentOffsetFromEnd > 10000) {
          currentOffsetFromEnd = 0
        }
        // NB : this is really ugly - any better way to manufacture
        // string values that are greater than anything else we care
        // about?
        "unmapped" + (Int.MaxValue - currentOffsetFromEnd).toString
      }
    }

    rdd.map(p => {
      val referencePos = ReferencePosition(p) match {
        case None =>
          // Move unmapped reads to the end of the file
          ReferencePosition(
            unmappedReferenceNames.next(), Long.MaxValue)
        case Some(pos) => pos
      }
      (referencePos, p)
    }).sortByKey().map(p => p._2)
  }

  def adamMarkDuplicates(): RDD[ADAMRecord] = {
    MarkDuplicates(rdd)
  }

  def adamBQSR(knownSnps: Broadcast[SnpTable]): RDD[ADAMRecord] = {
    BaseQualityRecalibration(rdd, knownSnps)
  }

  def adamRealignIndels(): RDD[ADAMRecord] = {
    RealignIndels(rdd)
  }

  // Returns a tuple of (failedQualityMetrics, passedQualityMetrics)
  def adamFlagStat(): (FlagStatMetrics, FlagStatMetrics) = {
    FlagStat(rdd)
  }

  /**
   * Groups all reads by record group and read name
   * @return SingleReadBuckets with primary, secondary and unmapped reads
   */
  def adamSingleReadBuckets(): RDD[SingleReadBucket] = {
    SingleReadBucket(rdd)
  }

  /**
   * Groups all reads by reference position and returns a non-aggregated pileup RDD.
   *
   * @param secondaryAlignments Creates pileups for non-primary aligned reads. Default is false.
   * @return ADAMPileup without aggregation
   */
  def adamRecords2Pileup(secondaryAlignments: Boolean = false): RDD[ADAMPileup] = {
    val helper = new Reads2PileupProcessor(secondaryAlignments)
    helper.process(rdd)
  }

  /**
   * Groups all reads by reference position, with all reference position bases grouped
   * into a rod.
   *
   * @param bucketSize Size in basepairs of buckets. Larger buckets take more time per
   * bucket to convert, but have lower skew. Default is 1000.
   * @param secondaryAlignments Creates rods for non-primary aligned reads. Default is false.
   * @return RDD of ADAMRods.
   */
  def adamRecords2Rods(bucketSize: Int = 1000,
                       secondaryAlignments: Boolean = false): RDD[ADAMRod] = {

    /**
     * Maps a read to one or two buckets. A read maps to a single bucket if both
     * it's start and end are in a single bucket.
     *
     * @param r Read to map.
     * @return List containing one or two mapping key/value pairs.
     */
    def mapToBucket(r: ADAMRecord): List[(ReferencePosition, ADAMRecord)] = {
      val s = r.getStart / bucketSize
      val e = RichADAMRecord(r).end.get / bucketSize
      val name = r.getContig.getContigName

      if (s == e) {
        List((new ReferencePosition(name, s), r))
      } else {
        List((new ReferencePosition(name, s), r), (new ReferencePosition(name, e), r))
      }
    }

    val bucketedReads = rdd.filter(_.getStart != null)
      .flatMap(mapToBucket)
      .groupByKey()

    val pp = new Reads2PileupProcessor(secondaryAlignments)

    /**
     * Converts all reads in a bucket into rods.
     *
     * @param bucket Tuple of (bucket number, reads in bucket).
     * @return A sequence containing the rods in this bucket.
     */
    def bucketedReadsToRods(bucket: (ReferencePosition, Iterable[ADAMRecord])): Iterable[ADAMRod] = {
      val (bucketStart, bucketReads) = bucket

      bucketReads.flatMap(pp.readToPileups)
        .groupBy(ReferencePosition(_))
        .toList
        .map(g => ADAMRod(g._1, g._2.toList)).toSeq
    }

    bucketedReads.flatMap(bucketedReadsToRods)
  }

  /**
   * Converts a set of records into an RDD containing the pairs of all unique tagStrings
   * within the records, along with the count (number of records) which have that particular
   * attribute.
   *
   * @return An RDD of attribute name / count pairs.
   */
  def adamCharacterizeTags(): RDD[(String, Long)] = {
    rdd.flatMap(RichADAMRecord(_).tags.map(attr => (attr.tag, 1L))).reduceByKey(_ + _)
  }

  /**
   * Calculates the set of unique attribute <i>values</i> that occur for the given
   * tag, and the number of time each value occurs.
   *
   * @param tag The name of the optional field whose values are to be counted.
   * @return A Map whose keys are the values of the tag, and whose values are the number of time each tag-value occurs.
   */
  def adamCharacterizeTagValues(tag: String): Map[Any, Long] = {
    adamFilterRecordsWithTag(tag).flatMap(RichADAMRecord(_).tags.find(_.tag == tag)).map(
      attr => Map(attr.value -> 1L)).reduce {
        (map1: Map[Any, Long], map2: Map[Any, Long]) =>
          MapTools.add(map1, map2)
      }
  }

  /**
   * Returns the subset of the ADAMRecords which have an attribute with the given name.
   * @param tagName The name of the attribute to filter on (should be length 2)
   * @return An RDD[ADAMRecord] containing the subset of records with a tag that matches the given name.
   */
  def adamFilterRecordsWithTag(tagName: String): RDD[ADAMRecord] = {
    assert(tagName.length == 2,
      "withAttribute takes a tagName argument of length 2; tagName=\"%s\"".format(tagName))
    rdd.filter(RichADAMRecord(_).tags.exists(_.tag == tagName))
  }

  /**
   * Trims bases from the start and end of all reads in an RDD.
   *
   * @param trimStart Number of bases to trim from the start of the read.
   * @param trimEnd Number of bases to trim from the end of the read.
   * @param readGroup Optional parameter specifying which read group to trim. If omitted,
   * all reads are trimmed.
   * @return Returns an RDD of trimmed reads.
   *
   * @note Trimming parameters must be >= 0.
   */
  def adamTrimReads(trimStart: Int, trimEnd: Int, readGroup: Int = -1): RDD[ADAMRecord] = {
    TrimReads(rdd, trimStart, trimEnd, readGroup)
  }

  /**
   * Trims low quality read prefix/suffixes. The average read prefix/suffix quality is
   * calculated from the Phred scaled qualities for read bases. We trim suffixes/prefixes
   * that are below a user provided threshold.
   *
   * @param phredThreshold Phred score for trimming. Defaut value is 20.
   * @return Returns an RDD of trimmed reads.
   */
  def adamTrimLowQualityReadGroups(phredThreshold: Int = 20): RDD[ADAMRecord] = {
    TrimReads(rdd, phredThreshold)
  }
}

class ADAMPileupRDDFunctions(rdd: RDD[ADAMPileup]) extends Serializable with Logging {
  /**
   * Aggregates pileup bases together.
   *
   * @param coverage Coverage value is used to increase number of reducer operators.
   * @return RDD with aggregated bases.
   *
   * @see ADAMRodRDDFunctions#adamAggregateRods
   */
  def adamAggregatePileups(coverage: Int = 30): RDD[ADAMPileup] = {
    val helper = new PileupAggregator
    helper.aggregate(rdd, coverage)
  }

  /**
   * Converts ungrouped pileup bases into reference grouped bases.
   *
   * @param coverage Coverage value is used to increase number of reducer operators.
   * @return RDD with rods grouped by reference position.
   */
  def adamPileupsToRods(coverage: Int = 30): RDD[ADAMRod] = {
    val groups = rdd.groupBy((p: ADAMPileup) => ReferencePosition(p), coverage)

    groups.map(kv => ADAMRod(kv._1, kv._2.toList))
  }

}

class ADAMRodRDDFunctions(rdd: RDD[ADAMRod]) extends Serializable with Logging {
  /**
   * Given an RDD of rods, splits the rods up by the specific sample they correspond to.
   * Returns a flat RDD.
   *
   * @return Rods split up by samples and _not_ grouped together.
   */
  def adamSplitRodsBySamples(): RDD[ADAMRod] = {
    rdd.flatMap(_.splitBySamples)
  }

  /**
   * Given an RDD of rods, splits the rods up by the specific sample they correspond to.
   * Returns an RDD where the samples are grouped by the reference position.
   *
   * @return Rods split up by samples and grouped together by position.
   */
  def adamDivideRodsBySamples(): RDD[(ReferencePosition, List[ADAMRod])] = {
    rdd.keyBy(_.position).map(r => (r._1, r._2.splitBySamples))
  }

  /**
   * Inside of a rod, aggregates pileup bases together.
   *
   * @return RDD with aggregated rods.
   *
   * @see ADAMPileupRDDFunctions#adamAggregatePileups
   */
  def adamAggregateRods(): RDD[ADAMRod] = {
    val helper = new PileupAggregator
    rdd.map(r => (r.position, r.pileups))
      .map(kv => (kv._1, helper.flatten(kv._2)))
      .map(kv => new ADAMRod(kv._1, kv._2))
  }

  /**
   * Returns the average coverage for all pileups.
   *
   * @note Coverage value does not include locus positions where no reads are mapped, as no rods exist for these positions.
   * @note If running on an RDD with multiple samples where the rods have been split by sample, will return the average
   *       coverage per sample, _averaged_ over all samples. If the RDD contains multiple samples and the rods have _not_ been split,
   *       this will return the average coverage per sample, _summed_ over all samples.
   *
   * @return Average coverage across mapped loci.
   */
  def adamRodCoverage(): Double = {
    val totalBases: Long = rdd.map(_.pileups.length.toLong).reduce(_ + _)

    // coverage is the total count of bases, over the total number of loci
    totalBases.toDouble / rdd.count.toDouble
  }
}

class ADAMNucleotideContigFragmentRDDFunctions(rdd: RDD[ADAMNucleotideContigFragment]) extends ADAMSequenceDictionaryRDDAggregator[ADAMNucleotideContigFragment](rdd) {

  /**
   * Rewrites the contig IDs of a FASTA reference set to match the contig IDs present in a
   * different sequence dictionary. Sequences are matched by name.
   *
   * @note Contigs with names that aren't present in the provided dictionary are filtered out of the RDD.
   *
   * @param sequenceDict A sequence dictionary containing the preferred IDs for the contigs.
   * @return New set of contigs with IDs rewritten.
   */
  def adamRewriteContigIds(sequenceDict: SequenceDictionary): RDD[ADAMNucleotideContigFragment] = {
    // broadcast sequence dictionary
    val bcastDict = rdd.context.broadcast(sequenceDict)

    /**
     * Remaps a single contig.
     *
     * @param fragment Contig to remap.
     * @param dictionary A sequence dictionary containing the IDs to use for remapping.
     * @return An option containing the remapped contig if it's sequence name was found in the dictionary.
     */
    def remapContig(fragment: ADAMNucleotideContigFragment, dictionary: SequenceDictionary): Option[ADAMNucleotideContigFragment] = {
      val name: CharSequence = fragment.getContig.getContigName

      if (dictionary.containsRefName(name)) {
        // NB : this is a no-op in the non-ref-id world. Should we delete it?
        val newFragment = ADAMNucleotideContigFragment.newBuilder(fragment)
          .setContig(fragment.getContig)
          .build()
        Some(newFragment)
      } else {
        None
      }
    }

    // remap all contigs
    rdd.flatMap(c => remapContig(c, bcastDict.value))
  }

  /**
   * From a set of contigs, returns the base sequence that corresponds to a region of the reference.
   *
   * @throws UnsupportedOperationException Throws exception if query region is not found.
   *
   * @param region Reference region over which to get sequence.
   * @return String of bases corresponding to reference sequence.
   */
  def adamGetReferenceString(region: ReferenceRegion): String = {
    def getString(fragment: (ReferenceRegion, ADAMNucleotideContigFragment)): (ReferenceRegion, String) = {
      val trimStart = max(0, region.start - fragment._1.start).toInt
      val trimEnd = max(0, fragment._1.end - region.end).toInt

      val fragmentSequence: String = fragment._2.getFragmentSequence

      val str = fragmentSequence.drop(trimStart)
        .dropRight(trimEnd)
      val reg = new ReferenceRegion(fragment._1.referenceName,
        fragment._1.start + trimStart,
        fragment._1.end - trimEnd)
      (reg, str)
    }

    def reducePairs(kv1: (ReferenceRegion, String),
                    kv2: (ReferenceRegion, String)): (ReferenceRegion, String) = {
      assert(kv1._1.isAdjacent(kv2._1), "Regions being joined must be adjacent. For: " +
        kv1 + ", " + kv2)

      (kv1._1.merge(kv2._1), if (kv1._1.compare(kv2._1) <= 0) {
        kv1._2 + kv2._2
      } else {
        kv2._2 + kv1._2
      })
    }

    try {
      val pair: (ReferenceRegion, String) = rdd.keyBy(ReferenceRegion(_))
        .filter(kv => kv._1.isDefined)
        .map(kv => (kv._1.get, kv._2))
        .filter(kv => kv._1.overlaps(region))
        .sortByKey()
        .map(kv => getString(kv))
        .reduce(reducePairs)

      assert(pair._1.compare(region) == 0,
        "Merging fragments returned a different region than requested.")

      pair._2
    } catch {
      case (uoe: UnsupportedOperationException) => {
        throw new UnsupportedOperationException("Could not find " + region + "in reference RDD.")
      }
    }
  }

  def getSequenceRecordsFromElement(elem: ADAMNucleotideContigFragment): Set[SequenceRecord] = {
    // variant context contains a single locus
    Set(SequenceRecord.fromADAMContigFragment(elem))
  }

}
