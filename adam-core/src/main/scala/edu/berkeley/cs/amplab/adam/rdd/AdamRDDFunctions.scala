/*
 * Copyright (c) 2013-2014. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.cs.amplab.adam.rdd

import edu.berkeley.cs.amplab.adam.avro.{ADAMPileup, 
                                         ADAMRecord, 
                                         ADAMNucleotideContigFragment,
                                         Base}
import edu.berkeley.cs.amplab.adam.converters.GenotypesToVariantsConverter
import edu.berkeley.cs.amplab.adam.models.{SequenceRecord,
                                           SequenceDictionary,
                                           SingleReadBucket, 
                                           SnpTable, 
                                           ReferencePosition, 
                                           ReferenceRegion,
                                           ADAMRod}
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rdd.recalibration.BaseQualityRecalibration
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord._
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import edu.berkeley.cs.amplab.adam.util.{MapTools, ParquetLogger}
import java.io.File
import java.util.logging.Level
import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import parquet.avro.{AvroParquetOutputFormat, AvroWriteSupport}
import parquet.hadoop.ParquetOutputFormat
import parquet.hadoop.metadata.CompressionCodecName
import parquet.hadoop.util.ContextUtil
import scala.math.{min, max}

class AdamRDDFunctions[T <% SpecificRecord : Manifest](rdd: RDD[T]) extends Serializable {

  def adamSave(filePath: String, blockSize: Int = 128 * 1024 * 1024,
               pageSize: Int = 1 * 1024 * 1024, compressCodec: CompressionCodecName = CompressionCodecName.GZIP,
               disableDictionaryEncoding: Boolean = false): RDD[T] = {
    val job = new Job(rdd.context.hadoopConfiguration)
    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)
    ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport])
    ParquetOutputFormat.setCompression(job, compressCodec)
    ParquetOutputFormat.setEnableDictionary(job, !disableDictionaryEncoding)
    ParquetOutputFormat.setBlockSize(job, blockSize)
    ParquetOutputFormat.setPageSize(job, pageSize)
    AvroParquetOutputFormat.setSchema(job, manifest[T].erasure.asInstanceOf[Class[T]].newInstance().getSchema)
    // Add the Void Key
    val recordToSave = rdd.map(p => (null, p))
    // Save the values to the ADAM/Parquet file
    recordToSave.saveAsNewAPIHadoopFile(filePath,
      classOf[java.lang.Void], manifest[T].erasure.asInstanceOf[Class[T]], classOf[ParquetOutputFormat[T]],
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
abstract class AdamSequenceDictionaryRDDAggregator[T](rdd: RDD[T]) extends Serializable with Logging {
  /**
   * For a single RDD element, returns 0+ sequence record elements.
   *
   * @param elem Element from which to extract sequence records.
   * @return A seq of sequence records.
   */
  def getSequenceRecordsFromElement (elem: T): scala.collection.Set[SequenceRecord]

  /**
   * Aggregates together a sequence dictionary from the different individual reference sequences
   * used in this dataset.
   *
   * @return A sequence dictionary describing the reference contigs in this dataset.
   */
  def adamGetSequenceDictionary (): SequenceDictionary = {
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
      new SequenceDictionary(recs.toArray)
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
class AdamSpecificRecordSequenceDictionaryRDDAggregator[T <% SpecificRecord : Manifest](rdd: RDD[T])
  extends AdamSequenceDictionaryRDDAggregator[T](rdd) {

  def getSequenceRecordsFromElement (elem: T): Set[SequenceRecord] = {
    Set(SequenceRecord.fromSpecificRecord(elem))
  }
}

class AdamRecordRDDFunctions(rdd: RDD[ADAMRecord]) extends AdamSequenceDictionaryRDDAggregator[ADAMRecord](rdd) {

  def getSequenceRecordsFromElement (elem: ADAMRecord): scala.collection.Set[SequenceRecord] = {
    SequenceRecord.fromADAMRecord(elem)
  }

  def adamSortReadsByReferencePosition(): RDD[ADAMRecord] = {
    log.info("Sorting reads by reference position")

    // NOTE: In order to keep unmapped reads from swamping a single partition
    // we place them in a range of referenceIds at the end of the file.
    // The referenceId is an Int and typical only a few dozen values are even used.
    // These referenceId values are not stored; they are only used during sorting.
    val unmappedReferenceIds = new Iterator[Int] with Serializable {
      var currentOffsetFromEnd = 0

      def hasNext: Boolean = true

      def next(): Int = {
        currentOffsetFromEnd += 1
        if (currentOffsetFromEnd > 10000) {
          currentOffsetFromEnd = 0
        }
        Int.MaxValue - currentOffsetFromEnd
      }
    }

    rdd.map(p => {
      val referencePos = ReferencePosition(p) match {
        case None =>
          // Move unmapped reads to the end of the file
          ReferencePosition(
            unmappedReferenceIds.next(), Long.MaxValue)
        case Some(pos) => pos
      }
      (referencePos, p)
    }).sortByKey().map(p => p._2)
  }

  def adamMarkDuplicates(): RDD[ADAMRecord] = {
    MarkDuplicates(rdd)
  }

  def adamBQSR(knownSnps: SnpTable): RDD[ADAMRecord] = {
    BaseQualityRecalibration(rdd, knownSnps)
  }

  /**
   * Realigns indels using a concensus-based heuristic.
   *
   * @see RealignIndels
   *
   * @param isSorted If the input data is sorted, setting this parameter to true avoids a second sort.
   * @param maxIndelSize The size of the largest indel to use for realignment.
   * @param maxConsensusNumber The maximum number of consensus sequences to realign against per
   * target region.
   * @param lodThreshold Log-odds threhold to use when realigning; realignments are only finalized
   * if the log-odds threshold is exceeded.
   * @param maxTargetSize The maximum width of a single target region for realignment.
   *
   * @return Returns an RDD of mapped reads which have been realigned.
   */
  def adamRealignIndels(isSorted: Boolean = false,
                        maxIndelSize: Int = 500,
                        maxConsensusNumber: Int = 30,
                        lodThreshold: Double = 5.0,
                        maxTargetSize: Int = 3000): RDD[ADAMRecord] = {
    RealignIndels(rdd, isSorted, maxIndelSize, maxConsensusNumber, lodThreshold)
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
  def adamRecords2Rods (bucketSize: Int = 1000,
                        secondaryAlignments: Boolean = false): RDD[ADAMRod] = {
    
    /**
     * Maps a read to one or two buckets. A read maps to a single bucket if both
     * it's start and end are in a single bucket.
     *
     * @param r Read to map.
     * @return List containing one or two mapping key/value pairs.
     */
    def mapToBucket (r: ADAMRecord): List[(ReferencePosition, ADAMRecord)] = {
      val s = r.getStart / bucketSize
      val e = RichADAMRecord(r).end.get / bucketSize
      val id = r.getReferenceId

      if (s == e) {
        List((new ReferencePosition(id, s), r))
      } else {
        List((new ReferencePosition(id, s), r), (new ReferencePosition(id, e), r))
      }
    }

    println("Putting reads in buckets.")

    val bucketedReads = rdd.filter(_.getStart != null)
      .flatMap(mapToBucket)
      .groupByKey()

    println ("Have reads in buckets.")

    val pp = new Reads2PileupProcessor(secondaryAlignments)
    
    /**
     * Converts all reads in a bucket into rods.
     *
     * @param bucket Tuple of (bucket number, reads in bucket).
     * @return A sequence containing the rods in this bucket.
     */
    def bucketedReadsToRods (bucket: (ReferencePosition, Seq[ADAMRecord])): Seq[ADAMRod] = {
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
  def adamCharacterizeTags() : RDD[(String,Long)] = {
    rdd.flatMap(RichADAMRecord(_).tags.map( attr => (attr.tag, 1L) )).reduceByKey( _ + _ )
  }

  /**
   * Calculates the set of unique attribute <i>values</i> that occur for the given 
   * tag, and the number of time each value occurs.  
   * 
   * @param tag The name of the optional field whose values are to be counted.
   * @return A Map whose keys are the values of the tag, and whose values are the number of time each tag-value occurs.
   */
  def adamCharacterizeTagValues(tag : String) : Map[Any,Long] = {
    adamFilterRecordsWithTag(tag).flatMap(RichADAMRecord(_).tags.find(_.tag == tag)).map(
      attr => Map(attr.value -> 1L)
    ).reduce {
      (map1 : Map[Any,Long], map2 : Map[Any,Long]) =>
        MapTools.add(map1, map2)
    }
  }

  /**
   * Returns the subset of the ADAMRecords which have an attribute with the given name.
   * @param tagName The name of the attribute to filter on (should be length 2)
   * @return An RDD[ADAMRecord] containing the subset of records with a tag that matches the given name.
   */
  def adamFilterRecordsWithTag(tagName : String) : RDD[ADAMRecord] = {
    assert( tagName.length == 2,
      "withAttribute takes a tagName argument of length 2; tagName=\"%s\"".format(tagName))
    rdd.filter(RichADAMRecord(_).tags.exists(_.tag == tagName))
  }
}

class AdamPileupRDDFunctions(rdd: RDD[ADAMPileup]) extends Serializable with Logging {
  /**
   * Aggregates pileup bases together.
   *
   * @param coverage Coverage value is used to increase number of reducer operators.
   * @return RDD with aggregated bases.
   *
   * @see AdamRodRDDFunctions#adamAggregateRods
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

class AdamRodRDDFunctions(rdd: RDD[ADAMRod]) extends Serializable with Logging {
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
   * @see AdamPileupRDDFunctions#adamAggregatePileups
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

class AdamNucleotideContigFragmentRDDFunctions(rdd: RDD[ADAMNucleotideContigFragment]) extends AdamSequenceDictionaryRDDAggregator[ADAMNucleotideContigFragment](rdd) {
  
  /**
   * Rewrites the contig IDs of a FASTA reference set to match the contig IDs present in a
   * different sequence dictionary. Sequences are matched by name.
   *
   * @note Contigs with names that aren't present in the provided dictionary are filtered out of the RDD.
   *
   * @param sequenceDict A sequence dictionary containing the preferred IDs for the contigs.
   * @return New set of contigs with IDs rewritten.
   */
  def adamRewriteContigIds (sequenceDict: SequenceDictionary): RDD[ADAMNucleotideContigFragment] = {
    // broadcast sequence dictionary
    val bcastDict = rdd.context.broadcast(sequenceDict)

    /**
     * Remaps a single contig.
     *
     * @param contig Contig to remap.
     * @param dictionary A sequence dictionary containing the IDs to use for remapping.
     * @return An option containing the remapped contig if it's sequence name was found in the dictionary.
     */
    def remapContig (contig: ADAMNucleotideContigFragment, dictionary: SequenceDictionary): Option[ADAMNucleotideContigFragment] = {
      val name: CharSequence = contig.getContigName
      
      if (dictionary.containsRefName(name)) {
        val newId = dictionary(contig.getContigName).id
        val newContig = ADAMNucleotideContigFragment.newBuilder(contig)
          .setContigId(newId)
          .build()
        
        Some(newContig)
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
  def adamGetReferenceString (region: ReferenceRegion): String = {
    def getString(fragment: (ReferenceRegion, ADAMNucleotideContigFragment)): (ReferenceRegion, String) = {
      val trimStart = max(0, region.start - fragment._1.start).toInt
      val trimEnd = max(0, fragment._1.end - region.end).toInt

      val fragmentSequence: String = fragment._2.getFragmentSequence

      val str = fragmentSequence.drop(trimStart)
        .dropRight(trimEnd)
      
      val reg = new ReferenceRegion(fragment._1.refId,
                                    fragment._1.start + trimStart,
                                    fragment._1.end - trimEnd)

      (reg, str)
    }

    def reducePairs(kv1: (ReferenceRegion, String), 
                    kv2: (ReferenceRegion, String)): (ReferenceRegion, String) = {
      assert(kv1._1.isAdjacent(kv2._1), "Regions being joined must be adjacent. For: " + 
             kv1 + ", " + kv2)

      
      (kv1._1.merge(kv2._1), if(kv1._1.compare(kv2._1) <= 0) {
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

  def getSequenceRecordsFromElement (elem: ADAMNucleotideContigFragment): Set[SequenceRecord] = {
    // variant context contains a single locus
    Set(SequenceRecord.fromADAMContigFragment(elem))
  }

}
