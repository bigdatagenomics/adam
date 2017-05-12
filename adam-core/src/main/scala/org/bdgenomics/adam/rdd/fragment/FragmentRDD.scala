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
package org.bdgenomics.adam.rdd.fragment

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.converters.AlignmentRecordConverter
import org.bdgenomics.adam.instrumentation.Timers._
import org.bdgenomics.adam.models.{
  RecordGroupDictionary,
  ReferenceRegion,
  ReferenceRegionSerializer,
  SequenceDictionary
}
import org.bdgenomics.adam.rdd.{ AvroReadGroupGenomicRDD, JavaSaveArgs }
import org.bdgenomics.adam.rdd.read.{
  AlignmentRecordRDD,
  BinQualities,
  MarkDuplicates,
  QualityScoreBin
}
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.interval.array.{
  IntervalArray,
  IntervalArraySerializer
}
import org.bdgenomics.utils.misc.Logging
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

private[adam] case class FragmentArray(
    array: Array[(ReferenceRegion, Fragment)],
    maxIntervalWidth: Long) extends IntervalArray[ReferenceRegion, Fragment] {

  def duplicate(): IntervalArray[ReferenceRegion, Fragment] = {
    copy()
  }

  protected def replace(arr: Array[(ReferenceRegion, Fragment)],
                        maxWidth: Long): IntervalArray[ReferenceRegion, Fragment] = {
    FragmentArray(arr, maxWidth)
  }
}

private[adam] class FragmentArraySerializer extends IntervalArraySerializer[ReferenceRegion, Fragment, FragmentArray] {

  protected val kSerializer = new ReferenceRegionSerializer
  protected val tSerializer = new AvroSerializer[Fragment]

  protected def builder(arr: Array[(ReferenceRegion, Fragment)],
                        maxIntervalWidth: Long): FragmentArray = {
    FragmentArray(arr, maxIntervalWidth)
  }
}

/**
 * Helper singleton object for building FragmentRDDs.
 */
object FragmentRDD {

  /**
   * Hadoop configuration path to check for a boolean value indicating whether
   * the current or original read qualities should be written. True indicates
   * to write the original qualities. The default is false.
   */
  val WRITE_ORIGINAL_QUALITIES = "org.bdgenomics.adam.rdd.fragment.FragmentRDD.writeOriginalQualities"

  /**
   * Hadoop configuration path to check for a boolean value indicating whether
   * to write the "/1" "/2" suffixes to the read name that indicate whether a
   * read is first or second in a pair. Default is false (no suffixes).
   */
  val WRITE_SUFFIXES = "org.bdgenomics.adam.rdd.fragment.FragmentRDD.writeSuffixes"

  /**
   * Creates a FragmentRDD where no record groups or sequence info are attached.
   *
   * @param rdd RDD of fragments.
   * @return Returns a FragmentRDD with an empty record group dictionary and sequence dictionary.
   */
  private[rdd] def fromRdd(rdd: RDD[Fragment]): FragmentRDD = {
    FragmentRDD(rdd, SequenceDictionary.empty, RecordGroupDictionary.empty)
  }
}

/**
 * A genomic RDD that supports RDDs of Fragments.
 *
 * @param rdd The underlying RDD of Fragment data.
 * @param sequences The genomic sequences this data was aligned to, if any.
 * @param recordGroups The record groups these Fragments came from.
 */
case class FragmentRDD(rdd: RDD[Fragment],
                       sequences: SequenceDictionary,
                       recordGroups: RecordGroupDictionary) extends AvroReadGroupGenomicRDD[Fragment, FragmentRDD] {

  protected def buildTree(rdd: RDD[(ReferenceRegion, Fragment)])(
    implicit tTag: ClassTag[Fragment]): IntervalArray[ReferenceRegion, Fragment] = {
    IntervalArray(rdd, FragmentArray.apply(_, _))
  }

  /**
   * Replaces the underlying RDD with a new RDD.
   *
   * @param newRdd The RDD to replace our underlying RDD with.
   * @return Returns a new FragmentRDD where the underlying RDD has been
   *   swapped out.
   */
  protected def replaceRdd(newRdd: RDD[Fragment]): FragmentRDD = {
    copy(rdd = newRdd)
  }

  /**
   * Essentially, splits up the reads in a Fragment.
   *
   * @return Returns this RDD converted back to reads.
   */
  def toReads(): AlignmentRecordRDD = {
    val converter = new AlignmentRecordConverter

    // convert the fragments to reads
    val newRdd = rdd.flatMap(converter.convertFragment)

    // are we aligned?
    AlignmentRecordRDD(newRdd, sequences, recordGroups)
  }

  /**
   * Marks reads as possible fragment duplicates.
   *
   * @return A new RDD where reads have the duplicate read flag set. Duplicate
   *   reads are NOT filtered out.
   */
  def markDuplicates(): FragmentRDD = MarkDuplicatesInDriver.time {
    replaceRdd(MarkDuplicates(this))
  }

  /**
   * Saves Fragments to Parquet.
   *
   * @param filePath Path to save fragments at.
   */
  def save(filePath: java.lang.String) {
    saveAsParquet(new JavaSaveArgs(filePath))
  }

  /**
   * Rewrites the quality scores of fragments to place all quality scores in bins.
   *
   * Quality score binning maps all quality scores to a limited number of
   * discrete values, thus reducing the entropy of the quality score
   * distribution, and reducing the amount of space that fragments consume on disk.
   *
   * @param bins The bins to use.
   * @return Fragments whose quality scores are binned.
   */
  def binQualityScores(bins: Seq[QualityScoreBin]): FragmentRDD = {
    AlignmentRecordRDD.validateBins(bins)
    BinQualities(this, bins)
  }

  /**
   * Returns the regions that this fragment covers.
   *
   * Since a fragment may be chimeric or multi-mapped, we do not try to compute
   * the hull of the underlying element.
   *
   * @param elem The Fragment to get the region from.
   * @return Returns all regions covered by this fragment.
   */
  protected def getReferenceRegions(elem: Fragment): Seq[ReferenceRegion] = {
    elem.getAlignments
      .flatMap(r => ReferenceRegion.opt(r))
      .toSeq
  }
}
