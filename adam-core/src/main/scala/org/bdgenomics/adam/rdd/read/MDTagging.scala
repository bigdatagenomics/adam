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
package org.bdgenomics.adam.rdd.read

import org.bdgenomics.adam.rdd.ADAMContext._
import htsjdk.samtools.{ TextCigarCodec, ValidationStringency }
import org.apache.spark.Logging
// NOTE(ryan): this is necessary for Spark <= 1.2.1.
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ShuffleRegionJoin
import org.bdgenomics.adam.util.MdTag
import org.bdgenomics.formats.avro.{ AlignmentRecord, NucleotideContigFragment }

case class MDTagging(reads: RDD[AlignmentRecord],
                     referenceFragments: RDD[NucleotideContigFragment],
                     shuffle: Boolean = false,
                     partitionSize: Long = 1000000,
                     overwriteExistingTags: Boolean = false,
                     validationStringency: ValidationStringency = ValidationStringency.STRICT) extends Logging {
  @transient val sc = reads.sparkContext

  val mdTagsAdded = sc.accumulator(0L, "MDTags Added")
  val mdTagsExtant = sc.accumulator(0L, "MDTags Extant")
  val numUnmappedReads = sc.accumulator(0L, "Unmapped Reads")
  val incorrectMDTags = sc.accumulator(0L, "Incorrect Extant MDTags")

  val taggedReads =
    (if (shuffle) {
      addMDTagsShuffle
    } else {
      addMDTagsBroadcast
    }).cache

  def maybeMDTagRead(read: AlignmentRecord, refSeq: String): AlignmentRecord = {

    val cigar = TextCigarCodec.decode(read.getCigar)
    val mdTag = MdTag(read.getSequence, refSeq, cigar, read.getStart)
    if (read.getMismatchingPositions != null) {
      mdTagsExtant += 1
      if (mdTag.toString != read.getMismatchingPositions) {
        incorrectMDTags += 1
        if (overwriteExistingTags) {
          read.setMismatchingPositions(mdTag.toString)
        } else {
          val exception = IncorrectMDTagException(read, mdTag.toString)
          if (validationStringency == ValidationStringency.STRICT) {
            throw exception
          } else if (validationStringency == ValidationStringency.LENIENT) {
            log.warn(exception.getMessage)
          }
        }
      }
    } else {
      read.setMismatchingPositions(mdTag.toString)
      mdTagsAdded += 1
    }
    read
  }

  def addMDTagsBroadcast(): RDD[AlignmentRecord] = {
    val collectedRefMap =
      referenceFragments
        .groupBy(_.getContig.getContigName)
        .mapValues(_.toSeq.sortBy(_.getFragmentStartPosition))
        .collectAsMap
        .toMap

    log.info(s"Found contigs named: ${collectedRefMap.keys.mkString(", ")}")

    val refMapB = sc.broadcast(collectedRefMap)

    def getRefSeq(contigName: String, read: AlignmentRecord): String = {
      val readStart = read.getStart
      val readEnd = readStart + read.referenceLength

      val fragments =
        refMapB
          .value
          .getOrElse(
            contigName,
            throw new Exception(
              s"Contig $contigName not found in reference map with keys: ${refMapB.value.keys.mkString(", ")}"
            )
          )
          .dropWhile(f => f.getFragmentStartPosition + f.getFragmentSequence.length < readStart)
          .takeWhile(_.getFragmentStartPosition < readEnd)

      getReferenceBasesForRead(read, fragments)
    }
    reads.map(read => {
      (for {
        contig <- Option(read.getContig)
        contigName <- Option(contig.getContigName)
        if read.getReadMapped
      } yield {
        maybeMDTagRead(read, getRefSeq(contigName, read))
      }).getOrElse({
        numUnmappedReads += 1
        read
      })
    })
  }

  def addMDTagsShuffle(): RDD[AlignmentRecord] = {
    val fragsWithRegions =
      for {
        fragment <- referenceFragments
        region <- ReferenceRegion(fragment)
      } yield {
        region -> fragment
      }

    val unmappedReads = reads.filter(!_.getReadMapped)
    numUnmappedReads += unmappedReads.count

    val readsWithRegions =
      for {
        read <- reads
        region <- ReferenceRegion.opt(read)
      } yield region -> read

    val sd = reads.adamGetSequenceDictionary()

    val readsWithFragments =
      ShuffleRegionJoin(sd, partitionSize)
        .partitionAndJoin(readsWithRegions, fragsWithRegions)
        .groupByKey
        .mapValues(_.toSeq.sortBy(_.getFragmentStartPosition))

    (for {
      (read, fragments) <- readsWithFragments
    } yield {
      maybeMDTagRead(read, getReferenceBasesForRead(read, fragments))
    }) ++ unmappedReads
  }

  private def getReferenceBasesForRead(read: AlignmentRecord, fragments: Seq[NucleotideContigFragment]): String = {
    fragments.map(clipFragment(_, read)).mkString("")
  }

  private def clipFragment(fragment: NucleotideContigFragment, read: AlignmentRecord): String = {
    clipFragment(fragment, read.getStart, read.getStart + read.referenceLength)
  }
  private def clipFragment(fragment: NucleotideContigFragment, start: Long, end: Long): String = {
    val min =
      math.max(
        0L,
        start - fragment.getFragmentStartPosition
      ).toInt

    val max =
      math.min(
        fragment.getFragmentSequence.length,
        end - fragment.getFragmentStartPosition
      ).toInt

    fragment.getFragmentSequence.substring(min, max)
  }
}

object MDTagging {
  def apply(reads: RDD[AlignmentRecord],
            referenceFile: String,
            shuffle: Boolean,
            fragmentLength: Long,
            overwriteExistingTags: Boolean,
            validationStringency: ValidationStringency): RDD[AlignmentRecord] = {
    val sc = reads.sparkContext
    new MDTagging(
      reads,
      sc.loadSequence(referenceFile, fragmentLength = fragmentLength),
      shuffle = shuffle,
      partitionSize = fragmentLength,
      overwriteExistingTags,
      validationStringency
    ).taggedReads
  }
}

case class IncorrectMDTagException(read: AlignmentRecord, mdTag: String) extends Exception {
  override def getMessage: String =
    s"Read: ${read.getReadName}, pos: ${read.getContig.getContigName}:${read.getStart}, cigar: ${read.getCigar}, existing MD tag: ${read.getMismatchingPositions}, correct MD tag: $mdTag"
}
