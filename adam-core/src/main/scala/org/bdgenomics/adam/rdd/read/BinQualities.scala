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

import org.bdgenomics.adam.rdd.fragment.FragmentDataset
import org.bdgenomics.formats.avro.{ AlignmentRecord, Fragment }
import scala.collection.JavaConversions._

/**
 * Companion object for creating quality score bins.
 */
object QualityScoreBin {

  /**
   * Creates multiple bins from a string of bin descriptions.
   *
   * The string defining the bins should have each bin separated by a semicolon
   * (";"). Each bin should be a comma separated list of three digits. The first
   * digit is the low end of the bin, the second digit is the high end of the
   * bin, and the third digit is the quality score to assign to bases in the
   * bin. E.g., to define a bin that runs from 0 to 20 (low end inclusive, high
   * end exclusive), and that assigns a quality score of 10, we'd write:
   *
   * 0,20,10
   *
   * To define this bin and another bin that runs from 20 to 40 and assigns a
   * score of 30, we would write:
   *
   * 0,20,10;20,40,30
   *
   * @param binString A string defining the bins to create.
   * @return Returns a Seq of bins.
   */
  def apply(binString: String): Seq[QualityScoreBin] = {
    binString.split(";")
      .map(parseBin)
  }

  /**
   * Creates a single bin.
   *
   * For more details, see the bin documentation in the apply method. A single
   * bin is defined as a triple of integers: low,high,score.
   *
   * @param bin The single bin to parse.
   * @return Returns an object representing this bin.
   */
  private[read] def parseBin(bin: String): QualityScoreBin = {
    val splitString = bin.split(",")
    require(splitString.size == 3,
      "Bin string (%s) did not contain exactly 3 elements.".format(bin))
    try {
      QualityScoreBin(splitString(0).toInt,
        splitString(1).toInt,
        splitString(2).toInt)
    } catch {
      case nfe: NumberFormatException => {
        throw new IllegalArgumentException("All elements in bin description (%s) must be integers.".format(bin))
      }
    }
  }
}

/**
 * A bin to put quality scores in.
 *
 * @param low The lowest quality score in the bin.
 * @param high The highest quality score in the bin.
 * @param score The score to assign to all these bases.
 */
case class QualityScoreBin(low: Int,
                           high: Int,
                           score: Int) {

  require(low >= 0, "Low phred score (%d) must be greater than 0.".format(low))
  require(high > low, "High score (%d) must be greater than the low score (%d).".format(
    high, low))
  require(high < 255, "High score (%d) must be below 255.".format(high))

  private val lowPhred = (low + 33).toChar
  private val highPhred = (high + 33).toChar
  private val scoreToEmit = (score + 33).toChar
  require(score >= low && score < high,
    "Score to emit (%d) must be between high and low scores (%d–%d).".format(
      score, low, high))

  private[rdd] def optGetBase(phred: Char): Option[Char] = {
    if ((phred >= lowPhred) && (phred < highPhred)) {
      Some(scoreToEmit)
    } else {
      None
    }
  }
}

private[rdd] object BinQualities extends Serializable {

  /**
   * Rewrites the quality scores attached to a read into bins.
   *
   * @param reads The reads to bin the quality scores of.
   * @param bins The bins to place the quality scores in.
   * @return Returns a new RDD of reads were the quality scores of the read
   *   bases have been binned.
   */
  def apply(reads: AlignmentRecordDataset,
            bins: Seq[QualityScoreBin]): AlignmentRecordDataset = {

    reads.transform(rdd => {
      rdd.map(binRead(_, bins))
    })
  }

  /**
   * Rewrites the quality scores attached to a fragment into bins.
   *
   * @param fragments The fragments to bin the quality scores of.
   * @param bins The bins to place the quality scores in.
   * @return Returns a new RDD of fragments were the quality scores of the fragment
   *   bases have been binned.
   */
  def apply(fragments: FragmentDataset,
            bins: Seq[QualityScoreBin]): FragmentDataset = {

    fragments.transform(rdd => {
      rdd.map(binFragment(_, bins))
    })
  }

  /**
   * Rewrites the quality scores of a single fragment.
   *
   * @param fragment The fragment whose quality scores should be rewritten.
   * @param bins The bins to place the quality scores in.
   * @return Returns a new fragment whose quality scores have been updated.
   */
  private[read] def binFragment(fragment: Fragment,
                                bins: Seq[QualityScoreBin]): Fragment = {
    val reads: Seq[AlignmentRecord] = fragment.getAlignments
    val binnedReads = reads.map(binRead(_, bins))
    Fragment.newBuilder(fragment)
      .setAlignments(binnedReads)
      .build
  }

  /**
   * Rewrites the quality scores of a single read.
   *
   * @param read The read whose quality scores should be rewritten.
   * @param bins The bins to place the quality scores in.
   * @return Returns a new read whose quality scores have been updated.
   */
  private[read] def binRead(read: AlignmentRecord,
                            bins: Seq[QualityScoreBin]): AlignmentRecord = {
    if (read.getQuality != null) {
      AlignmentRecord.newBuilder(read)
        .setQuality(binQualities(read.getQuality, bins))
        .build
    } else {
      read
    }
  }

  /**
   * Rewrites a string of quality scores.
   *
   * @param read The read whose quality scores should be rewritten.
   * @param bins The bins to place the quality scores in.
   * @return Returns a new read whose quality scores have been updated.
   */
  private[read] def binQualities(quals: String,
                                 bins: Seq[QualityScoreBin]): String = {
    quals.map(q => {
      val scores = bins.flatMap(_.optGetBase(q))

      if (scores.size == 1) {
        scores.head
      } else if (scores.size > 1) {
        throw new IllegalStateException("Quality score (%s) fell into multiple bins (from bins %s)".format(q,
          bins.mkString(",")))
      } else {
        throw new IllegalStateException("Quality score (%s) fell into no bins (from bins %s).".format(q,
          bins.mkString(",")))
      }
    }).mkString
  }
}
