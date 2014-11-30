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
package org.bdgenomics.adam.rdd.read.correction

import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.PhredUtils
import org.bdgenomics.adam.instrumentation.Timers._
import scala.annotation.tailrec
import scala.math.{ log => mathLog, exp }

private[rdd] object TrimReads extends Logging {

  /**
   * Finds the optimal trimming length for the start and end of a read, and trims the read.
   *
   * @param rdd RDD of reads to trim.
   * @param phredThreshold Phred score threshold for trimming read start/end.
   * @return Returns an RDD of trimmed reads.
   */
  def apply(rdd: RDD[AlignmentRecord], phredThreshold: Int): RDD[AlignmentRecord] = {
    val tr = new TrimReads

    // get read length
    val readLength = rdd.adamFirst().getSequence.length

    // map read quality scores into doubles
    log.info("Collecting read quality scores.")
    val doubleRdd: RDD[((String, Int), Double)] = rdd.adamFlatMap(tr.readToDoubles)
      .cache()

    // reduce this by key, and also get counts
    log.info("Summarizing quality scores.")
    val columnValues = doubleRdd.reduceByKeyLocally(_ + _)
    val columnCounts = doubleRdd.countByKey()

    // we are done with doubleRdd - so, unpersist to free memory
    doubleRdd.unpersist()

    // get read group ids
    val rgIds = columnValues.keys
      .map(p => p._1)
      .toSet

    // loop per read group
    var loopRdd = rdd
    rgIds.foreach(rg => {
      // get column values and counts per this read group
      val qualities: Seq[(Int, Double)] = columnValues.filterKeys(k => k._1 == rg)
        .map(kv => (kv._1._2, kv._2))
        .toSeq
        .sortBy(kv => kv._1)
      val counts: Seq[(Int, Long)] = columnCounts.filterKeys(k => k._1 == rg)
        .map(kv => (kv._1._2, kv._2))
        .toSeq
        .sortBy(kv => kv._1)

      // zip columns and counts, and take the mean quality score
      val meanQuals: Seq[Int] = qualities.zip(counts)
        .map(qc => {
          val ((p1, qual), (p2, count)) = qc

          // check that positions are the same
          assert(p1 == p2, "During zip, value ranks disagree.")

          // convert back to phred
          PhredUtils.successProbabilityToPhred(exp(qual / count))
        })

      // find the number of bases off of the start/end that are below the requested 
      // phred score threshold
      val trimStart = meanQuals.takeWhile(_ < phredThreshold).length
      val trimEnd = meanQuals.reverse.takeWhile(_ < phredThreshold).length

      // call trimming function
      loopRdd = apply(loopRdd, trimStart, trimEnd, rg)
    })

    // return rdd
    loopRdd
  }

  /**
   * Trims bases from the start and end of reads.
   *
   * @param rdd An RDD of reads to trim.
   * @param trimStart The number of bases to trim from the start of the read.
   * @param trimEnd The number of bases to trim from the end of the read.
   * @param rg Optional read group ID parameter. Sets which read group to apply this trim to.
   * Set to -1 if you would like to trim all reads.
   * @return Returns an RDD of trimmed reads.
   */
  def apply(rdd: RDD[AlignmentRecord],
            trimStart: Int,
            trimEnd: Int,
            rg: String = null): RDD[AlignmentRecord] = {
    assert(trimStart >= 0 && trimEnd >= 0,
      "Trim parameters must be positive.")
    assert(rdd.adamFirst().getSequence.length > trimStart + trimEnd,
      "Cannot trim more than the length of the read.")

    log.info("Trimming reads.")

    val tr = new TrimReads
    rdd.adamMap(read => {
      // only trim reads that are in this read group
      if (read.getRecordGroupName == rg || rg == null) {
        tr.trimRead(read, trimStart, trimEnd)
      } else {
        read
      }
    })
  }
}

private[correction] class TrimReads extends Serializable {

  /**
   * Converts a read into an array of doubles which are the base quality scores
   * as success probabilities.
   *
   * @param read Read to convert.
   * @return Returns an array of log scaled success probabilities for each base, zipped with
   * the position of the double in the read, and the read group ID. 'null' is returned if the
   * read group is not set.
   */
  def readToDoubles(read: AlignmentRecord): Array[((String, Int), Double)] = {
    val rgIdx: String = read.getRecordGroupName

    read.getQual
      .toArray
      .map(q => mathLog(PhredUtils.phredToSuccessProbability(q.toInt - 33)))
      .zipWithIndex
      .map(p => ((rgIdx, p._2), p._1))
  }

  /**
   * Trims bases off of an MD tag.
   *
   * @param mdTag Tag to trim.
   * @param trimStart Bases to trim off of the start.
   * @param trimEnd Bases to trim off of the end.
   * @return Returns the trimmed MD tag.
   */
  def trimMdTag(mdTag: String, trimStart: Int, trimEnd: Int): String = {
    @tailrec def trimFront(m: String, trim: Int): String = {
      if (trim <= 0) {
        if (m(0).isLetter) {
          "0" + m
        } else {
          m
        }
      } else {
        val (md: String, t: Int) = if (m(0) == '^') {
          (m.drop(1).dropWhile(_.isLetter), trim)
        } else if (m(0).isLetter) {
          (m.drop(1), trim - 1)
        } else if (m(0).isDigit) {
          val num = m.takeWhile(_.isDigit).toInt
          if (num == 0) {
            (m.dropWhile(_.isDigit), trim)
          } else {
            ((num - 1) + m.dropWhile(_.isDigit), trim - 1)
          }
        } else {
          throw new IllegalArgumentException("Illegal character in MD tag: " + m)
        }

        // call recursively
        trimFront(md, t)
      }
    }

    // this should be called on a reversed MD tag, and returns a reversed MD tag
    // this is a bit of a smell, but is done because some scala string operations
    // are only defined from the start of a string
    def trimRear(m: String, trim: Int): String = {
      if (trim <= 0) {
        if (m(0).isLetter) {
          "0" + m
        } else {
          m
        }
      } else {
        val (md: String, t: Int) = if (m(0).isDigit) {
          // get the number of matches at this site
          val num = m.takeWhile(_.isDigit).reverse.toInt

          // get the remainder of the MD tag
          val remainder = m.dropWhile(_.isDigit)

          // if we have 0 at this location, nothing to trim, just remove and move on
          if (num == 0) {
            (remainder, trim)
          } else {
            ((num - 1).toString.reverse + remainder, trim - 1)
          }
        } else {
          // if we have bases here, there is a mismatch or a deletion, so collect bases and parse
          val bases = m.takeWhile(!_.isDigit)

          // get updated trim length
          val t = if (bases.last == '^') { // we have a deletion
            trim // excise deletion
          } else if (bases.length == 1) { // we have a mismatch
            trim - 1 // trim base
          } else { // something went wrong
            throw new IllegalArgumentException("Illegal base string in MD tag: " + bases)
          }

          (m.dropWhile(!_.isDigit), t)
        }

        // call recursively
        trimRear(md, t)
      }
    }

    // call helper functions, we must reverse input and output of trimRear
    trimRear(trimFront(mdTag, trimStart).reverse, trimEnd).reverse
  }

  /**
   * Trims a CIGAR string. This method handles several corner cases:
   *
   * - When trimming through a deletion/reference skip, the deletion/skip is excised.
   * - This method updates the read start when trimming M/X/= bases.
   *
   * The trimmed segments are replaced with hard clipping.
   *
   * @param cigar CIGAR string to trim.
   * @param trimStart Number of bases to trim from the start of the CIGAR. Must be >= 0.
   * @param trimEnd Number of bases to trim from the end of the CIGAR. Must be >= 0.
   * @param startPos Start position of the alignment.
   * @return Returns a tuple containing the (updated cigar, updated alignment start position).
   */
  def trimCigar(cigar: String, trimStart: Int, trimEnd: Int, startPos: Long): (String, Long) = TrimCigar.time {
    @tailrec def trimFront(c: String, trim: Int, start: Long): (String, Long) = {
      if (trim <= 0) {
        (c, start)
      } else {
        val count = c.takeWhile(_.isDigit).toInt
        val operator = c.dropWhile(_.isDigit).head
        val withoutOp = c.dropWhile(_.isDigit).drop(1)

        val (cNew, tNew, sNew) = if (operator == 'D' || operator == 'N') {
          // must trim all the way through a reference deletion or skip
          (withoutOp, trim, start + count)
        } else {
          // get updated cigar
          val cNew = if (count == 1) {
            withoutOp
          } else {
            (count - 1) + operator.toString + withoutOp
          }

          // get updated start
          val sNew = if (operator == 'M' ||
            operator == '=' ||
            operator == 'X') {
            // if we are trimming into an alignment match, we must update the start position
            start + 1
          } else {
            start
          }

          (cNew, trim - 1, sNew)
        }

        trimFront(cNew, tNew, sNew)
      }
    }

    def trimBack(c: String, trim: Int): String = {
      @tailrec def trimBackHelper(ch: String, t: Int): String = {
        if (t <= 0) {
          ch.reverse
        } else {
          val count = ch.drop(1).takeWhile(_.isDigit).reverse.toInt
          val operator = ch.head
          val withoutOp = ch.drop(1).dropWhile(_.isDigit)

          val (cNew, tNew) = if (operator == 'D' || operator == 'N') {
            // must trim all the way through a reference deletion or skip
            (withoutOp, t)
          } else if (count == 1) {
            (withoutOp, t - 1)
          } else {
            (operator.toString + (count - 1).toString.reverse + withoutOp, t - 1)
          }

          trimBackHelper(cNew, tNew)
        }
      }

      if (trim <= 0) {
        c
      } else {
        trimBackHelper(c.reverse, trim)
      }
    }

    // add hard clipping and return   
    val trimPrefix = if (trimStart > 0) {
      trimStart + "H"
    } else {
      ""
    }
    val trimSuffix = if (trimEnd > 0) {
      trimEnd + "H"
    } else {
      ""
    }

    val (cigarFrontTrimmed, newStart) = trimFront(cigar, trimStart, startPos)

    (trimPrefix + trimBack(cigarFrontTrimmed, trimEnd) + trimSuffix, newStart)
  }

  /**
   * Trims a read. Updates the read sequence, qualities, start position, and cigar.
   * All other fields are copied over from the old read.
   *
   * @param read Read to trim.
   * @param trimStart Number of bases to trim from the start of the read.
   * @param trimEnd Number of bases to trim from the end of the read.
   * @return Trimmed read.
   */
  def trimRead(read: AlignmentRecord, trimStart: Int, trimEnd: Int): AlignmentRecord = TrimRead.time {
    // trim sequence and quality value
    val seq: String = read.getSequence.toString.drop(trimStart).dropRight(trimEnd)
    val qual: String = read.getQual.toString.drop(trimStart).dropRight(trimEnd)

    // make copy builder
    val builder = AlignmentRecord.newBuilder(read)
      .setSequence(seq)
      .setBasesTrimmedFromStart(Option(read.getBasesTrimmedFromStart).fold(0)(v => v) + trimStart)
      .setBasesTrimmedFromEnd(Option(read.getBasesTrimmedFromEnd).fold(0)(v => v) + trimEnd)
      .setQual(qual)

    // clean up cigar and read start position
    Option(read.getCigar).filter(_ != "*")
      .map(trimCigar(_, trimStart, trimEnd, read.getStart))
      .foreach(p => {
        val (c, s) = p
        builder.setCigar(c)
          .setStart(s)

        // also, clean up md tag
        Option(read.getMismatchingPositions).map(trimMdTag(_, trimStart, trimEnd))
          .foreach(builder.setMismatchingPositions)
      })

    // finish building and return
    builder.build()
  }
}
