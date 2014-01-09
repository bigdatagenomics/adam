/*
 * Copyright (c) 2013. The Broad Institute
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
package edu.berkeley.cs.amplab.adam.rdd.recalibration

import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord._
import org.apache.spark.rdd.RDD

// this class is required, not just standard. Baked in to recalibration.
class QualByRG(rdd: RDD[RichADAMRecord]) extends Serializable {
  // need to get the unique read groups todo --- this is surprisingly slow
  //val readGroups = rdd.map(_.getRecordGroupId.toString).distinct().collect().sorted.zipWithIndex.toMap
  var readGroups = Map[String, Int]()

  def apply(read: RichADAMRecord, start: Int, end: Int): Array[Int] = {
    if (!readGroups.contains(read.getRecordGroupId.asInstanceOf[String])) {
      readGroups += (read.getRecordGroupId.asInstanceOf[String] -> readGroups.size)
    }
    val rg_offset = RecalUtil.Constants.MAX_REASONABLE_QSCORE * readGroups(read.getRecordGroupId.toString)
    read.qualityScores.slice(start, end).map(_.toInt + rg_offset)
  }

  def numPartitions = RecalUtil.Constants.MAX_REASONABLE_QSCORE * (1 + readGroups.size)
}

trait StandardCovariate extends Serializable {
  def apply(read: RichADAMRecord, start: Int, end: Int): Array[Int] // get the covariate for all the bases of the read
}

case class DiscreteCycle(args: RDD[RichADAMRecord]) extends StandardCovariate {
  // this is a special-case of the GATK's Cycle covariate for discrete technologies.
  // Not to be used for 454 or ion torrent (which are flow cycles)
  def apply(read: RichADAMRecord, startOffset: Int, endOffset: Int): Array[Int] = {
    var cycles: Array[Int] = if (read.getReadNegativeStrand) Range(read.getSequence.toString.size, 0, -1).toArray
    else Range(1, 1 + read.getSequence.toString.size, 1).toArray
    cycles = if (read.getReadPaired && read.getSecondOfPair) cycles.map(-_) else cycles
    cycles.slice(startOffset, endOffset)
  }
}

case class BaseContext(records: RDD[RichADAMRecord], size: Int) extends StandardCovariate {
  def this(_s: Int) = this(null, _s)

  def this(_r: RDD[RichADAMRecord]) = this(_r, 2)

  val BASES = Array('A'.toByte, 'C'.toByte, 'G'.toByte, 'T'.toByte)
  val COMPL = Array('T'.toByte, 'G'.toByte, 'C'.toByte, 'A'.toByte)
  val N_BASE = 'N'.toByte
  val COMPL_MP = (BASES zip COMPL toMap) + (N_BASE -> N_BASE)

  def apply(read: RichADAMRecord, startOffset: Int, endOffset: Int): Array[Int] = {
    // the context of a covariate is the previous @size bases, though "previous" depends on
    // how the read was aligned (negative strand is reverse-complemented).
    if (read.getReadNegativeStrand) reverseContext(read, startOffset, endOffset) else forwardContext(read, startOffset, endOffset)
  }

  // note: the last base is dropped from the construction of contexts because it is not
  // present in any context - just as the first base cannot have a context assigned to it.
  def forwardContext(rec: RichADAMRecord, st: Int, end: Int): Array[Int] = {
    getContext(rec.getSequence.asInstanceOf[String].toCharArray.map(_.toByte).slice(st, end))
  }

  def simpleReverseComplement(bases: Array[Byte]): Array[Byte] = {
    bases.map(b => COMPL_MP(b)).reverseIterator.toArray
  }

  def reverseContext(rec: RichADAMRecord, st: Int, end: Int): Array[Int] = {
    // first reverse-complement the sequence
    val baseSeq = simpleReverseComplement(rec.getSequence.asInstanceOf[String].toCharArray.map(_.toByte))
    getContext(baseSeq.slice(baseSeq.size - end, baseSeq.size - st))
  }

  // the first @size bases don't get a context
  def getContext(bases: Array[Byte]): Array[Int] = {
    (1 to size - 1).map(_ => 0).toArray ++ bases.sliding(size).map(encode)
  }

  def encode(bases: Array[Byte]): Int = {
    // takes a base sequence and maps it to an integer between 1 and 4^n+1
    // (0 is reserved for contextless bases (e.g. beginning of read)
    if (bases.indexOf(N_BASE) > -1) 0 else 1 + bases.map(BASES.indexOf(_)).reduceLeft(_ * 4 + _)
  }

  def decode(bases: Int): String = {
    if (bases == 0) {
      "N" * size
    } else {
      var baseStr = ""
      var bInt = bases - 1
      while (baseStr.size < size) {
        val base = BASES(bInt % 4).toChar.toString
        baseStr = base + baseStr
        bInt /= 4
      }
      baseStr
    }
  }
}
