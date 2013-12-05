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
import org.apache.spark.broadcast.{Broadcast => SparkBroadcast}

object ReadCovariates {
  def apply(rec: ADAMRecord, qualRG: QualByRG, covars: List[StandardCovariate], dbsnp: SparkBroadcast[Map[String, Set[Int]]] = null): ReadCovariates = {
    new ReadCovariates(rec, qualRG, covars, dbsnp)
  }
}

class ReadCovariates(val read: ADAMRecord, qualByRG: QualByRG, covars: List[StandardCovariate],
                     val dbsnp: SparkBroadcast[Map[String, Set[Int]]] = null) extends Iterator[BaseCovariates] with Serializable {

  val startOffset = read.qualityScores.takeWhile(_ <= 2).size
  val endOffset = read.qualityScores.size - read.qualityScores.reverseIterator.takeWhile(_ <= 2).size
  val qualCovar: Array[Int] = qualByRG(read, startOffset, endOffset)
  val requestedCovars: List[Array[Int]] = covars.map(covar => covar(read, startOffset, endOffset))

  var iter_position = startOffset

  override def hasNext: Boolean = iter_position < endOffset

  override def next(): BaseCovariates = {
    val offset = (iter_position - startOffset).toInt
    val position = read.offsetToPosition(offset)
    val isMasked = dbsnp == null || position.isEmpty ||
      dbsnp.value(read.getReferenceName.toString).contains(position.get.toInt) ||
      read.isMismatchAtOffset(offset).isEmpty
    val isMisMatch = read.isMismatchAtOffset(offset).getOrElse(false) // getOrElse because reads without an MD tag can appear during *application* of recal table
    iter_position += 1
    new BaseCovariates(qualCovar(offset), requestedCovars.map(v => v(offset)).toArray,
      read.qualityScores(offset), isMisMatch, isMasked)
  }

}

class BaseCovariates(val qualByRG: Int, val covar: Array[Int], val qual: Byte, val isMismatch: Boolean, val isMasked: Boolean) {}

// holder class
