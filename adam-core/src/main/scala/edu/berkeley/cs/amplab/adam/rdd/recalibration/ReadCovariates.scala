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
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord._
import edu.berkeley.cs.amplab.adam.models.SnpTable

object ReadCovariates {
  def apply(rec: RichADAMRecord, qualRG: QualByRG, covars: List[StandardCovariate],
            dbsnp: SnpTable = SnpTable()): ReadCovariates = {
    new ReadCovariates(rec, qualRG, covars, dbsnp)
  }
}

class ReadCovariates(val read: RichADAMRecord, qualByRG: QualByRG, covars: List[StandardCovariate],
                     val dbSNP: SnpTable, val minQuality:Int = 2) extends Iterator[BaseCovariates] with Serializable {

  def isLowQualityBase(qual : Byte) : Boolean = {
    qual.toInt <=  minQuality
  }

  val qualityStartOffset = read.qualityScores.takeWhile(isLowQualityBase).size
  val qualityEndOffset = read.qualityScores.size - read.qualityScores.reverseIterator.takeWhile(isLowQualityBase).size


  val qualCovar: Array[Int] = qualByRG(read, qualityStartOffset, qualityEndOffset)
  val requestedCovars: List[Array[Int]] = covars.map(covar => covar(read, qualityStartOffset, qualityEndOffset))

  var readOffset = qualityStartOffset


  override def hasNext: Boolean = readOffset < qualityEndOffset

  override def next(): BaseCovariates = {
    val baseCovarOffset = readOffset - qualityStartOffset
    val mismatch = read.isMismatchAtReadOffset(readOffset)
    // FIXME: why does empty mismatch mean it should be masked?
    val isMasked = dbSNP.isMaskedAtReadOffset(read, readOffset) || mismatch.isEmpty
    // getOrElse because reads without an MD tag can appear during *application* of recal table
    val isMismatch = mismatch.getOrElse(false)
    val qualityScore = read.qualityScores(readOffset)
    readOffset += 1
    new BaseCovariates(qualCovar(baseCovarOffset), requestedCovars.map(v => v(baseCovarOffset)).toArray,
      qualityScore, isMismatch, isMasked)
  }

}

class BaseCovariates(val qualByRG: Int, val covar: Array[Int], val qual: Byte, val isMismatch: Boolean, val isMasked: Boolean) {}

// holder class
