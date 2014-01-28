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

import scala.collection.immutable.Map
import scala.collection.mutable
import edu.berkeley.cs.amplab.adam.util.PhredUtils


class RecalTable(_counts: mutable.HashMap[Int, Array[ErrorCounts]], _expectedMM: Double) extends Serializable {
  def this() = this(mutable.HashMap[Int, Array[ErrorCounts]](), 0.0)

  // the int here is the readgroup + quality score covariate
  val counts = _counts
  var expectedMismatch = _expectedMM

  /**
   * A mutable lookup with a side effect :
   * Look up the qualByRG integer, if present, return the error counts
   * If not present:
   * 1) instantiate new error counts for all the covariates
   * 2) Place new counts into the map
   * 3) Return the new counts
   * @param qualByRG - the qualByRG stratification int to look up
   * @param nCovariates - the number of covariates
   * @note - Sideffect - if qualByRG not present in @counts, a new default entry is placed into the map
   * @return - the array of ErrorCounts objects associated with the qual/rg stratification
   */
  def lookup(qualByRG: Int, nCovariates: Int): Array[ErrorCounts] = {
    try {
      counts(qualByRG)
    } catch {
      case e: java.util.NoSuchElementException => {
        val newErrorCounts = (1 to nCovariates).map(t => new ErrorCounts).toArray
        counts += (qualByRG -> newErrorCounts)
        newErrorCounts
      }
      case e: Exception => throw new java.lang.IllegalStateException(e)
    }
  }

  def +=(info: BaseCovariates): Unit = {
    val errorCounts: Array[ErrorCounts] = lookup(info.qualByRG, info.covar.size)
    // the error counts and covariate values have the same order, so zip them
    errorCounts zip info.covar foreach (errorCovar => {
      errorCovar._1(errorCovar._2) += info
    })
    expectedMismatch += PhredUtils.phredToErrorProbability(info.qual)
  }

  /*val BASES = "ACGT".toCharArray.map(_.toByte)
  def _decode_debug(bases : Int) : String = {
      if ( bases == 0 ) {
        "N"*2
      } else {
        var baseStr = ""
        var bInt = bases - 1
        while ( baseStr.size < 2 ) {
          val base = BASES(bInt % 4).toChar.toString
          baseStr = base + baseStr
          bInt /= 4
        }
        baseStr
      }
    }*/

  def +(info: ReadCovariates): RecalTable = {
    /*println("RC: "+info.read.record.getReadName+" with start "+info.startOffset.toString+" and end "+info.endOffset +
      " QCov size "+info.qualCovar.size.toString+" and cov sizes "+info.requestedCovars.map(_.size.toString).toString())
    println("isRev: " + ( if ( info.read.record.getReadNegativeStrand) "true" else "false" ) )
    println("BContext: "+info.requestedCovars(1).map(_decode_debug).toList)*/
    info.foreach(b => this += b)
    //println("Done! "+info.read.record.getReadName)
    this
  }

  def ++(other: RecalTable): RecalTable = {
    val v: Array[ErrorCounts] = if (counts.size > 0) {
      counts.values.head
    } else if (other.counts.size > 0) {
      other.counts.values.head
    } else {
      Array[ErrorCounts]()

    }
    val newCounts = mutable.HashMap[Int, Array[ErrorCounts]]()
    newCounts ++= (counts.keySet ++ other.counts.keySet).map {
      case k => k -> {
        val mine = counts.getOrElse(k, v.map(t => new ErrorCounts()))
        val yours = other.counts.getOrElse(k, v.map(t => new ErrorCounts()))
        mine.zip(yours).map(ec => ec._1 ++ ec._2)
      }
    }
    new RecalTable(newCounts, this.expectedMismatch + other.expectedMismatch)
  }

  var qualByRGCounts = Map[Int, ErrorCount]()
  var readGroupCounts = Map[Int, ErrorCount]()
  var globalCounts = new ErrorCount
  var averageReportedError = 0.0
  var globalError = 0.0

  // rather than perform the above updates on the fly, they can be calculated at the end by folding
  def finalizeTable() = {
    qualByRGCounts = counts map {
      kv => kv._1 -> kv._2(0).errorsByVariate.values.reduce(_ ++ _)
    } toMap
    val readgroups = qualByRGCounts.keySet.toList.sorted.groupBy(t => (t - 1) / RecalUtil.Constants.MAX_REASONABLE_QSCORE)
    readGroupCounts = readgroups.mapValues(idxs => idxs.map(t => qualByRGCounts(t)).reduce(_ ++ _)).map(identity) // workaround, MapLike not serializable
    globalCounts = readGroupCounts.values.reduce(_ ++ _)
    averageReportedError = expectedMismatch / globalCounts.basesObserved
    globalError = globalCounts.getErrorProb.getOrElse(averageReportedError)
  }

  def getReadGroupDelta(baseCovar: BaseCovariates): Double = {
    val empiricalCounts = readGroupCounts((baseCovar.qualByRG - 1) / RecalUtil.Constants.MAX_REASONABLE_QSCORE)
    empiricalCounts.getErrorProb.getOrElse(averageReportedError) - averageReportedError
  }

  def getQualScoreDelta(baseCovar: BaseCovariates): Double = {
    val readGroupDelta = getReadGroupDelta(baseCovar)
    val empiricalCounts = qualByRGCounts(baseCovar.qualByRG)
    val reportedErr = PhredUtils.phredToErrorProbability(baseCovar.qual)
    val errAdjusted = reportedErr + readGroupDelta
    empiricalCounts.getErrorProb.getOrElse(errAdjusted) - errAdjusted
  }

  def getCovariateDelta(baseCovar: BaseCovariates): Seq[Double] = {
    val errAdjusted = PhredUtils.phredToErrorProbability(baseCovar.qual) + getReadGroupDelta(baseCovar) + getQualScoreDelta(baseCovar)
    val errs = this.lookup(baseCovar.qualByRG, baseCovar.covar.size).zip(baseCovar.covar).map(t => t._1(t._2).getErrorProb)
    errs.map(_.getOrElse(errAdjusted) - errAdjusted)
  }

  def getErrorRateShifts(baseCovar: BaseCovariates): Seq[Double] = {
    var shifts = List[Double]()
    shifts :+= getReadGroupDelta(baseCovar)
    shifts :+= getQualScoreDelta(baseCovar)
    shifts ++ getCovariateDelta(baseCovar)
  }
}

class ErrorCounts extends Serializable {
  var errorsByVariate = mutable.Map[Int, ErrorCount]()

  /**
   *
   * @param covar - the covariate integer requested
   * @note - sideffect - if @covar not present in @errorsByVariate, a new (@Covar -> ErrorCount) object is emplaced
   * @return the ErrorCount object for the covariate value @covar
   */
  def apply(covar: Int): ErrorCount = {
    try {
      errorsByVariate(covar)
    } catch {
      case e: java.util.NoSuchElementException => {
        errorsByVariate(covar) = new ErrorCount
        this(covar)
      }
      case e: Exception => throw new java.lang.IllegalStateException(e)
    }
  }

  def ++(other: ErrorCounts): ErrorCounts = {
    val newErrors = mutable.HashMap[Int, ErrorCount]()
    newErrors ++= (this.errorsByVariate.keySet ++ other.errorsByVariate.keySet).map(
      key => {
        val mine = this.errorsByVariate.getOrElse(key, new ErrorCount)
        val yours = other.errorsByVariate.getOrElse(key, new ErrorCount)
        (key, mine ++ yours)
      }
    )
    val newCounts = new ErrorCounts
    newCounts.errorsByVariate = newErrors
    newCounts
  }
}

class ErrorCount extends Serializable {
  var basesObserved = 0L
  var mismatches = 0L

  def +=(errors: BaseCovariates) {
    if (!errors.isMasked) {
      basesObserved += 1
      if (errors.isMismatch)
        mismatches += 1
    }
  }

  def ++(other: ErrorCount): ErrorCount = {
    val err_new = new ErrorCount
    err_new.basesObserved = this.basesObserved + other.basesObserved
    err_new.mismatches = this.mismatches + other.mismatches
    err_new
  }

  def getErrorProb: Option[Double] = {
    if (basesObserved == 0) return None

    Some(math.max(RecalUtil.Constants.MIN_REASONABLE_ERROR, mismatches.toDouble / basesObserved))
  }
}
