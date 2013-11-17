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

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord

object RecalUtil extends Serializable {

  object Constants {
    val MAX_REASONABLE_QSCORE = 60
    val MIN_REASONABLE_ERROR = math.pow(10.0, -MAX_REASONABLE_QSCORE / 10)
    val MAX_NUMBER_OF_OBSERVATIONS = Int.MaxValue
  }

  def qualToErrorProb(q: Byte): Double = math.pow(10.0, -q / 10)

  def errorProbToQual(d: Double): Byte = (-10 * math.log10(d)).toInt.toByte

  def recalibrate(read: ADAMRecord, qualByRG: QualByRG, covars: List[StandardCovariate], table: RecalTable): ADAMRecord = {
    // get the covariates
    val readCovariates = ReadCovariates(read, qualByRG, covars)
    val toQual = errorProbToQual _
    val toErr = qualToErrorProb _
    val newQuals = readCovariates.map(b => {
      toQual(table.getErrorRateShifts(b).foldLeft(toErr(b.qual))(_ + _))
    }).toArray
    val builder = ADAMRecord.newBuilder(read)
    builder.setQual(newQuals.foldLeft("")((a, b) => a + (b + 33).toChar.toString))
    builder.build()
  }

}