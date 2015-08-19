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
package org.bdgenomics.adam.util

import scala.math.{ exp, pow, log, log10 }

object PhredUtils {

  lazy val phredToErrorProbabilityCache: Array[Double] = {
    (0 until 256).map { p => pow(10.0, -p / 10.0) }.toArray
  }

  lazy val phredToSuccessProbabilityCache: Array[Double] = {
    phredToErrorProbabilityCache.map { p => 1.0 - p }
  }

  def phredToLogProbability(phred: Int): Float = log(phredToSuccessProbability(phred)).toFloat

  def phredToSuccessProbability(phred: Int): Double = if (phred < 255) {
    phredToSuccessProbabilityCache(phred)
  } else {
    phredToSuccessProbabilityCache(255)
  }

  def phredToErrorProbability(phred: Int): Double = if (phred < 255) {
    phredToErrorProbabilityCache(phred)
  } else {
    phredToErrorProbabilityCache(255)
  }

  private def probabilityToPhred(p: Double): Int = math.round(-10.0 * log10(p)).toInt

  def successProbabilityToPhred(p: Double): Int = probabilityToPhred(1.0 - p)

  def errorProbabilityToPhred(p: Double): Int = probabilityToPhred(p)

  def logProbabilityToPhred(p: Float): Int = if (p == 0.0) {
    256
  } else {
    successProbabilityToPhred(exp(p))
  }

  def main(args: Array[String]) = {
    phredToErrorProbabilityCache.zipWithIndex.foreach(p => println("%3d = %f".format(p._2, p._1)))
  }

}
