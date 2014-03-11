/*
 * Copyright (c) 2013. Regents of the University of California
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
package edu.berkeley.cs.amplab.adam.util

import scala.math.{pow, log10}

object PhredUtils {

  lazy val phredToErrorProbabilityCache: Array[Double] = {
    (0 until 256).map{p => pow(10.0, -p / 10.0)}.toArray
  }

  lazy val phredToSuccessProbabilityCache: Array[Double] = {
    phredToErrorProbabilityCache.map{p => 1.0 - p}
  }

  def phredToSuccessProbability(phred: Int): Double = phredToSuccessProbabilityCache(phred)

  def phredToErrorProbability(phred: Int): Double = phredToErrorProbabilityCache(phred)

  private def probabilityToPhred(p: Double): Int = math.round(-10.0 * log10(p)).toInt

  def successProbabilityToPhred(p: Double): Int = probabilityToPhred(1.0 - p)

  def errorProbabilityToPhred(p: Double): Int = probabilityToPhred(p)

  def main(args: Array[String]) = {
    phredToErrorProbabilityCache.zipWithIndex.foreach(p => println("%3d = %f".format(p._2, p._1)))
  }

}
