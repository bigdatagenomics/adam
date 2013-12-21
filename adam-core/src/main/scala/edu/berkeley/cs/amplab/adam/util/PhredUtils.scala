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

  // conversion methods going to/from phred
  def phredToDouble(phred: Int): Double = 1.0 - pow(10.0, -(phred.toDouble) / 10.0)

  def doubleToPhred(p: Double): Int = (-10.0 * log10(1.0 - p)).toInt

}
