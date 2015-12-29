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

object VcfStringUtils {

  def clean(s: String): String = {
    val s0 = if (s.startsWith("[")) {
      s.drop(1)
    } else {
      s
    }
    if (s0.endsWith("]")) {
      s0.dropRight(1)
    } else {
      s0
    }
  }

  def vcfListToInts(l: String): List[Int] = {
    val valueList = l.split(",").toList

    def convertListToInts(l: List[String]): List[Int] =
      l.map((s) => clean(s).toInt)

    convertListToInts(valueList)
  }

  def vcfListToDoubles(l: String): List[Double] = {
    val valueList = l.split(",").toList

    def convertListToDoubles(l: List[String]): List[Double] =
      l.map((s: String) => clean(s).toDouble)

    convertListToDoubles(valueList)
  }

  def listToString(l: List[Any]): String = listToString(l.map(_.toString))

  private def stringListToString(l: List[String]): String =
    l.mkString(",")

  def stringToList(s: String): List[String] = s.split(",").toList
}
