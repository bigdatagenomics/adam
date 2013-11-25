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

import scala.annotation.tailrec
import scala.collection.immutable.StringOps

object VcfStringUtils {
  
  def vcfListToInts(l: String): List[Int] = {
    val valueList = l.split(",").toList
    
    // TODO: @tailrec 
    def convertListToInts(l: List[String]): List[Int] = {
      if (l.length == 0) {
        List[Int]()
      } else {
        l.head.toInt :: convertListToInts(l.tail)
      }
    }
    
    convertListToInts(valueList)
  }
  
  def vcfListToDoubles(l: String): List[Double] = {
    val valueList = l.split(",").toList
    
    // TODO: @tailrec 
    def convertListToDoubles(l: List[String]): List[Double] = {
      if (l.length == 0) {
        List[Double]()
      } else {
        l.head.toDouble :: convertListToDoubles(l.tail)
      }
    }
    
    convertListToDoubles(valueList)
  }

  def listToString (l: List[Any]): String = listToString (l.map(_.toString))
  /*l match {
    case (ls: List[String]) => stringListToString(ls)
    case _ => listToString (l.map(_.toString))
  }*/
  
  // TODO: @tailrec final 
  private def stringListToString (l: List[String]): String = {
    if (l.length == 0) {
      ""
    } else {
      l.head + "," + listToString(l.tail)
    }
  } 

  def stringToList (s: String): List[String] = s.split(",").toList
}
