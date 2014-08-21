/*
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
package org.bdgenomics.adam.models

trait Alphabet {
  def letters: Set[Char]
  def complement(char: Char): Char

  def reverseComplement(dna: String): String = {
    val builder = new StringBuilder()
    dna.foreach(c => builder.append(complement(c)))
    builder.reverse.toString()
  }
}

object DNAAlphabet extends Alphabet {

  val letters = Set('A', 'T', 'G', 'C', 'N')

  def complement(c: Char): Char = {
    c match {
      case 'A' => 'T'
      case 'a' => 't'
      case 'G' => 'C'
      case 'g' => 'c'
      case 'C' => 'G'
      case 'c' => 'g'
      case 'T' => 'A'
      case 't' => 'a'
      case _   => c
    }
  }
}
