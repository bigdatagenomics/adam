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
package org.bdgenomics.adam.models

import scala.util.Try

/**
 * Created by bryan on 4/17/15.
 *
 * An alphabet of symbols and related operations
 *
 */
trait Alphabet {

  /** the symbols in this alphabet */
  val symbols: Seq[Symbol]

  /**
   * flag if symbols are case-sensitive.
   * if true, this alphabet will treat symbols representing upper and lower case symbols as distinct
   * if false, this alphabet will treat upper or lower case chars as the same for its symbols
   */
  val caseSensitive: Boolean

  /** map of the symbol char to the symbol */
  lazy val symbolMap: Map[Char, Symbol] =
    if (caseSensitive)
      symbols.map(symbol => symbol.label -> symbol).toMap
    else
      symbols.flatMap(symbol => Seq(symbol.label.toLower -> symbol, symbol.label.toUpper -> symbol)).toMap

  /**
   *
   * @param s Each char in this string represents a symbol on the alphabet.
   *          If the char is not in the alphabet then a NoSuchElementException is thrown
   * @return the reversed complement of the given string.
   * @throws IllegalArgumentException if the string contains a symbol which is not in the alphabet
   */
  def reverseComplementExact(s: String): String = {
    reverseComplement(s,
      (symbol: Char) => throw new IllegalArgumentException("Character %s not found in alphabet.".format(symbol))
    )
  }

  /**
   *
   * @param s Each char in this string represents a symbol on the alphabet.
   * @param notFound If the char is not in the alphabet then this function is called.
   *                 default behavior is to return a new Symbol representing the unknown character,
   *                 so that the unknown char is treated as the complement
   * @return the reversed complement of the given string.
   */
  def reverseComplement(s: String, notFound: (Char => Symbol) = ((c: Char) => Symbol(c, c))) = {
    s.map(x => Try(apply(x)).getOrElse(notFound(x)).complement).reverse
  }

  /** number of symbols in the alphabet */
  def size = symbols.size

  /**
   * @param c char to lookup as a symbol in this alphabet
   * @return the given symbol
   */
  def apply(c: Char): Symbol = symbolMap(c)

}

/**
 * A symbol in an alphabet
 * @param label a character which represents the symbol
 * @param complement acharacter which represents the complement of the symbol
 */
case class Symbol(label: Char, complement: Char)

/**
 * The standard DNA alphabet with A,T,C, and G
 */
class DNAAlphabet extends Alphabet {

  override val caseSensitive = false

  override val symbols = Seq(
    Symbol('A', 'T'),
    Symbol('T', 'A'),
    Symbol('G', 'C'),
    Symbol('C', 'G')
  )
}

object Alphabet {
  val dna = new DNAAlphabet
}
