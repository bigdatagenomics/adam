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

import org.bdgenomics.adam.util.ADAMFunSuite

class AlphabetSuite extends ADAMFunSuite {

  val testSymbols = Seq(
    Symbol('a', 'z'),
    Symbol('b', 'y'),
    Symbol('c', 'x')
  )

  val csAlpha = new Alphabet {
    override val caseSensitive = true
    override val symbols = testSymbols
  }

  test("test size of a case-sensitive alphabet") {
    assert(3 == csAlpha.size)
  }

  test("test apply of a case-sensitive alphabet") {
    assert(Symbol('b', 'y') == csAlpha('b'))
    val e = intercept[NoSuchElementException] {
      csAlpha('B')
    }
    assert(e.getMessage == "key not found: B")
  }

  test("test reverse complement of a case-sensitive alphabet") {
    assert("xyz" == csAlpha.reverseComplement("abc"))
    assert("CBA" == csAlpha.reverseComplement("ABC"))
    assert("" == csAlpha.reverseComplement(""))
  }

  test("test exact reverse complement of a case-sensitive alphabet") {
    assert("zzyyxx" == csAlpha.reverseComplement("ccbbaa"))
    assert("" == csAlpha.reverseComplement(""))

    val e = intercept[IllegalArgumentException] {
      csAlpha.reverseComplementExact("ABC")
    }
    assert(e.getMessage == "Character A not found in alphabet.")
  }

  val ciAlpha = new Alphabet {
    override val caseSensitive = false
    override val symbols = testSymbols
  }

  test("test size of a case-insensitive alphabet") {
    assert(3 == ciAlpha.size)
  }

  test("test apply of a case-insensitive alphabet") {
    assert(Symbol('b', 'y') == ciAlpha('b'))
    assert(Symbol('b', 'y') == ciAlpha('B'))
  }

  test("test reverse complement of a case-insensitive alphabet") {
    assert("xyz" == ciAlpha.reverseComplement("abc"))
    assert("xyz" == ciAlpha.reverseComplement("ABC"))
    assert("" == ciAlpha.reverseComplement(""))
  }

  test("test exact reverse complement of a case-insensitive alphabet") {
    assert("zzyyxx" == ciAlpha.reverseComplement("ccbbaa"))
    assert("zzyyxx" == ciAlpha.reverseComplement("cCbBaA"))
    assert("" == ciAlpha.reverseComplement(""))

    val e = intercept[IllegalArgumentException] {
      ciAlpha.reverseComplementExact("xxx")
    }
    assert(e.getMessage == "Character x not found in alphabet.")
  }

  test("DNA alphabet") {
    assert(4 == Alphabet.dna.size)
    assert("CGCGATAT" == Alphabet.dna.reverseComplement("atatcgcg"))
    assert("CGxATAT" == Alphabet.dna.reverseComplement("ATATxcg"))
  }

  test("map unknown bases to N") {
    assert(4 == Alphabet.dna.size)
    assert("CGNATAT" == Alphabet.dna.reverseComplement("ATATxcg", (c: Char) => Symbol('N', 'N')))
  }
}

