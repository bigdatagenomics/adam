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

import org.scalatest.FunSuite

class MdTagSuite extends FunSuite {

  test("null md tag") {
    MdTag(null)
  }

  test("zero length md tag") {
    MdTag("")
  }

  test("md tag with non-digit initial value") {
    intercept[IllegalArgumentException] {
      MdTag("ACTG0")
    }
  }

  test("md tag invalid base") {
    intercept[IllegalArgumentException] {
      MdTag("0ACTZ")
    }
  }

  test("md tag with no digit at end") {
    intercept[IllegalArgumentException] {
      MdTag("0ACTG")
    }
  }

  test("valid md tags") {
    val md1 = MdTag("0A0")
    assert(md1.mismatchedBase(0) == Some('A'))

    val md2 = MdTag("100")
    for (i <- 0 until 100) {
      assert(md2.isMatch(i))
    }
    assert(!md2.isMatch(-1))

    val md3 = MdTag("100C2")
    for (i <- 0 until 100) {
      assert(md3.isMatch(i))
    }
    assert(md3.mismatchedBase(100) == Some('C'))
    for (i <- 101 until 103) {
      assert(md3.isMatch(i))
    }

    val md4 = MdTag("100C0^C20")
    for (i <- 0 until 100) {
      assert(md4.isMatch(i))
    }
    assert(md4.mismatchedBase(100) == Some('C'))
    assert(md4.deletedBase(101) == Some('C'))
    for (i <- 102 until 122) {
      assert(md4.isMatch(i))
    }

    val deletedString = "ACGTACGTACGT"
    val md5 = MdTag("0^" + deletedString + "10")
    for (i <- 0 until deletedString.length) {
      assert(md5.deletedBase(i) == Some(deletedString charAt i))
    }

    val md6 = MdTag("22^A79")
    for (i <- 0 until 22) {
      assert(md6.isMatch(i))
    }
    assert(md6.deletedBase(22) == Some('A'))
    for (i <- 23 until 23 + 79) {
      assert(md6.isMatch(i))
    }

    // seen in 1000G, causes errors in 9c05baa2e0e9c59cbf56e241b8ae3a7b87402fa2
    val md7 = MdTag("39r36c23")
    for (i <- 0 until 39) {
      assert(md7.isMatch(i))
    }
    assert(md7.mismatchedBase(39) == Some('R'))
    for (i <- 40 until 40 + 36) {
      assert(md7.isMatch(i))
    }
    assert(md7.mismatchedBase(40 + 36) == Some('C'))
    for (i <- 40 + 37 until 40 + 37 + 23) {
      assert(md7.isMatch(i))
    }

    val mdy = MdTag("34Y18G46")
    assert(mdy.mismatchedBase(34) == Some('Y'))

  }

}
