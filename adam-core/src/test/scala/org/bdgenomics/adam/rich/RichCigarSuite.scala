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
package org.bdgenomics.adam.rich

import htsjdk.samtools.{ Cigar, TextCigarCodec }
import org.bdgenomics.adam.rich.RichAlignment._
import org.bdgenomics.formats.avro.Alignment
import org.scalatest.FunSuite

class RichCigarSuite extends FunSuite {

  test("moving 2 bp from a deletion to a match operator") {
    val read = Alignment.newBuilder()
      .setReadMapped(true)
      .setStart(0)
      .setCigar("10M10D10M")
      .build()

    val newCigar = RichCigar(new Cigar(read.samtoolsCigar.getCigarElements)).moveLeft(1)
    val newCigar2 = newCigar.moveLeft(1)

    assert(newCigar2.cigar.getReadLength == read.samtoolsCigar.getReadLength)
    assert(newCigar2.cigar.toString === "8M10D12M")
  }

  test("moving 2 bp from a insertion to a match operator") {
    val read = Alignment
      .newBuilder()
      .setReadMapped(true)
      .setStart(0)
      .setCigar("10M10I10M")
      .build()

    val newCigar = RichCigar(new Cigar(read.samtoolsCigar.getCigarElements)).moveLeft(1)
    val newCigar2 = newCigar.moveLeft(1)

    assert(newCigar2.cigar.getReadLength == read.samtoolsCigar.getReadLength)
    assert(newCigar2.cigar.toString === "8M10I12M")
  }

  test("moving 1 base in a two element cigar") {
    val read = Alignment
      .newBuilder()
      .setReadMapped(true)
      .setStart(0)
      .setCigar("10M1D")
      .build()

    val newCigar = RichCigar(new Cigar(read.samtoolsCigar.getCigarElements)).moveLeft(1)

    assert(newCigar.cigar.getReadLength == read.samtoolsCigar.getReadLength)
    assert(newCigar.cigar.toString === "9M1D1M")
  }

  test("move to start of read") {
    val read = Alignment
      .newBuilder()
      .setReadMapped(true)
      .setStart(0)
      .setCigar("1M1D1M")
      .build()

    val newCigar = RichCigar(new Cigar(read.samtoolsCigar.getCigarElements)).moveLeft(1)

    assert(newCigar.cigar.getReadLength == read.samtoolsCigar.getReadLength)
    assert(newCigar.cigar.toString === "1D2M")
  }

  val rightClippedCigar = new RichCigar(TextCigarCodec.decode("10H2S10M"))
  val leftClippedCigar = new RichCigar(TextCigarCodec.decode("12M4S5H"))
  val bothClippedCigar = new RichCigar(TextCigarCodec.decode("1S12M3S2H"))

  test("process right clipped cigar") {
    assert(rightClippedCigar.softClippedBasesAtStart === 2)
    assert(rightClippedCigar.softClippedBasesAtEnd === 0)
  }

  test("process left clipped cigar") {
    assert(leftClippedCigar.softClippedBasesAtStart === 0)
    assert(leftClippedCigar.softClippedBasesAtEnd === 4)
  }

  test("process cigar clipped on both ends") {
    assert(bothClippedCigar.softClippedBasesAtStart === 1)
    assert(bothClippedCigar.softClippedBasesAtEnd === 3)
  }
}
