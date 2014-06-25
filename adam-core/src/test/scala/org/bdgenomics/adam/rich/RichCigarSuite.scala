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

import org.scalatest.FunSuite
import org.bdgenomics.formats.avro.ADAMRecord
import RichADAMRecord._
import net.sf.samtools.Cigar
import RichCigar._

class RichCigarSuite extends FunSuite {

  test("moving 2 bp from a deletion to a match operator") {
    val read = ADAMRecord.newBuilder()
      .setReadMapped(true)
      .setStart(0)
      .setCigar("10M10D10M")
      .build()

    val newCigar = new Cigar(read.samtoolsCigar.getCigarElements).moveLeft(1)
    val newCigar2 = new Cigar(newCigar.getCigarElements).moveLeft(1)

    assert(newCigar2.getReadLength == read.samtoolsCigar.getReadLength)
    assert(newCigar2.toString === "8M10D12M")
  }

  test("moving 2 bp from a insertion to a match operator") {
    val read = ADAMRecord
      .newBuilder()
      .setReadMapped(true)
      .setStart(0)
      .setCigar("10M10I10M")
      .build()

    val newCigar = new Cigar(read.samtoolsCigar.getCigarElements).moveLeft(1)
    val newCigar2 = new Cigar(newCigar.getCigarElements).moveLeft(1)

    assert(newCigar2.getReadLength == read.samtoolsCigar.getReadLength)
    assert(newCigar2.toString === "8M10I12M")
  }

  test("moving 1 base in a two element cigar") {
    val read = ADAMRecord
      .newBuilder()
      .setReadMapped(true)
      .setStart(0)
      .setCigar("10M1D")
      .build()

    val newCigar = new Cigar(read.samtoolsCigar.getCigarElements).moveLeft(1)

    assert(newCigar.getReadLength == read.samtoolsCigar.getReadLength)
    assert(newCigar.toString === "9M1D1M")
  }

  test("move to start of read") {
    val read = ADAMRecord
      .newBuilder()
      .setReadMapped(true)
      .setStart(0)
      .setCigar("1M1D1M")
      .build()

    val newCigar = new Cigar(read.samtoolsCigar.getCigarElements).moveLeft(1)

    assert(newCigar.getReadLength == read.samtoolsCigar.getReadLength)
    assert(newCigar.toString === "1D2M")
  }

}
