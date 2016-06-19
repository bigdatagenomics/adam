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
package org.bdgenomics.adam.converters

import org.bdgenomics.formats.avro.AlignmentRecord
import org.ga4gh.GACigarOperation
import org.scalatest.FunSuite

class GA4GHConverterSuite extends FunSuite {

  test("converting an empty cigar should yield an empty cigar") {
    assert(GA4GHConverter.convertCigar(null).length === 0)
  }

  test("converting a pure match cigar should work") {
    val cigarList = GA4GHConverter.convertCigar("100M")
    assert(cigarList.length === 1)

    val headElem = cigarList.head
    assert(headElem.getOperation === GACigarOperation.ALIGNMENT_MATCH)
    assert(headElem.getOperationLength === 100)
  }

  test("convert a more complex cigar") {
    val cigarList = GA4GHConverter.convertCigar("50M10D3I47M")
    assert(cigarList.length === 4)

    def checkElem(idx: Int, op: GACigarOperation, len: Int) {
      val elem = cigarList(idx)
      assert(elem.getOperation === op)
      assert(elem.getOperationLength === len)
    }

    checkElem(0, GACigarOperation.ALIGNMENT_MATCH, 50)
    checkElem(1, GACigarOperation.DELETE, 10)
    checkElem(2, GACigarOperation.INSERT, 3)
    checkElem(3, GACigarOperation.ALIGNMENT_MATCH, 47)
  }

  def makeRead(start: Long, cigar: String, mdtag: String, length: Int, id: Int = 0, nullQuality: Boolean = false): AlignmentRecord.Builder = {
    val sequence: String = "A" * length
    val qual: String = "*" * length
    val builder = AlignmentRecord.newBuilder()
      .setReadName("read" + id.toString)
      .setStart(start)
      .setReadMapped(true)
      .setCigar(cigar)
      .setSequence(sequence)
      .setReadNegativeStrand(false)
      .setMapq(60)
      .setMismatchingPositions(mdtag)
      .setOldPosition(12)
      .setOldCigar("2^AAA3")
      .setRecordGroupName("rg")
      .setContigName("myCtg")

    if (!nullQuality) {
      builder.setQual(qual) // no typo, we just don't care
    }

    builder
  }

  test("converting a read without a read group fails") {
    intercept[IllegalArgumentException] {
      GA4GHConverter.toGAReadAlignment(makeRead(10L, "10M", "10", 10)
        .setRecordGroupName(null)
        .build())
    }
  }

  test("converting a read without a name fails") {
    intercept[IllegalArgumentException] {
      GA4GHConverter.toGAReadAlignment(makeRead(10L, "10M", "10", 10)
        .setReadName(null)
        .build())
    }
  }

  test("converting a read without a start fails") {
    intercept[IllegalArgumentException] {
      GA4GHConverter.toGAReadAlignment(makeRead(10L, "10M", "10", 10)
        .setStart(null)
        .build())
    }
  }

  test("converting a read without a contig fails") {
    intercept[IllegalArgumentException] {
      GA4GHConverter.toGAReadAlignment(makeRead(10L, "10M", "10", 10)
        .setContigName(null)
        .build())
    }
  }

  test("converting a read without a strand fails") {
    intercept[IllegalArgumentException] {
      GA4GHConverter.toGAReadAlignment(makeRead(10L, "10M", "10", 10)
        .setReadNegativeStrand(null)
        .build())
    }
  }

  test("converting a properly formatted read succeeds") {
    val adamRead = makeRead(10L, "10M", "10", 10).build()
    val gaRead = GA4GHConverter.toGAReadAlignment(adamRead)

    // check name
    assert(gaRead.getReadGroupId === "rg")
    assert(gaRead.getFragmentName === "read0")

    // check alignment status
    assert(gaRead.getAlignment != null)
    assert(gaRead.getAlignment.getCigar.size === 1)
    assert(gaRead.getAlignment.getMappingQuality === 60)
    assert(gaRead.getAlignment.getPosition != null)
    assert(gaRead.getAlignment.getPosition.getReferenceName === "myCtg")
    assert(gaRead.getAlignment.getPosition.getPosition === 10)
    assert(!gaRead.getAlignment.getPosition.getReverseStrand)

    // check sequence and qual
    assert(gaRead.getAlignedSequence === "AAAAAAAAAA")
    val qual = gaRead.getAlignedQuality
    assert(qual.size === 10)
    (0 until 10).foreach(i => {
      assert(qual.get(i) === 9)
    })
  }
}
