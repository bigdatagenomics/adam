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

import htsjdk.samtools.ValidationStringency
import org.apache.hadoop.io.Text
import org.scalatest.FunSuite

/**
 * Created by zyxue on 2016-09-27.
 */
class FastqConverterSuite extends FunSuite {
  val converter = new FastqRecordConverter

  test("testing FastqRecordConverter.convertPair with valid input") {
    val input = (null, new Text("@read/1\nATCGA\n+\nabcde\n@read/2\nTCGAT\n+\n12345"))
    val alignRecIterator = converter.convertPair(input)

    assert(alignRecIterator.map(a => a.getReadName) === List("read", "read"))
    assert(alignRecIterator.map(a => a.getSequence) === List("ATCGA", "TCGAT"))
    assert(alignRecIterator.map(a => a.getQual) === List("abcde", "12345"))
    assert(alignRecIterator.map(a => a.getReadPaired) === List(true, true))
    assert(alignRecIterator.map(a => a.getReadInFragment) === List(0, 1))
  }

  test("testing FastqRecordConverter.convertPair with 7-line invalid input") {
    val input = (null, new Text("@read/1\nATCGA\n+\nabcde\n@read/2\nTCGAT\n+"))
    intercept[IllegalArgumentException] {
      converter.convertPair(input)
    }
  }

  test("testing FastqRecordConverter.convertPair with invalid input: first read length and qual don't match") {
    val input = (null, new Text("@read/1\nATCGA\n+\nabcd\n@read/2\nTCGAT\n+\n12345"))
    intercept[IllegalArgumentException] {
      converter.convertPair(input)
    }
  }

  test("testing FastqRecordConverter.convertPair with invalid input: second read length and qual don't match") {
    val input = (null, new Text("@read/1\nATCGA\n+\nabcde\n@read/2\nTCGAT\n+\n1234"))
    intercept[IllegalArgumentException] {
      converter.convertPair(input)
    }
  }

  test("testing FastqRecordConverter.convertFragment with valid input") {
    val input = (null, new Text("@read\nATCGA\n+\nabcde\n@read\nTCGAT\n+\n12345"))
    val fragment = converter.convertFragment(input)
    assert(fragment.getReadName == "read")

    // more detailed testing
    val align1 = fragment.getAlignments.get(0)
    assert(align1.getReadName === "read")
    assert(align1.getSequence === "ATCGA")
    assert(align1.getQual === "abcde")
    assert(align1.getReadPaired === true)
    assert(align1.getReadInFragment === 0)

    val align2 = fragment.getAlignments.get(1)
    assert(align2.getReadName === "read")
    assert(align2.getSequence === "TCGAT")
    assert(align2.getQual === "12345")
    assert(align2.getReadPaired === true)
    assert(align2.getReadInFragment === 1)
  }

  test("testing FastqRecordConverter.convertFragment with another valid input having /1, /2 suffixes") {
    val input = (null, new Text("@read/1\nATCGA\n+\nabcde\n@read/2\nTCGAT\n+\n12345"))
    val fragment = converter.convertFragment(input)
    assert(fragment.getReadName == "read")

    // more detailed testing
    val align1 = fragment.getAlignments.get(0)
    assert(align1.getReadName === "read")
    assert(align1.getSequence === "ATCGA")
    assert(align1.getQual === "abcde")
    assert(align1.getReadPaired === true)
    assert(align1.getReadInFragment === 0)

    val align2 = fragment.getAlignments.get(1)
    assert(align2.getReadName === "read")
    assert(align2.getSequence === "TCGAT")
    assert(align2.getQual === "12345")
    assert(align2.getReadPaired === true)
    assert(align2.getReadInFragment === 1)
  }

  test("testing FastqRecordConverter.convertFragment with invalid input: different read names") {
    val input = (null, new Text("@nameX\nATCGA\n+\nabcde\n@nameY/2\nTCGAT\n+\n12345"))
    intercept[IllegalArgumentException] {
      converter.convertFragment(input)
    }
  }

  test("testing FastqRecordConverter.convertRead with valid input") {
    val input = (null, new Text("@nameX\nATCGA\n+\nabcde"))
    val alignment = converter.convertRead(input)
    assert(alignment.getReadName === "nameX")
    assert(alignment.getSequence === "ATCGA")
    assert(alignment.getQual === "abcde")
    assert(alignment.getReadPaired === false)
  }

  test("testing FastqRecordConverter.convertRead with valid input, no qual, strict") {
    val input = (null, new Text("@nameX\nATCGA\n+\n*"))
    intercept[IllegalArgumentException] {
      converter.convertRead(input)
    }
  }

  test("testing FastqRecordConverter.convertRead with valid input, no qual, not strict") {
    val input = (null, new Text("@nameX\nATCGA\n+\n*"))
    val alignment = converter.convertRead(input, stringency = ValidationStringency.LENIENT)
    assert(alignment.getReadName === "nameX")
    assert(alignment.getQual === "BBBBB")
  }
}
