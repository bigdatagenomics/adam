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
import org.scalatest.{ FunSuite, PrivateMethodTester }

class FastqRecordConverterSuite extends FunSuite with PrivateMethodTester {
  val converter = new FastqRecordConverter
  val lenient = ValidationStringency.LENIENT

  test("test read name suffix and index of pair must match") {
    for (
      (readName, isFirstOfPair) <- List(
        // if isFirstOfPair is false, then it's isSecondOfPair
        ("@desc/1", false),
        ("@desc/2", true)
      )
    ) intercept[IllegalArgumentException] {
      converter.readNameSuffixAndIndexOfPairMustMatch(readName, isFirstOfPair)
    }

    for (
      (readName, isFirstOfPair) <- List(
        ("@desc/1", true),
        ("@desc/2", false),
        // it can be considered to be either first or second of pair
        ("@desc", true),
        ("@desc", false)
      )
    ) // not exception raised
    converter.readNameSuffixAndIndexOfPairMustMatch(readName, isFirstOfPair)
  }

  test("test parseReadInFastq, read suffix removal") {
    // simple cases
    for (
      desc <- List(
        "@desc/1", "@desc/2",
        "@desc 1", "@desc 2",
        "@desc+1", "@desc+2",
        "@desc_1", "@desc_2"
      )
    ) assert(converter.parseReadInFastq(s"${desc}\nATCG\n+\n1234")._1 === "desc")

    // a bit more complicated cases
    for (
      desc <- List(
        "@more desc/1", "@more desc/2",
        "@more desc 1", "@more desc 2",
        "@more desc+1", "@more desc+2",
        "@more desc_1", "@more desc_2"
      )
    ) assert(converter.parseReadInFastq(s"${desc}\nATCG\n+\n1234")._1 === "more desc")
  }

  test("test parseReadInFastq, read metadata removal") {
    Seq("@desc 1:N:0:2",
      "@desc 2:Y:4:0",
      "@desc 1:N:0:1234").foreach(desc =>
        assert(converter.parseReadInFastq(s"${desc}\nATCG\n+\n1234")._1 === "desc"))
  }

  test("test parseReadInFastq, read quality shorter than read length, padded with B") {
    assert(converter.parseReadInFastq("@description\nAAA\n+\nZ", stringency = lenient) ===
      ("description", "AAA", "ZBB"))
  }

  test("test parseReadInFastq, read quality longer than read length ") {
    val thrown = intercept[IllegalArgumentException] {
      converter.parseReadInFastq("@description\nA\n+\nZZ", stringency = lenient)
    }
    assert(thrown.getMessage === "Quality length must not be longer than read length")
  }

  test("test parseReadInFastq, no read quality") {
    assert(converter.parseReadInFastq("@description\nAAA\n+\n*", stringency = lenient) ===
      ("description", "AAA", "BBB"))
  }

  test("testing FastqRecordConverter.convertPair with valid input") {
    val input = (null, new Text("@read/1\nATCGA\n+\nabcde\n@read/2\nTCGAT\n+\n12345"))
    val alignRecIterator = converter.convertPair(input)

    assert(alignRecIterator.map(a => a.getReadName) === List("read", "read"))
    assert(alignRecIterator.map(a => a.getSequence) === List("ATCGA", "TCGAT"))
    assert(alignRecIterator.map(a => a.getQuality) === List("abcde", "12345"))
    assert(alignRecIterator.map(a => a.getReadPaired) === List(true, true))
    assert(alignRecIterator.map(a => a.getReadInFragment) === List(0, 1))
  }

  test("testing FastqRecordConverter.convertPair with 7-line invalid input") {
    val input = (null, new Text("@read/1\nATCGA\n+\nabcde\n@read/2\nTCGAT\n+"))
    val thrown = intercept[IllegalArgumentException] {
      converter.convertPair(input)
    }
    assert(thrown.getMessage.contains("Record must have 8 lines (7 found)"))
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
    assert(fragment.getName == "read")

    // more detailed testing
    val align1 = fragment.getAlignments.get(0)
    assert(align1.getReadName === "read")
    assert(align1.getSequence === "ATCGA")
    assert(align1.getQuality === "abcde")
    assert(align1.getReadPaired === true)
    assert(align1.getReadInFragment === 0)

    val align2 = fragment.getAlignments.get(1)
    assert(align2.getReadName === "read")
    assert(align2.getSequence === "TCGAT")
    assert(align2.getQuality === "12345")
    assert(align2.getReadPaired === true)
    assert(align2.getReadInFragment === 1)
  }

  test("testing FastqRecordConverter.convertFragment with another valid input having /1, /2 suffixes") {
    val input = (null, new Text("@read/1\nATCGA\n+\nabcde\n@read/2\nTCGAT\n+\n12345"))
    val fragment = converter.convertFragment(input)
    assert(fragment.getName == "read")

    // more detailed testing
    val align1 = fragment.getAlignments.get(0)
    assert(align1.getReadName === "read")
    assert(align1.getSequence === "ATCGA")
    assert(align1.getQuality === "abcde")
    assert(align1.getReadPaired === true)
    assert(align1.getReadInFragment === 0)

    val align2 = fragment.getAlignments.get(1)
    assert(align2.getReadName === "read")
    assert(align2.getSequence === "TCGAT")
    assert(align2.getQuality === "12345")
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
    assert(alignment.getQuality === "abcde")
    assert(alignment.getReadPaired === false)
    assert(alignment.getReadInFragment === 0)
  }

  test("testing FastqRecordConverter.convertRead with valid input: setFirstOfPair set to true") {
    val alignment = converter.convertRead(
      (null, new Text("@nameX\nATCGA\n+\nabcde")),
      setFirstOfPair = true
    )
    assert(alignment.getReadPaired === true)
    assert(alignment.getReadInFragment === 0)
  }

  test("testing FastqRecordConverter.convertRead with valid input: setSecondOfPair set to true") {
    val alignment = converter.convertRead(
      (null, new Text("@nameX\nATCGA\n+\nabcde")),
      setSecondOfPair = true
    )
    assert(alignment.getReadPaired === true)
    assert(alignment.getReadInFragment === 1)
  }

  test("testing FastqRecordConverter.convertRead with valid input: setFirstOfPair and setSecondOfPair both true") {
    val thrown = intercept[IllegalArgumentException] {
      converter.convertRead(
        (null, new Text("@nameX\nATCGA\n+\nabcde")),
        setFirstOfPair = true,
        setSecondOfPair = true
      )
    }
    assert(thrown.getMessage === "setFirstOfPair and setSecondOfPair cannot be true at the same time")
  }

  test("testing FastqRecordConverter.convertRead with valid input, no qual, strict") {
    val input = (null, new Text("@nameX\nATCGA\n+\n*"))
    val thrown = intercept[IllegalArgumentException] {
      converter.convertRead(input)
    }
    assert(thrown.getMessage.contains("Fastq quality must be defined"))
  }

  test("testing FastqRecordConverter.convertRead with valid input, no qual, not strict") {
    val input = (null, new Text("@nameX\nATCGA\n+\n*"))
    val alignment = converter.convertRead(input, stringency = lenient)
    assert(alignment.getReadName === "nameX")
    assert(alignment.getQuality === "BBBBB")
  }
}
