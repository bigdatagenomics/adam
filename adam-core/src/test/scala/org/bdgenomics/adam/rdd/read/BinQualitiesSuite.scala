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
package org.bdgenomics.adam.rdd.read

import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ AlignmentRecord, Fragment }
import scala.collection.JavaConversions._

class BinQualitiesSuite extends ADAMFunSuite {

  test("make a quality score bin") {
    val bin = QualityScoreBin(0, 25, 12)

    assert(bin.optGetBase((-10 + 33).toChar).isEmpty)
    assert(bin.optGetBase((25 + 33).toChar).isEmpty)
    val optBase = bin.optGetBase((20 + 33).toChar)
    assert(optBase.isDefined)
    assert(optBase.get === (12 + 33).toChar)
  }

  test("can't have a quality score bin with negative score") {
    intercept[IllegalArgumentException] {
      QualityScoreBin(-5, 20, 10)
    }
  }

  test("can't have a quality score bin with high score below low") {
    intercept[IllegalArgumentException] {
      QualityScoreBin(15, 5, 10)
    }
  }

  test("can't have a quality score bin with high score above 255") {
    intercept[IllegalArgumentException] {
      QualityScoreBin(5, 270, 10)
    }
  }

  test("can't have a quality score bin with score outside") {
    intercept[IllegalArgumentException] {
      QualityScoreBin(5, 20, 25)
    }
  }

  test("make a quality score bin from a string") {
    val bin = QualityScoreBin.parseBin("0,10,5")
    assert(bin.low === 0)
    assert(bin.high === 10)
    assert(bin.score === 5)
  }

  test("quality score bin must have exactly 3 elements") {
    intercept[IllegalArgumentException] {
      QualityScoreBin.parseBin("0,10")
    }
    intercept[IllegalArgumentException] {
      QualityScoreBin.parseBin("0,10,5,abc")
    }
  }

  test("quality score bin must be integers") {
    intercept[IllegalArgumentException] {
      QualityScoreBin.parseBin("0,10,abc")
    }
  }

  test("must define at least one bin") {
    intercept[IllegalArgumentException] {
      QualityScoreBin("")
    }
  }

  test("build multiple bins") {
    val bins = QualityScoreBin("0,10,5;10,20,15").toSet

    assert(bins.size === 2)
    assert(bins(QualityScoreBin(0, 10, 5)))
    assert(bins(QualityScoreBin(10, 20, 15)))
  }

  val bins = Seq(QualityScoreBin(0, 20, 10),
    QualityScoreBin(20, 50, 40))

  test("rewrite quality scores for a read") {
    val sequence = Seq(0, 5, 15, 20, 25, 20, 15).map(i => (i + 33).toChar)
      .mkString

    val newSequence = BinQualities.binQualities(sequence, bins)
      .map(i => i.toInt - 33)

    assert(newSequence.length === sequence.length)
    assert(newSequence(0) === 10)
    assert(newSequence(1) === 10)
    assert(newSequence(2) === 10)
    assert(newSequence(3) === 40)
    assert(newSequence(4) === 40)
    assert(newSequence(5) === 40)
    assert(newSequence(6) === 10)
  }

  test("rewriting quality scores fails if bins overlap") {
    val binsPlusOne = bins ++ Seq(QualityScoreBin(10, 30, 20))
    val sequence = Seq(0, 5, 15, 20, 25, 20, 15).map(i => (i + 33).toChar)
      .mkString

    intercept[IllegalStateException] {
      BinQualities.binQualities(sequence, binsPlusOne)
    }
  }

  test("rewriting quality scores fails if base is out of bounds") {
    val sequence = Seq(0, 5, 15, 20, 55, 20, 15).map(i => (i + 33).toChar)
      .mkString

    intercept[IllegalStateException] {
      BinQualities.binQualities(sequence, bins)
    }
  }

  test("skip read if qualities are null") {
    val read = AlignmentRecord.newBuilder
      .setSequence("ACAGATTCG")
      .setReadName("aRead")
      .build

    val newRead = BinQualities.binRead(read, bins)

    assert(newRead === read)
  }

  test("rewrite a read") {
    val sequence = Seq(0, 10, 20, 25, 25, 22, 20, 19, 18)
      .map(i => (i + 33).toChar)
      .mkString

    val read = AlignmentRecord.newBuilder
      .setQuality(sequence)
      .setSequence("ACAGATTCG")
      .setReadName("aRead")
      .build

    val newRead = BinQualities.binRead(read, bins)

    assert(newRead.getSequence === "ACAGATTCG")
    assert(newRead.getReadName === "aRead")

    def testQuals(qualString: String) {
      val newQuals = qualString.map(i => i.toInt - 33)

      assert(newQuals.size === 9)
      assert(newQuals(0) === 10)
      assert(newQuals(1) === 10)
      assert(newQuals(2) === 40)
      assert(newQuals(3) === 40)
      assert(newQuals(4) === 40)
      assert(newQuals(5) === 40)
      assert(newQuals(6) === 40)
      assert(newQuals(7) === 10)
      assert(newQuals(8) === 10)
    }

    testQuals(newRead.getQuality)

    val fragment = Fragment.newBuilder
      .setName("testFragment")
      .setAlignments(List(read))
      .build

    val newFragment = BinQualities.binFragment(fragment, bins)

    assert(newFragment.getAlignments.size === 1)
    assert(newFragment.getName === "testFragment")

    testQuals(newFragment.getAlignments.head.getQuality)
  }
}
