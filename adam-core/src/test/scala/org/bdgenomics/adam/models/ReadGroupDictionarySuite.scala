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

import htsjdk.samtools.SAMReadGroupRecord
import org.scalatest.FunSuite

class ReadGroupDictionarySuite extends FunSuite {

  test("simple conversion to and from sam read group") {
    val origSAMRGR = new SAMReadGroupRecord("myId")
    origSAMRGR.setSample("mySample")
    val rg = ReadGroup(origSAMRGR)

    // sample and record name should be converted
    assert(rg.sampleId == "mySample")
    assert(rg.id == "myId")

    val newSAMRGR = rg.toSAMReadGroupRecord

    // the two should be equals
    assert(origSAMRGR.equals(newSAMRGR))
  }

  test("sample name must be set") {
    val samRGR = new SAMReadGroupRecord("myId")
    intercept[IllegalArgumentException] {
      ReadGroup(samRGR)
    }
  }

  test("simple equality checks") {
    val rg1a = new ReadGroup("me", "rg1")
    val rg1b = new ReadGroup("me", "rg1")
    val rg2 = new ReadGroup("me", "rg2")
    val rg3 = new ReadGroup("you", "rg1") // all hail the king!

    assert(rg1a.equals(rg1a))
    assert(rg1a.sampleId === rg1b.sampleId)
    assert(rg1a.id === rg1b.id)
    assert(!rg1a.equals(rg2))
    assert(!rg1a.equals(rg3))
    assert(!rg1b.equals(rg2))
    assert(!rg1b.equals(rg3))
    assert(!rg2.equals(rg3))
  }

  test("get samples from read group dictionary") {
    val rgd = ReadGroupDictionary(Seq(ReadGroup("sample1", "rg1"),
      ReadGroup("sample1", "rg2"),
      ReadGroup("sample1", "rg3"),
      ReadGroup("sample1", "rg4"),
      ReadGroup("sample1", "rg5"),
      ReadGroup("sample2", "rgSample2"),
      ReadGroup("sample3", "rg1Sample3"),
      ReadGroup("sample3", "rg2Sample3")))
    assert(!rgd.isEmpty)
    val samples = rgd.toSamples

    assert(samples.size === 3)
    assert(samples.count(_.getId == "sample1") === 1)
    assert(samples.count(_.getId == "sample2") === 1)
    assert(samples.count(_.getId == "sample3") === 1)
  }

  test("empty read group is empty") {
    val emptyRgd = ReadGroupDictionary.empty

    assert(emptyRgd.isEmpty)
    assert(emptyRgd.readGroups.size === 0)
  }

  test("merging a dictionary with itself should work") {
    val rgd = ReadGroupDictionary(Seq(ReadGroup("sample1", "rg1")))

    val mergedRgd = rgd ++ rgd
    assert(rgd === mergedRgd)
  }

  test("round trip a record with all attributes set") {
    val rg = new SAMReadGroupRecord("RG1")
    rg.setSample("S1")
    rg.setLibrary("L1")
    rg.setPlatformUnit("PU1")
    rg.setPlatform("ILLUMINA")
    rg.setFlowOrder("ACGT")
    rg.setKeySequence("AATT")
    rg.setSequencingCenter("BDG")
    rg.setDescription("TEST")
    rg.setPredictedMedianInsertSize(100)
    val arg = ReadGroup(rg)
    val htsjdkRg = arg.toSAMReadGroupRecord()
    assert(rg.equivalent(htsjdkRg))
  }
}
