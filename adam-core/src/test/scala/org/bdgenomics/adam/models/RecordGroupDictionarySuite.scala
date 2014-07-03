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

import java.util.Date
import net.sf.samtools.SAMReadGroupRecord
import org.bdgenomics.formats.avro.ADAMRecord
import org.scalatest.FunSuite

class RecordGroupDictionarySuite extends FunSuite {

  test("simple conversion to and from sam read group") {
    val origSAMRGR = new SAMReadGroupRecord("myId")
    origSAMRGR.setSample("mySample")
    val rg = RecordGroup(origSAMRGR)

    // sample and record name should be converted
    assert(rg.sample == "mySample")
    assert(rg.recordGroupName == "myId")

    val newSAMRGR = rg.toSAMReadGroupRecord

    // the two should be equals
    assert(origSAMRGR.equals(newSAMRGR))
  }

  test("sample name must be set") {
    val samRGR = new SAMReadGroupRecord("myId")
    intercept[AssertionError] {
      RecordGroup(samRGR)
    }
  }

  test("create a record group from a read") {
    val ar = ADAMRecord.newBuilder()
      .setRecordGroupSample("mySample")
      .setRecordGroupName("myName")
      .build()

    val rg = RecordGroup(ar)

    assert(rg.isDefined)
    assert(rg.get.sample === "mySample")
    assert(rg.get.recordGroupName === "myName")
  }

  test("don't create a record group from a read without record group info") {
    val ar = ADAMRecord.newBuilder()
      .build()

    val rg = RecordGroup(ar)

    assert(rg.isEmpty)
  }

  test("simple equality checks") {
    val rg1a = new RecordGroup("me", "rg1")
    val rg1b = RecordGroup(ADAMRecord.newBuilder()
      .setRecordGroupSample("me")
      .setRecordGroupName("rg1")
      .build())
    val rg2 = new RecordGroup("me", "rg2")
    val rg3 = new RecordGroup("you", "rg1") // all hail the king!

    assert(rg1a.equals(rg1a))
    assert(rg1a.sample === rg1b.get.sample)
    assert(rg1a.recordGroupName === rg1b.get.recordGroupName)
    assert(!rg1a.equals(rg1b))
    assert(rg1a.equals(rg1b.get))
    assert(!rg1a.equals(rg2))
    assert(!rg1a.equals(rg3))
  }
}
