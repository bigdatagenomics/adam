/*
 * Copyright (c) 2014. Regents of the University of California
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
package edu.berkeley.cs.amplab.adam.models

import edu.berkeley.cs.amplab.adam.avro.{ADAMContig, ADAMVariant}
import edu.berkeley.cs.amplab.adam.util.SparkFunSuite

class IndelTableSuite extends SparkFunSuite {

  // indel table containing a 1 bp deletion at chr1, pos 1000
  val indelTable = new IndelTable(Map(1 -> Seq(Consensus("", ReferenceRegion(1, 1000L, 1002L)))))

  test ("check for indels in a region with known indels") {
    assert(indelTable.getIndelsInRegion(ReferenceRegion(1, 0L, 2000L)).length === 1)
  }

  test ("check for indels in a contig that doesn't exist") {
    assert(indelTable.getIndelsInRegion(ReferenceRegion(0, 0L, 0L)).length === 0)
  }

  test ("check for indels in a region without known indels") {
    assert(indelTable.getIndelsInRegion(ReferenceRegion(1, 1002L, 1005L)).length === 0)
  }

  sparkTest("build indel table from rdd of variants") {
    val ctg1 = ADAMContig.newBuilder()
      .setContigId(1)
      .setContigName("chr1")
      .build()
    val ctg2 = ADAMContig.newBuilder()
      .setContigId(2)
      .setContigName("chr2")
      .build()
    val ins = ADAMVariant.newBuilder()
      .setContig(ctg1)
      .setPosition(1000L)
      .setReferenceAllele("A")
      .setVariantAllele("ATT")
      .build()
    val del = ADAMVariant.newBuilder()
      .setContig(ctg2)
      .setPosition(50L)
      .setReferenceAllele("ACAT")
      .setVariantAllele("A")
      .build()

    val rdd = sc.parallelize(Seq(ins, del))

    val table = IndelTable(rdd)

    // check insert
    val insT = table.getIndelsInRegion(ReferenceRegion(1, 1000L, 1010L))
    assert(insT.length === 1)
    assert(insT.head.consensus === "TT")
    assert(insT.head.index.refId === 1)
    assert(insT.head.index.start === 1001)
    assert(insT.head.index.end === 1002)

    // check delete
    val delT = table.getIndelsInRegion(ReferenceRegion(2, 40L, 60L))
    assert(delT.length === 1)
    assert(delT.head.consensus === "")
    assert(delT.head.index.refId === 2)
    assert(delT.head.index.start === 51)
    assert(delT.head.index.end === 54)
  }

}
