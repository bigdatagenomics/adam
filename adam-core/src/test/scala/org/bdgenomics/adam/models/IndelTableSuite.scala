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

import org.bdgenomics.adam.algorithms.consensus.Consensus
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.Variant

class IndelTableSuite extends ADAMFunSuite {

  // indel table containing a 1 bp deletion at chr1, pos 1000
  val indelTable = new IndelTable(Map("1" -> Iterable(Consensus("", ReferenceRegion("1", 1000L, 1002L)))))

  test("check for indels in a region with known indels") {
    assert(indelTable.getIndelsInRegion(ReferenceRegion("1", 0L, 2000L)).length === 1)
  }

  test("check for indels in a contig that doesn't exist") {
    assert(indelTable.getIndelsInRegion(ReferenceRegion("0", 0L, 1L)).length === 0)
  }

  test("check for indels in a region without known indels") {
    assert(indelTable.getIndelsInRegion(ReferenceRegion("1", 1002L, 1005L)).length === 0)
  }

  sparkTest("build indel table from rdd of variants") {
    val ins = Variant.newBuilder()
      .setReferenceName("1")
      .setStart(1000L)
      .setReferenceAllele("A")
      .setAlternateAllele("ATT")
      .build()
    val del = Variant.newBuilder()
      .setReferenceName("2")
      .setStart(50L)
      .setReferenceAllele("ACAT")
      .setAlternateAllele("A")
      .build()

    val rdd = sc.parallelize(Seq(ins, del))

    val table = IndelTable(rdd)

    // check insert
    val insT = table.getIndelsInRegion(ReferenceRegion("1", 1000L, 1010L))
    assert(insT.length === 1)
    assert(insT.head.consensus === "TT")
    assert(insT.head.index.referenceName === "1")
    assert(insT.head.index.start === 1001)
    assert(insT.head.index.end === 1002)

    // check delete
    val delT = table.getIndelsInRegion(ReferenceRegion("2", 40L, 60L))
    assert(delT.length === 1)
    assert(delT.head.consensus === "")
    assert(delT.head.index.referenceName === "2")
    assert(delT.head.index.start === 51)
    assert(delT.head.index.end === 55)
  }

}
