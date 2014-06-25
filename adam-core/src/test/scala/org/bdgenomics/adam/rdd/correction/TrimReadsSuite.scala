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
package org.bdgenomics.adam.rdd.correction

import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.ADAMRecord
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.SparkFunSuite

class TrimReadsSuite extends SparkFunSuite {

  val ec = new TrimReads

  def makeRead(sequence: String, qual: String): ADAMRecord = {
    ADAMRecord.newBuilder
      .setSequence(sequence)
      .setQual(qual)
      .build
  }

  test("trim a few md tags") {
    assert(ec.trimMdTag("10", 2, 0) === "8")
    assert(ec.trimMdTag("2A10", 4, 0) === "9")
    assert(ec.trimMdTag("0C10C1", 1, 2) === "10")
    assert(ec.trimMdTag("1^AC3", 2, 0) === "2")
    assert(ec.trimMdTag("3^AC1", 0, 2) === "2")
    assert(ec.trimMdTag("2A0C0", 3, 0) === "0C0")
    assert(ec.trimMdTag("2A0C0", 0, 1) === "2A0")
  }

  test("trim a few cigars that just have clipping and matches") {
    // trim a single element incompletely
    assert(ec.trimCigar("2S10M", 1, 0, 0L) === ("1H1S10M", 0L))
    assert(ec.trimCigar("10M3S", 0, 2, 0L) === ("10M1S2H", 0L))
    assert(ec.trimCigar("2S10M3S", 1, 2, 0L) === ("1H1S10M1S2H", 0L))

    // trim a single element completely
    assert(ec.trimCigar("2S10M", 2, 0, 0L) === ("2H10M", 0L))
    assert(ec.trimCigar("10M3S", 0, 3, 0L) === ("10M3H", 0L))
    assert(ec.trimCigar("2S10M3S", 2, 3, 0L) === ("2H10M3H", 0L))

    // trim into multiple elements
    assert(ec.trimCigar("2S10M", 3, 0, 0L) === ("3H9M", 1L))
    assert(ec.trimCigar("10M3S", 0, 4, 0L) === ("9M4H", 0L))
    assert(ec.trimCigar("2S10M3S", 3, 4, 0L) === ("3H8M4H", 1L))
  }

  test("trim cigars with indels") {
    // trim through a deletion
    assert(ec.trimCigar("2S2M2D4M", 5, 0, 0L) === ("5H3M", 5L))
    assert(ec.trimCigar("4M1D1M", 0, 3, 0L) === ("2M3H", 0L))
    assert(ec.trimCigar("2S2M2N4M", 5, 0, 0L) === ("5H3M", 5L))
    assert(ec.trimCigar("4M1N1M", 0, 3, 0L) === ("2M3H", 0L))

    // trim into an insert
    assert(ec.trimCigar("2M2I10M", 3, 0, 0L) === ("3H1I10M", 2L))
    assert(ec.trimCigar("10M3I1M", 0, 3, 0L) === ("10M1I3H", 0L))
  }

  test("trim a few reads") {
    // trim from the front only, read without cigar
    val read1 = ADAMRecord.newBuilder
      .setSequence("ACTCGCCCACTCA")
      .setQual("##/9:::::::::")
      .build
    val trimmedRead1 = ec.trimRead(read1, 2, 0)

    assert(trimmedRead1.getSequence.toString === "TCGCCCACTCA")
    assert(trimmedRead1.getQual.toString === "/9:::::::::")

    // trim from both ends, read with cigar
    val read2 = ADAMRecord.newBuilder
      .setSequence("ACTCGCCCACTCAAA")
      .setQual("##/9:::::::::##")
      .setCigar("2S11M2S")
      .setStart(5L)
      .build
    val trimmedRead2 = ec.trimRead(read2, 2, 2)

    assert(trimmedRead2.getSequence.toString === "TCGCCCACTCA")
    assert(trimmedRead2.getQual.toString === "/9:::::::::")
    assert(trimmedRead2.getStart === 5L)
    assert(trimmedRead2.getCigar.toString === "2H11M2H")
  }

  sparkTest("correctly trim an RDD of reads") {
    // put the same sequence at start and end of reads
    val reads = Seq(makeRead("AACTCGACGCTTT", "##::::::::$$$"),
      makeRead("AACTCCCTGCTTT", "##::::::::$$$"),
      makeRead("AACTCATAGCTTT", "##::::::::$$$"),
      makeRead("AACTCCCAGCTTT", "##::::::::$$$"),
      makeRead("AACTCGGAGCTTT", "##::::::::$$$"))
    val rdd = sc.parallelize(reads)

    val trimFront = TrimReads(rdd, 2, 0)

    trimFront.collect.foreach(r => {
      assert(r.getSequence.length === 11)
      assert(r.getQual.length === 11)
      assert(r.getSequence.toString.startsWith("CT"))
      assert(r.getSequence.toString.endsWith("TTT"))
      assert(r.getQual.toString.startsWith("::"))
      assert(r.getQual.toString.endsWith("$$$"))
      assert(r.getBasesTrimmedFromStart === 2)
      assert(r.getBasesTrimmedFromEnd === 0)
    })

    val trimEnd = TrimReads(trimFront, 0, 3)

    trimEnd.collect.foreach(r => {
      assert(r.getSequence.length === 8, r.getSequence)
      assert(r.getQual.length === 8)
      assert(r.getSequence.toString.startsWith("CT"))
      assert(r.getSequence.toString.endsWith("GC"))
      assert(r.getQual.toString === "::::::::")
      assert(r.getBasesTrimmedFromStart === 2)
      assert(r.getBasesTrimmedFromEnd === 3)
    })
  }

  sparkTest("adaptively trim reads") {
    val readsFilepath = ClassLoader.getSystemClassLoader.getResource("bqsr1.sam").getFile
    val reads: RDD[ADAMRecord] = sc.adamLoad(readsFilepath)

    // put all reads into a single read group
    val readsSingleRG = reads.map(read => {
      ADAMRecord.newBuilder(read)
        .setRecordGroupId(0)
        .setRecordGroupName("group0")
        .build()
    })

    // trim reads - use phred Q10 as threshold
    val trimmed = TrimReads(readsSingleRG, 10)

    // we should trim the first and last 5 bases off all reads
    trimmed.collect.foreach(r => {
      assert(r.getBasesTrimmedFromStart === 5)
      assert(r.getBasesTrimmedFromEnd === 5)
    })
  }

}
