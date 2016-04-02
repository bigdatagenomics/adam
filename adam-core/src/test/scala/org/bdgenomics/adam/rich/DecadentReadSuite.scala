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

import htsjdk.samtools.ValidationStringency
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferencePosition
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ AlignmentRecord, Contig }

class DecadentReadSuite extends ADAMFunSuite {

  test("reference position of decadent read") {
    val contig = Contig.newBuilder
      .setContigName("chr1")
      .build

    val hardClippedRead = RichAlignmentRecord(AlignmentRecord
      .newBuilder()
      .setReadMapped(true)
      .setStart(1000)
      .setContigName(contig.getContigName)
      .setMismatchingPositions("10")
      .setSequence("AACCTTGGC")
      .setQual("FFFFFFFFF")
      .setCigar("9M1H").build())

    val record = DecadentRead(hardClippedRead)
    assert(record.residues.size === 9)

    val residueSeq = record.residues
    assert(residueSeq.head.referencePosition === ReferencePosition("chr1", 1000))
  }

  test("reference position of decadent read with insertions") {
    val contig = Contig.newBuilder
      .setContigName("chr1")
      .build

    val hardClippedRead = RichAlignmentRecord(AlignmentRecord
      .newBuilder()
      .setReadMapped(true)
      .setStart(1000)
      .setContigName(contig.getContigName)
      .setMismatchingPositions("1TT10")
      .setSequence("ATTGGGGGGGGGG")
      .setQual("FFFFFFFFFFFFF")
      .setCigar("1M2I10M").build())

    val record = DecadentRead(hardClippedRead)
    assert(record.residues.size === 13)

    val residueSeq = record.residues
    assert(residueSeq.head.referencePosition === ReferencePosition("chr1", 1000))
  }

  test("build a decadent read from a read with null qual") {
    val contig = Contig.newBuilder
      .setContigName("chr1")
      .build

    val hardClippedRead = RichAlignmentRecord(AlignmentRecord
      .newBuilder()
      .setReadMapped(true)
      .setStart(1000)
      .setContigName(contig.getContigName)
      .setMismatchingPositions("10")
      .setSequence("AACCTTGGC")
      .setCigar("9M1H").build())

    val record = DecadentRead(hardClippedRead)
    assert(record.residues.size === 9)
  }

  test("converting bad read should fail") {
    val readBad = AlignmentRecord.newBuilder()
      .setContigName("1")
      .setStart(248262648L)
      .setEnd(248262721L)
      .setMapq(23)
      .setSequence("GATCTTTTCAACAGTTACAGCAGAAAGTTTTCATGGAGAAATGGAATCACACTTCAAATGATTTCATTTTGTTGGG")
      .setQual("IBBHEFFEKFCKFHFACKFIJFJDCFHFEEDJBCHIFIDDBCGJDBBJAJBJFCIDCACHBDEBHADDDADDAED;")
      .setCigar("4S1M1D71M")
      .setReadMapped(true)
      .setMismatchingPositions("3^C71")
      .build()

    intercept[IllegalArgumentException] {
      DecadentRead(readBad)
    }
  }

  def badGoodReadRDD: RDD[AlignmentRecord] = {
    val readBad = AlignmentRecord.newBuilder()
      .setContigName("1")
      .setStart(248262648L)
      .setEnd(248262721L)
      .setMapq(23)
      .setSequence("GATCTTTTCAACAGTTACAGCAGAAAGTTTTCATGGAGAAATGGAATCACACTTCAAATGATTTCATTTTGTTGGG")
      .setQual("IBBHEFFEKFCKFHFACKFIJFJDCFHFEEDJBCHIFIDDBCGJDBBJAJBJFCIDCACHBDEBHADDDADDAED;")
      .setCigar("4S1M1D71M")
      .setReadMapped(true)
      .setMismatchingPositions("3^C71")
      .build()
    val readGood = AlignmentRecord.newBuilder()
      .setContigName("1")
      .setStart(248262648L)
      .setEnd(248262721L)
      .setMapq(23)
      .setSequence("GATCTTTTCAACAGTTACAGCAGAAAGTTTTCATGGAGAAATGGAATCACACTTCAAATGATTTCATTTTGTTGGG")
      .setQual("IBBHEFFEKFCKFHFACKFIJFJDCFHFEEDJBCHIFIDDBCGJDBBJAJBJFCIDCACHBDEBHADDDADDAED;")
      .setCigar("4S1M1D71M")
      .setReadMapped(true)
      .setMismatchingPositions("1^C71")
      .build()
    sc.parallelize(Seq(readBad, readGood))
  }

  sparkTest("convert an RDD that has an bad read in it with loose validation") {
    val rdd = badGoodReadRDD
    val decadent = DecadentRead.cloy(rdd, ValidationStringency.LENIENT)
      .repartition(2)
      .collect()
    assert(decadent.size === 2)
    assert(decadent.count(_._1.isDefined) === 1)
    assert(decadent.count(_._1.isEmpty) === 1)
    assert(decadent.count(_._2.isDefined) === 1)
    assert(decadent.count(_._2.isEmpty) === 1)
  }

  sparkTest("converting an RDD that has an bad read in it with strict validation will throw an error") {
    val rdd = badGoodReadRDD
    intercept[SparkException] {
      // need count to force computation
      DecadentRead.cloy(rdd).count
    }
  }
}
