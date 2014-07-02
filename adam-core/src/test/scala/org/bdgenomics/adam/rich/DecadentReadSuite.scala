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
/*
* Copyright (c) 2014. Mount Sinai School of Medicine
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

package org.bdgenomics.adam.rich

import org.scalatest.FunSuite
import org.bdgenomics.formats.avro.{ ADAMContig, ADAMRecord }
import org.bdgenomics.adam.models.ReferencePosition

class DecadentReadSuite extends FunSuite {

  test("reference position of decadent read") {
    val contig = ADAMContig.newBuilder
      .setContigName("chr1")
      .build

    val hardClippedRead = RichADAMRecord(ADAMRecord
      .newBuilder()
      .setReadMapped(true)
      .setStart(1000)
      .setContig(contig)
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
    val contig = ADAMContig.newBuilder
      .setContigName("chr1")
      .build

    val hardClippedRead = RichADAMRecord(ADAMRecord
      .newBuilder()
      .setReadMapped(true)
      .setStart(1000)
      .setContig(contig)
      .setMismatchingPositions("1TT10")
      .setSequence("ATTGGGGGGGGGG")
      .setQual("FFFFFFFFFFFFF")
      .setCigar("1M2I10M").build())

    val record = DecadentRead(hardClippedRead)
    assert(record.residues.size === 13)

    val residueSeq = record.residues
    assert(residueSeq.head.referencePosition === ReferencePosition("chr1", 1000))
  }

}
