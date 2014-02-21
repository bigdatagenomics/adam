/**
 * Copyright 2014. Regents of the University of California.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.cs.amplab.adam.models

import org.scalatest.FunSuite
import edu.berkeley.cs.amplab.adam.avro.{ADAMContig, 
                                         ADAMGenotype, 
                                         ADAMPileup,
                                         ADAMRecord,
                                         ADAMVariant}

class ReferencePositionSuite extends FunSuite {

  test("create reference position from mapped read") {
    val read = ADAMRecord.newBuilder()
      .setStart(1L)
      .setReferenceId(1)
      .setReadMapped(true)
      .build()

    val refPosOpt = ReferencePosition(read)
    
    assert(refPosOpt.isDefined)
    
    val refPos = refPosOpt.get

    assert(refPos.referenceName === "1")
    assert(refPos.pos === 1L)
  }

  test("create reference position from unmapped read") {
    val read = ADAMRecord.newBuilder()
      .setReadMapped(false)
      .build()

    val refPosOpt = ReferencePosition(read)
   
    assert(refPosOpt.isEmpty)
  }

  test("create reference position from pileup") {
    val pileup = ADAMPileup.newBuilder()
      .setPosition(2L)
      .setReferenceId(2)
      .build()

    val refPos = ReferencePosition(pileup)
    
    assert(refPos.referenceName === "2")
    assert(refPos.pos === 2L)
  }

  test("create reference position from variant") {
    val contig = ADAMContig.newBuilder()
      .setContigName("10").build
    val variant = ADAMVariant.newBuilder()
      .setPosition(10L)
      .setContig(contig)
      .build()

    val refPos = ReferencePosition(variant)
    
    assert(refPos.pos === 10L)
  }

  test("create reference position from genotype") {
    val contig = ADAMContig.newBuilder()
      .setContigName("100").build
    val variant = ADAMVariant.newBuilder()
      .setPosition(100L)
      .setContig(contig)
      .build
    val genotype = ADAMGenotype.newBuilder()
      .setVariant(variant)
      .build()

    val refPos = ReferencePosition(genotype)
    
    assert(refPos.referenceName === "100")
    assert(refPos.pos === 100L)
  }
}
