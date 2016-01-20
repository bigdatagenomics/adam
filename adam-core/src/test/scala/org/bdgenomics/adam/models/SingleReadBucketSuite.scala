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

import org.bdgenomics.adam.formats.avro.SingleReadBuckets
import org.bdgenomics.formats.avro._
import org.scalatest.FunSuite

import scala.collection.JavaConversions._

class SingleReadBucketSuite extends FunSuite {

  test("convert unmapped pair to fragment") {
    val reads = Iterable(AlignmentRecord.newBuilder()
      .setReadName("myRead")
      .setSequence("ACACACAC")
      .setQual("********")
      .setReadInFragment(0)
      .setReadPaired(true)
      .build(), AlignmentRecord.newBuilder()
      .setReadName("myRead")
      .setSequence("GTGTGTGT")
      .setQual(";;;;++++")
      .setReadInFragment(1)
      .setReadPaired(true)
      .build())
    val srbb: SingleReadBucket.Builder = SingleReadBucket.newBuilder()
    val srb = srbb.setUnmapped(reads.toList).build()
    val fragment = SingleReadBuckets.toFragment(srb)
    assert(fragment.getAlignments.size === 2)
    assert(fragment.getReadName === "myRead")
  }

  test("convert proper pair to fragment") {
    val reads = Iterable(AlignmentRecord.newBuilder()
      .setReadName("myRead")
      .setSequence("ACACACAC")
      .setQual("********")
      .setReadInFragment(0)
      .setReadPaired(true)
      .setReadMapped(true)
      .setPrimaryAlignment(true)
      .setReadNegativeStrand(false)
      .setCigar("8M")
      .setContig(Contig.newBuilder()
        .setContigName("1")
        .build())
      .setStart(10L)
      .setEnd(18L)
      .setInferredInsertSize(8L)
      .build(), AlignmentRecord.newBuilder()
      .setReadName("myRead")
      .setSequence("GTGTGTGT")
      .setQual(";;;;++++")
      .setReadInFragment(1)
      .setReadPaired(true)
      .setReadMapped(true)
      .setPrimaryAlignment(true)
      .setReadNegativeStrand(true)
      .setCigar("8M")
      .setContig(Contig.newBuilder()
        .setContigName("1")
        .build())
      .setStart(22L)
      .setEnd(30L)
      .setInferredInsertSize(8L)
      .build())
    val srbb: SingleReadBucket.Builder = SingleReadBucket.newBuilder()
    val srb = srbb.setPrimaryMapped(reads.toList).build()
    val fragment = SingleReadBuckets.toFragment(srb)
    assert(fragment.getFragmentSize === 8)
    assert(fragment.getReadName === "myRead")
    assert(fragment.getAlignments.size === 2)
  }

  test("convert read pair to fragment with first of pair chimeric read") {
    val reads = Iterable(AlignmentRecord.newBuilder()
      .setReadName("myRead")
      .setSequence("ACACACAC")
      .setQual("********")
      .setReadInFragment(0)
      .setReadPaired(true)
      .setReadMapped(true)
      .setReadNegativeStrand(false)
      .setPrimaryAlignment(true)
      .setCigar("8M6H")
      .setBasesTrimmedFromEnd(6)
      .setContig(Contig.newBuilder()
        .setContigName("1")
        .build())
      .setStart(10L)
      .setEnd(18L)
      .build(), AlignmentRecord.newBuilder()
      .setReadName("myRead")
      .setSequence("GTGTGTGT")
      .setQual(";;;;++++")
      .setReadInFragment(1)
      .setReadPaired(true)
      .setReadMapped(true)
      .setPrimaryAlignment(true)
      .setReadNegativeStrand(true)
      .setCigar("8M")
      .setContig(Contig.newBuilder()
        .setContigName("1")
        .build())
      .setStart(22L)
      .setEnd(30L)
      .build())
    val chimera = Iterable(AlignmentRecord.newBuilder()
      .setReadName("myRead")
      .setSequence("GTGTGT")
      .setQual("123456")
      .setReadInFragment(0)
      .setReadPaired(true)
      .setReadMapped(true)
      .setReadNegativeStrand(true)
      .setSupplementaryAlignment(true)
      .setBasesTrimmedFromStart(8)
      .setCigar("8H6M")
      .setContig(Contig.newBuilder()
        .setContigName("2")
        .build())
      .setStart(100L)
      .setEnd(106L)
      .build())
    val srbb: SingleReadBucket.Builder = SingleReadBucket.newBuilder()
    val srb = srbb.setPrimaryMapped(reads.toList).setSecondaryMapped(chimera.toList).build()

    val fragment = SingleReadBuckets.toFragment(srb)
    assert(fragment.getFragmentSize === null)
    assert(fragment.getReadName === "myRead")
    assert(fragment.getAlignments.size === 3)
  }
}

