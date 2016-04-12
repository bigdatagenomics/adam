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
package org.bdgenomics.adam.rdd.features

import com.google.common.collect.ImmutableMap
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ Feature, Strand }

class FeaturesSuite extends ADAMFunSuite {
  sparkTest("sort by reference") {
    val fb = Feature.newBuilder()
    val f1 = fb.setContigName("1").setStart(1L).setEnd(100L).build()
    val f2 = fb.setContigName("1").setStart(10L).setEnd(110L).setStrand(Strand.FORWARD).build()
    val f3 = fb.setContigName("1").setStart(10L).setEnd(110L).setStrand(Strand.REVERSE).build()
    val f4 = fb.setContigName("1").setStart(10L).setEnd(110L).setStrand(Strand.INDEPENDENT).build()
    val f5 = fb.setContigName("1").setStart(10L).setEnd(110L).setStrand(Strand.UNKNOWN).build()
    val f6 = fb.setContigName("1").setStart(10L).setEnd(110L).clearStrand().build() // null strand last
    val f7 = fb.setContigName("2").build()

    val features: RDD[Feature] = sc.makeRDD(Seq(f7, f6, f5, f4, f3, f2, f1))
    val sorted = features.sortByReference().collect()

    assert(f1 == sorted(0))
    assert(f2 == sorted(1))
    assert(f3 == sorted(2))
    assert(f4 == sorted(3))
    assert(f5 == sorted(4))
    assert(f6 == sorted(5))
    assert(f7 == sorted(6))
  }

  sparkTest("sort by reference and feature fields") {
    val fb = Feature.newBuilder().setContigName("1").setStart(1L).setEnd(100L)
    val f1 = fb.setFeatureId("featureId").build()
    val f2 = fb.clearFeatureId().setName("name").build()
    val f3 = fb.clearName().setPhase(0).build()
    val f4 = fb.setPhase(1).build() // Int defaults to increasing sort order
    val f5 = fb.clearPhase().setScore(0.1).build()
    val f6 = fb.setScore(0.9).build() // Double defaults to increasing sort order
    val f7 = fb.clearScore().build() // nulls last

    val features: RDD[Feature] = sc.makeRDD(Seq(f7, f6, f5, f4, f3, f2, f1))
    val sorted = features.sortByReference().collect()

    assert(f1 == sorted(0))
    assert(f2 == sorted(1))
    assert(f3 == sorted(2))
    assert(f4 == sorted(3))
    assert(f5 == sorted(4))
    assert(f6 == sorted(5))
    assert(f7 == sorted(6))
  }

  sparkTest("sort gene features by reference and gene structure") {
    val fb = Feature.newBuilder().setContigName("1").setStart(1L).setEnd(100L).setFeatureType("gene")
    val f1 = fb.setGeneId("gene1").build()
    val f2 = fb.setGeneId("gene2").build()
    val f3 = fb.clearGeneId().build() // nulls last

    val features: RDD[Feature] = sc.makeRDD(Seq(f3, f2, f1))
    val sorted = features.sortByReference().collect()

    assert(f1 == sorted(0))
    assert(f2 == sorted(1))
    assert(f3 == sorted(2))
  }

  sparkTest("sort transcript features by reference and gene structure") {
    val fb = Feature.newBuilder().setContigName("1").setStart(1L).setEnd(100L).setFeatureType("transcript")
    val f1 = fb.setGeneId("gene1").setTranscriptId("transcript1").build()
    val f2 = fb.setGeneId("gene1").setTranscriptId("transcript1").build()
    val f3 = fb.setGeneId("gene2").setTranscriptId("transcript1").build()
    val f4 = fb.setGeneId("gene2").setTranscriptId("transcript2").build()
    val f5 = fb.setGeneId("gene2").clearTranscriptId().build() // nulls last

    val features: RDD[Feature] = sc.makeRDD(Seq(f5, f4, f3, f2, f1))
    val sorted = features.sortByReference().collect()

    assert(f1 == sorted(0))
    assert(f2 == sorted(1))
    assert(f3 == sorted(2))
    assert(f4 == sorted(3))
    assert(f5 == sorted(4))
  }

  sparkTest("sort exon features by reference and gene structure") {
    val fb = Feature.newBuilder().setContigName("1").setStart(1L).setEnd(100L).setFeatureType("exon")
    val f1 = fb.setGeneId("gene1").setTranscriptId("transcript1").setExonId("exon1").build()
    val f2 = fb.setGeneId("gene1").setTranscriptId("transcript1").setExonId("exon2").build()
    val f3 = fb.setGeneId("gene1").setTranscriptId("transcript2").setExonId("exon1").build()
    val f4 = fb.setGeneId("gene2").setTranscriptId("transcript1").setExonId("exon1").build()
    val f5 = fb.setGeneId("gene2").setTranscriptId("transcript1").clearExonId().setAttributes(ImmutableMap.of("exon_number", "1")).build()
    val f6 = fb.setGeneId("gene2").setTranscriptId("transcript1").setAttributes(ImmutableMap.of("exon_number", "2")).build()
    val f7 = fb.setGeneId("gene2").setTranscriptId("transcript1").setAttributes(ImmutableMap.of("rank", "1")).build()
    val f8 = fb.setGeneId("gene2").setTranscriptId("transcript1").setAttributes(ImmutableMap.of("rank", "2")).build()
    val f9 = fb.setGeneId("gene2").setTranscriptId("transcript1").clearAttributes().build() // nulls last

    val features: RDD[Feature] = sc.makeRDD(Seq(f9, f8, f7, f6, f5, f4, f3, f2, f1))
    val sorted = features.sortByReference().collect()

    assert(f1 == sorted(0))
    assert(f2 == sorted(1))
    assert(f3 == sorted(2))
    assert(f4 == sorted(3))
    assert(f5 == sorted(4))
    assert(f6 == sorted(5))
    assert(f7 == sorted(6))
    assert(f8 == sorted(7))
    assert(f9 == sorted(8))
  }

  sparkTest("sort intron features by reference and gene structure") {
    val fb = Feature.newBuilder().setContigName("1").setStart(1L).setEnd(100L).setGeneId("gene1").setTranscriptId("transcript1").setFeatureType("intron")
    val f1 = fb.setAttributes(ImmutableMap.of("intron_number", "1")).build()
    val f2 = fb.setAttributes(ImmutableMap.of("intron_number", "2")).build()
    val f3 = fb.setAttributes(ImmutableMap.of("rank", "1")).build()
    val f4 = fb.setAttributes(ImmutableMap.of("rank", "2")).build()
    val f5 = fb.clearAttributes().build() // nulls last

    val features: RDD[Feature] = sc.makeRDD(Seq(f5, f4, f3, f2, f1))
    val sorted = features.sortByReference().collect()

    assert(f1 == sorted(0))
    assert(f2 == sorted(1))
    assert(f3 == sorted(2))
    assert(f4 == sorted(3))
    assert(f5 == sorted(4))
  }
}
