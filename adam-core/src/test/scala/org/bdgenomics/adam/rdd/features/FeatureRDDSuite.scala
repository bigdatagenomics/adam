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
import java.io.File
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ Feature, Strand }
import org.scalactic.{ Equivalence, TypeCheckedTripleEquals }

class FeatureRDDSuite extends ADAMFunSuite with TypeCheckedTripleEquals {
  implicit val strongFeatureEq = new Equivalence[Feature] {
    def areEquivalent(a: Feature, b: Feature): Boolean = {
      a.getContigName === b.getContigName &&
        a.getStart === b.getStart &&
        a.getEnd === b.getEnd &&
        a.getStrand === b.getStrand &&
        a.getFeatureId === b.getFeatureId &&
        a.getName === b.getName &&
        a.getFeatureType === b.getFeatureType &&
        a.getSource === b.getSource &&
        a.getPhase === b.getPhase &&
        a.getFrame === b.getFrame &&
        a.getScore === b.getScore &&
        a.getGeneId === b.getGeneId &&
        a.getTranscriptId === b.getTranscriptId &&
        a.getExonId === b.getExonId &&
        a.getTarget === b.getTarget &&
        a.getGap === b.getGap &&
        a.getDerivesFrom === b.getDerivesFrom &&
        a.getCircular === b.getCircular &&
        a.getAliases === b.getAliases &&
        a.getNotes === b.getNotes &&
        a.getParentIds === b.getParentIds &&
        a.getDbxrefs === b.getDbxrefs &&
        a.getOntologyTerms === b.getOntologyTerms &&
        a.getAttributes === b.getAttributes
    }
  }

  def tempLocation(suffix: String = ".adam"): String = {
    val tempFile = File.createTempFile("FeatureRDDFunctionsSuite", "")
    val tempDir = tempFile.getParentFile
    new File(tempDir, tempFile.getName + suffix).getAbsolutePath
  }

  sparkTest("save GTF as GTF format") {
    val inputPath = resourcePath("Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)
    val outputPath = tempLocation(".gtf")
    features.saveAsGtf(outputPath)
  }

  sparkTest("save GTF as GFF3 format") {
    val inputPath = resourcePath("Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)
    val outputPath = tempLocation(".gff3")
    features.saveAsGff3(outputPath)
  }

  sparkTest("save GTF as BED format") {
    val inputPath = resourcePath("Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)
    val outputPath = tempLocation(".bed")
    features.saveAsBed(outputPath)
  }

  sparkTest("save GTF as IntervalList format") {
    val inputPath = resourcePath("Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)
    val outputPath = tempLocation(".interval_list")
    features.saveAsIntervalList(outputPath)
  }

  sparkTest("save GTF as NarrowPeak format") {
    val inputPath = resourcePath("Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)
    val outputPath = tempLocation(".narrowPeak")
    features.saveAsNarrowPeak(outputPath)
  }

  sparkTest("round trip GTF format") {
    val inputPath = resourcePath("Homo_sapiens.GRCh37.75.trun100.gtf")
    val expected = sc.loadGtf(inputPath)
    val outputPath = tempLocation(".gtf")
    expected.saveAsGtf(outputPath)

    // grab all partitions, may not necessarily be in order; sort by reference
    val actual = sc.loadGtf(outputPath + "/part-*")
    val pairs = expected.transform(_.coalesce(1)).sortByReference().rdd.zip(actual.transform(_.coalesce(1)).sortByReference().rdd).collect

    // separate foreach since assert is not serializable
    pairs.foreach({ pair: (Feature, Feature) => assert(pair._1 === pair._2) })
  }

  sparkTest("save GFF3 as GTF format") {
    val inputPath = resourcePath("dvl1.200.gff3")
    val features = sc.loadGff3(inputPath)
    val outputPath = tempLocation(".gtf")
    features.saveAsGtf(outputPath)
  }

  sparkTest("save GFF3 as BED format") {
    val inputPath = resourcePath("dvl1.200.gff3")
    val features = sc.loadGff3(inputPath)
    val outputPath = tempLocation(".bed")
    features.saveAsBed(outputPath)
  }

  sparkTest("save GFF3 as IntervalList format") {
    val inputPath = resourcePath("dvl1.200.gff3")
    val features = sc.loadGff3(inputPath)
    val outputPath = tempLocation(".interval_list")
    features.saveAsIntervalList(outputPath)
  }

  sparkTest("save GFF3 as NarrowPeak format") {
    val inputPath = resourcePath("dvl1.200.gff3")
    val features = sc.loadGff3(inputPath)
    val outputPath = tempLocation(".narrowPeak")
    features.saveAsNarrowPeak(outputPath)
  }

  sparkTest("round trip GFF3 format") {
    val inputPath = resourcePath("dvl1.200.gff3")
    val expected = sc.loadGff3(inputPath)
    val outputPath = tempLocation(".gff3")
    expected.saveAsGff3(outputPath)

    // grab all partitions, may not necessarily be in order; sort by reference
    val actual = sc.loadGff3(outputPath + "/part-*")
    val pairs = expected.transform(_.coalesce(1)).sortByReference().rdd.zip(actual.transform(_.coalesce(1)).sortByReference().rdd).collect

    // separate foreach since assert is not serializable
    pairs.foreach({ pair: (Feature, Feature) => assert(pair._1 === pair._2) })
  }

  sparkTest("save BED as GTF format") {
    val inputPath = resourcePath("dvl1.200.bed")
    val features = sc.loadBed(inputPath)
    val outputPath = tempLocation(".gtf")
    features.saveAsGtf(outputPath)
  }

  sparkTest("save BED as GFF3 format") {
    val inputPath = resourcePath("dvl1.200.bed")
    val features = sc.loadBed(inputPath)
    val outputPath = tempLocation(".gff3")
    features.saveAsGff3(outputPath)
  }

  sparkTest("save BED as BED format") {
    val inputPath = resourcePath("dvl1.200.bed")
    val features = sc.loadBed(inputPath)
    val outputPath = tempLocation(".bed")
    features.saveAsBed(outputPath)
  }

  sparkTest("save BED as IntervalList format") {
    val inputPath = resourcePath("dvl1.200.bed")
    val features = sc.loadBed(inputPath)
    val outputPath = tempLocation(".interval_list")
    features.saveAsIntervalList(outputPath)
  }

  sparkTest("save BED as NarrowPeak format") {
    val inputPath = resourcePath("dvl1.200.bed")
    val features = sc.loadBed(inputPath)
    val outputPath = tempLocation(".narrowPeak")
    features.saveAsNarrowPeak(outputPath)
  }

  sparkTest("round trip BED format") {
    val inputPath = resourcePath("dvl1.200.bed")
    val expected = sc.loadBed(inputPath)
    val outputPath = tempLocation(".bed")
    expected.saveAsBed(outputPath)

    // grab all partitions, may not necessarily be in order; sort by reference
    val actual = sc.loadBed(outputPath + "/part-*")
    val pairs = expected.transform(_.coalesce(1)).sortByReference().rdd.zip(actual.transform(_.coalesce(1)).sortByReference().rdd).collect

    // separate since assert is not serializable
    pairs.foreach({ pair: (Feature, Feature) => assert(pair._1 === pair._2) })
  }

  sparkTest("save IntervalList as GTF format") {
    val inputPath = resourcePath("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    val outputPath = tempLocation(".gtf")
    features.saveAsGtf(outputPath)
  }

  sparkTest("save IntervalList as GFF3 format") {
    val inputPath = resourcePath("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    val outputPath = tempLocation(".gff3")
    features.saveAsGff3(outputPath)
  }

  sparkTest("save IntervalList as BED format") {
    val inputPath = resourcePath("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    val outputPath = tempLocation(".bed")
    features.saveAsBed(outputPath)
  }

  sparkTest("save IntervalList as IntervalList format") {
    val inputPath = resourcePath("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    val outputPath = tempLocation(".interval_list")
    features.saveAsIntervalList(outputPath)
  }

  sparkTest("save IntervalList as NarrowPeak format") {
    val inputPath = resourcePath("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    val outputPath = tempLocation(".narrowPeak")
    features.saveAsNarrowPeak(outputPath)
  }

  ignore("round trip IntervalList format") { // writing IntervalList headers is not yet supported
    val inputPath = resourcePath("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val expected = sc.loadIntervalList(inputPath)
    val outputPath = tempLocation(".interval_list")
    expected.saveAsIntervalList(outputPath)

    // grab all partitions, may not necessarily be in order; sort by reference
    val actual = sc.loadIntervalList(outputPath + "/part-*")
    val pairs = expected.transform(_.coalesce(1)).sortByReference().rdd.zip(actual.transform(_.coalesce(1)).sortByReference().rdd).collect

    // separate foreach since assert is not serializable
    pairs.foreach({ pair: (Feature, Feature) => assert(pair._1 === pair._2) })
  }

  sparkTest("save NarrowPeak as GTF format") {
    val inputPath = resourcePath("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    val outputPath = tempLocation(".gtf")
    features.saveAsGtf(outputPath)
  }

  sparkTest("save NarrowPeak as GFF3 format") {
    val inputPath = resourcePath("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    val outputPath = tempLocation(".gff3")
    features.saveAsGff3(outputPath)
  }

  sparkTest("save NarrowPeak as BED format") {
    val inputPath = resourcePath("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    val outputPath = tempLocation(".bed")
    features.saveAsBed(outputPath)
  }

  sparkTest("save NarrowPeak as IntervalList format") {
    val inputPath = resourcePath("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    val outputPath = tempLocation(".interval_list")
    features.saveAsIntervalList(outputPath)
  }

  sparkTest("save NarrowPeak as NarrowPeak format") {
    val inputPath = resourcePath("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    val outputPath = tempLocation(".narrowPeak")
    features.saveAsNarrowPeak(outputPath)
  }

  sparkTest("round trip NarrowPeak format") {
    val inputPath = resourcePath("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val expected = sc.loadNarrowPeak(inputPath)
    val outputPath = tempLocation(".narrowPeak")
    expected.saveAsNarrowPeak(outputPath)

    // grab all partitions, may not necessarily be in order; sort by reference
    val actual = sc.loadNarrowPeak(outputPath + "/part-*")
    val pairs = expected.transform(_.coalesce(1)).sortByReference().rdd.zip(actual.transform(_.coalesce(1)).sortByReference().rdd).collect

    // separate foreach since assert is not serializable
    pairs.foreach({ pair: (Feature, Feature) => assert(pair._1 === pair._2) })
  }

  sparkTest("sort by reference") {
    val fb = Feature.newBuilder()
    val f1 = fb.setContigName("1").setStart(1L).setEnd(100L).build()
    val f2 = fb.setContigName("1").setStart(10L).setEnd(110L).setStrand(Strand.FORWARD).build()
    val f3 = fb.setContigName("1").setStart(10L).setEnd(110L).setStrand(Strand.REVERSE).build()
    val f4 = fb.setContigName("1").setStart(10L).setEnd(110L).setStrand(Strand.INDEPENDENT).build()
    val f5 = fb.setContigName("1").setStart(10L).setEnd(110L).setStrand(Strand.UNKNOWN).build()
    val f6 = fb.setContigName("1").setStart(10L).setEnd(110L).clearStrand().build() // null strand last
    val f7 = fb.setContigName("2").build()

    val features = FeatureRDD(sc.parallelize(Seq(f7, f6, f5, f4, f3, f2, f1)))
    val sorted = features.sortByReference().rdd.collect()

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

    val features = FeatureRDD(sc.parallelize(Seq(f7, f6, f5, f4, f3, f2, f1)))
    val sorted = features.sortByReference().rdd.collect()

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

    val features = FeatureRDD(sc.parallelize(Seq(f3, f2, f1)))
    val sorted = features.sortByReference().rdd.collect()

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

    val features = FeatureRDD(sc.parallelize(Seq(f5, f4, f3, f2, f1)))
    val sorted = features.sortByReference().rdd.collect()

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

    val features = FeatureRDD(sc.parallelize(Seq(f9, f8, f7, f6, f5, f4, f3, f2, f1)))
    val sorted = features.sortByReference().rdd.collect()

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

    val features = FeatureRDD(sc.parallelize(Seq(f5, f4, f3, f2, f1)))
    val sorted = features.sortByReference().rdd.collect()

    assert(f1 == sorted(0))
    assert(f2 == sorted(1))
    assert(f3 == sorted(2))
    assert(f4 == sorted(3))
    assert(f5 == sorted(4))
  }
}
