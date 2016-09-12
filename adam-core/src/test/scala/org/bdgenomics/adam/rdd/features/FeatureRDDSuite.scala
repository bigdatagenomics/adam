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
        a.getIsCircular === b.getIsCircular &&
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

  sparkTest("round trip GTF format") {
    val inputPath = resourcePath("Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)

    val firstGtfRecord = FeatureRDD.toGtf(features.rdd.first)

    val gtfSplitTabs = firstGtfRecord.split('\t')
    assert(gtfSplitTabs.size === 9)
    assert(gtfSplitTabs(0) === "1")
    assert(gtfSplitTabs(1) === "pseudogene")
    assert(gtfSplitTabs(2) === "gene")
    assert(gtfSplitTabs(3) === "11869")
    assert(gtfSplitTabs(4) === "14412")
    assert(gtfSplitTabs(5) === ".")
    assert(gtfSplitTabs(6) === "+")
    assert(gtfSplitTabs(7) === ".")

    val gtfAttributes = gtfSplitTabs(8).split(";").map(_.trim)
    assert(gtfAttributes.size === 4)
    assert(gtfAttributes(0) === "gene_id \"ENSG00000223972\"")
    assert(gtfAttributes(1) === "gene_biotype \"pseudogene\"")
    // gene name/source move to the end
    assert(gtfAttributes(2) === "gene_name \"DDX11L1\"")
    assert(gtfAttributes(3) === "gene_source \"ensembl_havana\"")

    val outputPath = tempLocation(".gtf")
    features.saveAsGtf(outputPath, asSingleFile = true)
    val reloadedFeatures = sc.loadGtf(outputPath)
    assert(reloadedFeatures.rdd.count === features.rdd.count)
    val zippedFeatures = reloadedFeatures.rdd.zip(features.rdd).collect
    zippedFeatures.foreach(p => {
      assert(p._1 === p._2)
    })
  }

  sparkTest("save GTF as GFF3 format") {
    val inputPath = resourcePath("Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)
    val outputPath = tempLocation(".gff3")
    features.saveAsGff3(outputPath)
    val reloadedFeatures = sc.loadGff3(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save GTF as BED format") {
    val inputPath = resourcePath("Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)
    val outputPath = tempLocation(".bed")
    features.saveAsBed(outputPath)
    val reloadedFeatures = sc.loadBed(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save GTF as IntervalList format") {
    val inputPath = resourcePath("Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)
    val outputPath = tempLocation(".interval_list")
    features.saveAsIntervalList(outputPath)
    val reloadedFeatures = sc.loadIntervalList(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save GTF as NarrowPeak format") {
    val inputPath = resourcePath("Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)
    val outputPath = tempLocation(".narrowPeak")
    features.saveAsNarrowPeak(outputPath)
    val reloadedFeatures = sc.loadNarrowPeak(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save GFF3 as GTF format") {
    val inputPath = resourcePath("dvl1.200.gff3")
    val features = sc.loadGff3(inputPath)
    val outputPath = tempLocation(".gtf")
    features.saveAsGtf(outputPath)
    val reloadedFeatures = sc.loadGtf(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save GFF3 as BED format") {
    val inputPath = resourcePath("dvl1.200.gff3")
    val features = sc.loadGff3(inputPath)
    val outputPath = tempLocation(".bed")
    features.saveAsBed(outputPath)
    val reloadedFeatures = sc.loadBed(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save GFF3 as IntervalList format") {
    val inputPath = resourcePath("dvl1.200.gff3")
    val features = sc.loadGff3(inputPath)
    val outputPath = tempLocation(".interval_list")
    features.saveAsIntervalList(outputPath)
    val reloadedFeatures = sc.loadIntervalList(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save GFF3 as NarrowPeak format") {
    val inputPath = resourcePath("dvl1.200.gff3")
    val features = sc.loadGff3(inputPath)
    val outputPath = tempLocation(".narrowPeak")
    features.saveAsNarrowPeak(outputPath)
    val reloadedFeatures = sc.loadNarrowPeak(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("round trip GFF3 format") {
    val inputPath = resourcePath("dvl1.200.gff3")
    val expected = sc.loadGff3(inputPath)
    val outputPath = tempLocation(".gff3")
    expected.saveAsGff3(outputPath, asSingleFile = true)

    val feature = expected.rdd.first
    val gff3Columns = FeatureRDD.toGff3(feature).split('\t')
    assert(gff3Columns.size === 9)
    assert(gff3Columns(0) === "1")
    assert(gff3Columns(1) === "Ensembl")
    assert(gff3Columns(2) === "gene")
    assert(gff3Columns(3) === "1331314")
    assert(gff3Columns(4) === "1335306")
    assert(gff3Columns(5) === ".")
    assert(gff3Columns(6) === "+")
    assert(gff3Columns(7) === ".")
    val attrs = gff3Columns(8).split(';')
    assert(attrs.size === 3)
    assert(attrs(0) === "ID=ENSG00000169962")
    assert(attrs(1) === "Name=ENSG00000169962")
    assert(attrs(2) === "biotype=protein_coding")

    val actual = sc.loadGff3(outputPath)
    val pairs = expected.rdd.collect.zip(actual.rdd.collect)
    pairs.foreach(p => {
      assert(p._1 === p._2)
    })
  }

  sparkTest("save BED as GTF format") {
    val inputPath = resourcePath("dvl1.200.bed")
    val features = sc.loadBed(inputPath)
    val outputPath = tempLocation(".gtf")
    features.saveAsGtf(outputPath)
    val reloadedFeatures = sc.loadGtf(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save BED as GFF3 format") {
    val inputPath = resourcePath("dvl1.200.bed")
    val features = sc.loadBed(inputPath)
    val outputPath = tempLocation(".gff3")
    features.saveAsGff3(outputPath)
    val reloadedFeatures = sc.loadGff3(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save BED as IntervalList format") {
    val inputPath = resourcePath("dvl1.200.bed")
    val features = sc.loadBed(inputPath)
    val outputPath = tempLocation(".interval_list")
    features.saveAsIntervalList(outputPath)
    val reloadedFeatures = sc.loadIntervalList(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save BED as NarrowPeak format") {
    val inputPath = resourcePath("dvl1.200.bed")
    val features = sc.loadBed(inputPath)
    val outputPath = tempLocation(".narrowPeak")
    features.saveAsNarrowPeak(outputPath)
    val reloadedFeatures = sc.loadNarrowPeak(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("round trip BED format") {
    val inputPath = resourcePath("dvl1.200.bed")
    val expected = sc.loadBed(inputPath)
    val outputPath = tempLocation(".bed")
    expected.saveAsBed(outputPath, asSingleFile = true)

    val feature = expected.rdd.first
    val bedCols = FeatureRDD.toBed(feature).split('\t')
    assert(bedCols.size === 6)
    assert(bedCols(0) === "1")
    assert(bedCols(1) === "1331345")
    assert(bedCols(2) === "1331536")
    assert(bedCols(3) === "106624")
    assert(bedCols(4) === "13.53")
    assert(bedCols(5) === "+")

    val actual = sc.loadBed(outputPath)
    val pairs = expected.rdd.collect.zip(actual.rdd.collect)
    pairs.foreach(p => {
      assert(p._1 === p._2)
    })
  }

  sparkTest("save IntervalList as GTF format") {
    val inputPath = resourcePath("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    val outputPath = tempLocation(".gtf")
    features.saveAsGtf(outputPath)
    val reloadedFeatures = sc.loadGtf(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save IntervalList as GFF3 format") {
    val inputPath = resourcePath("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    val outputPath = tempLocation(".gff3")
    features.saveAsGff3(outputPath)
    val reloadedFeatures = sc.loadGff3(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save IntervalList as BED format") {
    val inputPath = resourcePath("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    val outputPath = tempLocation(".bed")
    features.saveAsBed(outputPath)
    val reloadedFeatures = sc.loadBed(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
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
    val reloadedFeatures = sc.loadNarrowPeak(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("round trip IntervalList format") {
    val inputPath = resourcePath("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val expected = sc.loadIntervalList(inputPath)

    // test single record
    val feature = expected.rdd.first
    val interval = FeatureRDD.toInterval(feature).split('\t')
    assert(interval.size === 5)
    assert(interval(0) === "chr1")
    assert(interval(1) === "14416")
    assert(interval(2) === "14499")
    assert(interval(3) === "+")
    assert(interval(4) === "gn|DDX11L1;gn|RP11-34P13.2;ens|ENSG00000223972;ens|ENSG00000227232;vega|OTTHUMG00000000958;vega|OTTHUMG00000000961")

    // test a record with a refseq attribute
    val refseqFeature = expected.rdd.filter(f => {
      f.getContigName == "chr7" &&
        f.getStart == 142111441L &&
        f.getEnd == 142111617L
    }).first
    val rsInterval = FeatureRDD.toInterval(refseqFeature).split('\t')
    assert(rsInterval.size === 5)
    assert(rsInterval(0) === "chr7")
    assert(rsInterval(1) === "142111442")
    assert(rsInterval(2) === "142111617")
    assert(rsInterval(3) === "+")
    assert(rsInterval(4) === "gn|TRBV5-7;ens|ENSG00000211731;refseq|NG_001333")

    val outputPath = tempLocation(".interval_list")
    expected.saveAsIntervalList(outputPath, asSingleFile = true)

    val actual = sc.loadIntervalList(outputPath)
    val pairs = expected.rdd.collect.zip(actual.rdd.collect)
    pairs.foreach(p => {
      assert(p._1 === p._2)
    })
  }

  sparkTest("save NarrowPeak as GTF format") {
    val inputPath = resourcePath("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    val outputPath = tempLocation(".gtf")
    features.saveAsGtf(outputPath)
    val reloadedFeatures = sc.loadGtf(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save NarrowPeak as GFF3 format") {
    val inputPath = resourcePath("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    val outputPath = tempLocation(".gff3")
    features.saveAsGff3(outputPath)
    val reloadedFeatures = sc.loadGff3(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save NarrowPeak as BED format") {
    val inputPath = resourcePath("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    val outputPath = tempLocation(".bed")
    features.saveAsBed(outputPath)
    val reloadedFeatures = sc.loadBed(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save NarrowPeak as IntervalList format") {
    val inputPath = resourcePath("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    val outputPath = tempLocation(".interval_list")
    features.saveAsIntervalList(outputPath)
    val reloadedFeatures = sc.loadIntervalList(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
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
    expected.saveAsNarrowPeak(outputPath, asSingleFile = true)

    val feature = expected.rdd.first
    val npColumns = FeatureRDD.toNarrowPeak(feature).split('\t')
    assert(npColumns.size === 10)
    assert(npColumns(0) === "chr1")
    assert(npColumns(1) === "713849")
    assert(npColumns(2) === "714434")
    assert(npColumns(3) === "chr1.1")
    assert(npColumns(4) === "1000")
    assert(npColumns(5) === ".")
    assert(npColumns(6) === "0.2252")
    assert(npColumns(7) === "9.16")
    assert(npColumns(8) === "-1")
    assert(npColumns(9) === "263")

    val actual = sc.loadNarrowPeak(outputPath)
    val pairs = expected.rdd.zip(actual.rdd).collect
    pairs.foreach(p => {
      assert(p._1 === p._2)
    })
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

  sparkTest("correctly flatmaps CoverageRDD from FeatureRDD") {
    val f1 = Feature.newBuilder().setContigName("chr1").setStart(1).setEnd(10).setScore(3.0).build()
    val f2 = Feature.newBuilder().setContigName("chr1").setStart(15).setEnd(20).setScore(2.0).build()
    val f3 = Feature.newBuilder().setContigName("chr2").setStart(15).setEnd(20).setScore(2.0).build()

    val featureRDD: FeatureRDD = FeatureRDD(sc.parallelize(Seq(f1, f2, f3)))
    val coverageRDD: CoverageRDD = featureRDD.toCoverage
    val coverage = coverageRDD.flatten

    assert(coverage.rdd.count == 19)
  }
}

