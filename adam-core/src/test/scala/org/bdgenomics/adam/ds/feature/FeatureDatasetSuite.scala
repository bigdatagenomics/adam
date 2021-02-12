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
package org.bdgenomics.adam.ds.feature

import com.google.common.collect.ImmutableMap
import java.io.File
import org.apache.spark.api.java.function.{ Function => JFunction }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Dataset, SQLContext }
import org.bdgenomics.adam.models.{
  Coverage,
  SequenceRecord,
  VariantContext
}
import org.bdgenomics.adam.ds.ADAMContext._
import org.bdgenomics.adam.ds.fragment.FragmentDataset
import org.bdgenomics.adam.ds.read.AlignmentDataset
import org.bdgenomics.adam.ds.sequence.SliceDataset
import org.bdgenomics.adam.ds.variant.{
  GenotypeDataset,
  VariantDataset,
  VariantContextDataset
}
import org.bdgenomics.adam.sql.{
  Alignment => AlignmentProduct,
  Feature => FeatureProduct,
  Fragment => FragmentProduct,
  Genotype => GenotypeProduct,
  Slice => SliceProduct,
  Variant => VariantProduct,
  VariantContext => VariantContextProduct
}
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro._
import scala.io.Source

object FeatureDatasetSuite extends Serializable {

  def covFn(f: Feature): Coverage = {
    Coverage(f.getReferenceName,
      f.getStart,
      f.getEnd,
      1)
  }

  def fragFn(f: Feature): Fragment = {
    Fragment.newBuilder
      .setName(f.getReferenceName)
      .build
  }

  def genFn(f: Feature): Genotype = {
    Genotype.newBuilder
      .setReferenceName(f.getReferenceName)
      .setStart(f.getStart)
      .setEnd(f.getEnd)
      .build
  }

  def sliceFn(f: Feature): Slice = {
    Slice.newBuilder
      .setName(f.getReferenceName)
      .build
  }

  def readFn(f: Feature): Alignment = {
    Alignment.newBuilder
      .setReferenceName(f.getReferenceName)
      .setStart(f.getStart)
      .setEnd(f.getEnd)
      .build
  }

  def varFn(f: Feature): Variant = {
    Variant.newBuilder
      .setReferenceName(f.getReferenceName)
      .setStart(f.getStart)
      .setEnd(f.getEnd)
      .build
  }

  def vcFn(f: Feature): VariantContext = {
    VariantContext(Variant.newBuilder
      .setReferenceName(f.getReferenceName)
      .setStart(f.getStart)
      .setEnd(f.getEnd)
      .build)
  }
}

class FeatureDatasetSuite extends ADAMFunSuite {

  def tempLocation(suffix: String = ".adam"): String = {
    val tempFile = File.createTempFile("FeatureDatasetFunctionsSuite", "")
    val tempDir = tempFile.getParentFile
    new File(tempDir, tempFile.getName + suffix).getAbsolutePath
  }

  sparkTest("round trip GTF format") {
    val inputPath = testFile("Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)

    val firstGtfRecord = FeatureDataset.toGtf(features.rdd.first)

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
    val inputPath = testFile("Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)
    val outputPath = tempLocation(".gff3")
    features.saveAsGff3(outputPath)
    val reloadedFeatures = sc.loadGff3(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save GTF as BED format") {
    val inputPath = testFile("Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)
    val outputPath = tempLocation(".bed")
    features.saveAsBed(outputPath)
    val reloadedFeatures = sc.loadBed(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save GTF as IntervalList format") {
    val inputPath = testFile("Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)
    val outputPath = tempLocation(".interval_list")
    features.saveAsIntervalList(outputPath)
    val reloadedFeatures = sc.loadIntervalList(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save GTF as NarrowPeak format") {
    val inputPath = testFile("Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)
    val outputPath = tempLocation(".narrowPeak")
    features.saveAsNarrowPeak(outputPath)
    val reloadedFeatures = sc.loadNarrowPeak(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save GFF3 as GTF format") {
    val inputPath = testFile("dvl1.200.gff3")
    val features = sc.loadGff3(inputPath)
    val outputPath = tempLocation(".gtf")
    features.saveAsGtf(outputPath)
    val reloadedFeatures = sc.loadGtf(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save GFF3 as BED format") {
    val inputPath = testFile("dvl1.200.gff3")
    val features = sc.loadGff3(inputPath)
    val outputPath = tempLocation(".bed")
    features.saveAsBed(outputPath)
    val reloadedFeatures = sc.loadBed(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save GFF3 as IntervalList format") {
    val inputPath = testFile("dvl1.200.gff3")
    val features = sc.loadGff3(inputPath)
    val outputPath = tempLocation(".interval_list")
    features.saveAsIntervalList(outputPath)
    val reloadedFeatures = sc.loadIntervalList(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save GFF3 as NarrowPeak format") {
    val inputPath = testFile("dvl1.200.gff3")
    val features = sc.loadGff3(inputPath)
    val outputPath = tempLocation(".narrowPeak")
    features.saveAsNarrowPeak(outputPath)
    val reloadedFeatures = sc.loadNarrowPeak(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("round trip GFF3 format") {
    val inputPath = testFile("dvl1.200.gff3")
    val expected = sc.loadGff3(inputPath)
    val outputPath = tempLocation(".gff3")
    expected.saveAsGff3(outputPath, asSingleFile = true)

    val lines = Source.fromFile(outputPath)
      .getLines
      .toSeq
    assert(lines.size > 1)
    assert(lines.head === GFF3HeaderWriter.HEADER_STRING)

    val feature = expected.rdd.first
    val gff3Columns = FeatureDataset.toGff3(feature).split('\t')
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

  sparkTest("ignore FASTA sequence in GFF3 file") {
    val inputPath = testFile("ctg123.fasta.gff3")
    val features = sc.loadGff3(inputPath)
    assert(features.rdd.count() === 12)
  }

  sparkTest("save BED as GTF format") {
    val inputPath = testFile("dvl1.200.bed")
    val features = sc.loadBed(inputPath)
    val outputPath = tempLocation(".gtf")
    features.saveAsGtf(outputPath)
    val reloadedFeatures = sc.loadGtf(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save BED as GFF3 format") {
    val inputPath = testFile("dvl1.200.bed")
    val features = sc.loadBed(inputPath)
    val outputPath = tempLocation(".gff3")
    features.saveAsGff3(outputPath)
    val reloadedFeatures = sc.loadGff3(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save BED as IntervalList format") {
    val inputPath = testFile("dvl1.200.bed")
    val features = sc.loadBed(inputPath)
    val outputPath = tempLocation(".interval_list")
    features.saveAsIntervalList(outputPath)
    val reloadedFeatures = sc.loadIntervalList(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save BED as NarrowPeak format") {
    val inputPath = testFile("dvl1.200.bed")
    val features = sc.loadBed(inputPath)
    val outputPath = tempLocation(".narrowPeak")
    features.saveAsNarrowPeak(outputPath)
    val reloadedFeatures = sc.loadNarrowPeak(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("round trip BED6 format") {
    val inputPath = testFile("dvl1.200.bed")
    val expected = sc.loadBed(inputPath)
    val outputPath = tempLocation(".bed")
    expected.saveAsBed(outputPath, asSingleFile = true)

    val feature = expected.rdd.first
    val bedCols = FeatureDataset.toBed(feature).split('\t')
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

  sparkTest("keeps sample metadata") {
    val sample1 = Sample.newBuilder()
      .setName("Sample1")
      .build()

    val sample2 = Sample.newBuilder()
      .setName("Sample2")
      .build()

    val inputPath = testFile("dvl1.200.bed")
    var f1 = sc.loadBed(inputPath)
    var f2 = sc.loadBed(inputPath)

    f1 = f1.replaceSamples(Iterable(sample1))
    f2 = f2.replaceSamples(Iterable(sample2))

    val union = f1.union(f2)
    assert(union.samples.size === 2)
  }

  sparkTest("round trip BED12 format") {
    val inputPath = testFile("small.1_12.bed")
    val expected = sc.loadBed(inputPath)
    val outputPath = tempLocation(".bed")
    expected.saveAsBed(outputPath, asSingleFile = true)

    val feature = expected.rdd.first
    val bedCols = FeatureDataset.toBed(feature).split('\t')
    assert(bedCols.size === 12)
    assert(bedCols(0) === "1")
    assert(bedCols(1) === "143")
    assert(bedCols(2) === "26423")
    assert(bedCols(3) === "line1")
    assert(bedCols(4) === "0.0")
    assert(bedCols(5) === ".")
    assert(bedCols(6) === "150")
    assert(bedCols(7) === "26400")
    assert(bedCols(8) === "0,0,0")
    assert(bedCols(9) === ".")
    assert(bedCols(10) === ".")
    assert(bedCols(11) === ".")

    val actual = sc.loadBed(outputPath)
    val pairs = expected.rdd.collect.zip(actual.rdd.collect)
    pairs.foreach(p => {
      assert(p._1 === p._2)
    })

    checkFiles(inputPath, outputPath)
  }

  sparkTest("save to UCSC BED format") {
    val inputPath = testFile("dvl1.200.bed")
    val expected = sc.loadBed(inputPath)
    val outputPath = tempLocation(".bed")
    expected.saveAsUcscBed(outputPath,
      asSingleFile = true,
      minimumScore = 0.0,
      maximumScore = 200.0)

    val lines = sc.textFile(outputPath)
    val bedCols = lines.first.split("\t")
    assert(bedCols.size === 6)
    assert(bedCols(0) === "1")
    assert(bedCols(1) === "1331345")
    assert(bedCols(2) === "1331536")
    assert(bedCols(3) === "106624")
    assert(bedCols(4) === "67")
    assert(bedCols(5) === "+")
  }

  sparkTest("save IntervalList as GTF format") {
    val inputPath = testFile("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    val outputPath = tempLocation(".gtf")
    features.saveAsGtf(outputPath)
    val reloadedFeatures = sc.loadGtf(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save IntervalList as GFF3 format") {
    val inputPath = testFile("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    val outputPath = tempLocation(".gff3")
    features.saveAsGff3(outputPath)
    val reloadedFeatures = sc.loadGff3(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save IntervalList as BED format") {
    val inputPath = testFile("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    val outputPath = tempLocation(".bed")
    features.saveAsBed(outputPath)
    val reloadedFeatures = sc.loadBed(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save IntervalList as IntervalList format") {
    val inputPath = testFile("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    val outputPath = tempLocation(".interval_list")
    features.saveAsIntervalList(outputPath)
  }

  sparkTest("save IntervalList as NarrowPeak format") {
    val inputPath = testFile("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    val outputPath = tempLocation(".narrowPeak")
    features.saveAsNarrowPeak(outputPath)
    val reloadedFeatures = sc.loadNarrowPeak(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("round trip IntervalList format") {
    val inputPath = testFile("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val expected = sc.loadIntervalList(inputPath)

    // test single record
    val feature = expected.rdd.first
    val interval = FeatureDataset.toInterval(feature).split('\t')
    assert(interval.size === 5)
    assert(interval(0) === "chr1")
    assert(interval(1) === "14416")
    assert(interval(2) === "14499")
    assert(interval(3) === "+")
    assert(interval(4) === "gn|DDX11L1;gn|RP11-34P13.2;ens|ENSG00000223972;ens|ENSG00000227232;vega|OTTHUMG00000000958;vega|OTTHUMG00000000961")

    // test a record with a refseq attribute
    val refseqFeature = expected.rdd.filter(f => {
      f.getReferenceName == "chr7" &&
        f.getStart == 142111441L &&
        f.getEnd == 142111617L
    }).first
    val rsInterval = FeatureDataset.toInterval(refseqFeature).split('\t')
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
    val inputPath = testFile("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    val outputPath = tempLocation(".gtf")
    features.saveAsGtf(outputPath)
    val reloadedFeatures = sc.loadGtf(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save NarrowPeak as GFF3 format") {
    val inputPath = testFile("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    val outputPath = tempLocation(".gff3")
    features.saveAsGff3(outputPath)
    val reloadedFeatures = sc.loadGff3(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save NarrowPeak as BED format") {
    val inputPath = testFile("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    val outputPath = tempLocation(".bed")
    features.saveAsBed(outputPath)
    val reloadedFeatures = sc.loadBed(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save NarrowPeak as IntervalList format") {
    val inputPath = testFile("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    val outputPath = tempLocation(".interval_list")
    features.saveAsIntervalList(outputPath)
    val reloadedFeatures = sc.loadIntervalList(outputPath)
    assert(features.rdd.count === reloadedFeatures.rdd.count)
  }

  sparkTest("save NarrowPeak as NarrowPeak format") {
    val inputPath = testFile("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    val outputPath = tempLocation(".narrowPeak")
    features.saveAsNarrowPeak(outputPath)
  }

  sparkTest("round trip NarrowPeak format") {
    val inputPath = testFile("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val expected = sc.loadNarrowPeak(inputPath)
    val outputPath = tempLocation(".narrowPeak")
    expected.saveAsNarrowPeak(outputPath, asSingleFile = true)

    val feature = expected.rdd.first
    val npColumns = FeatureDataset.toNarrowPeak(feature).split('\t')
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
    val f1 = fb.setReferenceName("1").setStart(1L).setEnd(100L).build()
    val f2 = fb.setReferenceName("1").setStart(10L).setEnd(110L).setStrand(Strand.FORWARD).build()
    val f3 = fb.setReferenceName("1").setStart(10L).setEnd(110L).setStrand(Strand.REVERSE).build()
    val f4 = fb.setReferenceName("1").setStart(10L).setEnd(110L).setStrand(Strand.INDEPENDENT).build()
    val f5 = fb.setReferenceName("1").setStart(10L).setEnd(110L).setStrand(Strand.UNKNOWN).build()
    val f6 = fb.setReferenceName("1").setStart(10L).setEnd(110L).clearStrand().build() // null strand last
    val f7 = fb.setReferenceName("2").build()

    val features = FeatureDataset(sc.parallelize(Seq(f7, f6, f5, f4, f3, f2, f1)))
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
    val fb = Feature.newBuilder().setReferenceName("1").setStart(1L).setEnd(100L)
    val f1 = fb.setFeatureId("featureId").build()
    val f2 = fb.clearFeatureId().setName("name").build()
    val f3 = fb.clearName().setPhase(0).build()
    val f4 = fb.setPhase(1).build() // Int defaults to increasing sort order
    val f5 = fb.clearPhase().setScore(0.1).build()
    val f6 = fb.setScore(0.9).build() // Double defaults to increasing sort order
    val f7 = fb.clearScore().build() // nulls last

    val features = FeatureDataset(sc.parallelize(Seq(f7, f6, f5, f4, f3, f2, f1)))
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
    val fb = Feature.newBuilder().setReferenceName("1").setStart(1L).setEnd(100L).setFeatureType("gene")
    val f1 = fb.setGeneId("gene1").build()
    val f2 = fb.setGeneId("gene2").build()
    val f3 = fb.clearGeneId().build() // nulls last

    val features = FeatureDataset(sc.parallelize(Seq(f3, f2, f1)))
    val sorted = features.sortByReference().rdd.collect()

    assert(f1 == sorted(0))
    assert(f2 == sorted(1))
    assert(f3 == sorted(2))
  }

  sparkTest("sort transcript features by reference and gene structure") {
    val fb = Feature.newBuilder().setReferenceName("1").setStart(1L).setEnd(100L).setFeatureType("transcript")
    val f1 = fb.setGeneId("gene1").setTranscriptId("transcript1").build()
    val f2 = fb.setGeneId("gene1").setTranscriptId("transcript1").build()
    val f3 = fb.setGeneId("gene2").setTranscriptId("transcript1").build()
    val f4 = fb.setGeneId("gene2").setTranscriptId("transcript2").build()
    val f5 = fb.setGeneId("gene2").clearTranscriptId().build() // nulls last

    val features = FeatureDataset(sc.parallelize(Seq(f5, f4, f3, f2, f1)))
    val sorted = features.sortByReference().rdd.collect()

    assert(f1 == sorted(0))
    assert(f2 == sorted(1))
    assert(f3 == sorted(2))
    assert(f4 == sorted(3))
    assert(f5 == sorted(4))
  }

  sparkTest("sort exon features by reference and gene structure") {
    val fb = Feature.newBuilder().setReferenceName("1").setStart(1L).setEnd(100L).setFeatureType("exon")
    val f1 = fb.setGeneId("gene1").setTranscriptId("transcript1").setExonId("exon1").build()
    val f2 = fb.setGeneId("gene1").setTranscriptId("transcript1").setExonId("exon2").build()
    val f3 = fb.setGeneId("gene1").setTranscriptId("transcript2").setExonId("exon1").build()
    val f4 = fb.setGeneId("gene2").setTranscriptId("transcript1").setExonId("exon1").build()
    val f5 = fb.setGeneId("gene2").setTranscriptId("transcript1").clearExonId().setAttributes(ImmutableMap.of("exon_number", "1")).build()
    val f6 = fb.setGeneId("gene2").setTranscriptId("transcript1").setAttributes(ImmutableMap.of("exon_number", "2")).build()
    val f7 = fb.setGeneId("gene2").setTranscriptId("transcript1").setAttributes(ImmutableMap.of("rank", "1")).build()
    val f8 = fb.setGeneId("gene2").setTranscriptId("transcript1").setAttributes(ImmutableMap.of("rank", "2")).build()
    val f9 = fb.setGeneId("gene2").setTranscriptId("transcript1").clearAttributes().build() // nulls last

    val features = FeatureDataset(sc.parallelize(Seq(f9, f8, f7, f6, f5, f4, f3, f2, f1)))
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
    val fb = Feature.newBuilder().setReferenceName("1").setStart(1L).setEnd(100L).setGeneId("gene1").setTranscriptId("transcript1").setFeatureType("intron")
    val f1 = fb.setAttributes(ImmutableMap.of("intron_number", "1")).build()
    val f2 = fb.setAttributes(ImmutableMap.of("intron_number", "2")).build()
    val f3 = fb.setAttributes(ImmutableMap.of("rank", "1")).build()
    val f4 = fb.setAttributes(ImmutableMap.of("rank", "2")).build()
    val f5 = fb.clearAttributes().build() // nulls last

    val features = FeatureDataset(sc.parallelize(Seq(f5, f4, f3, f2, f1)))
    val sorted = features.sortByReference().rdd.collect()

    assert(f1 == sorted(0))
    assert(f2 == sorted(1))
    assert(f3 == sorted(2))
    assert(f4 == sorted(3))
    assert(f5 == sorted(4))
  }

  sparkTest("correctly flatmaps CoverageDataset from FeatureDataset") {
    val f1 = Feature.newBuilder().setReferenceName("chr1").setStart(1).setEnd(10).setScore(3.0).build()
    val f2 = Feature.newBuilder().setReferenceName("chr1").setStart(15).setEnd(20).setScore(2.0).build()
    val f3 = Feature.newBuilder().setReferenceName("chr2").setStart(15).setEnd(20).setScore(2.0).build()

    val featureDs: FeatureDataset = FeatureDataset(sc.parallelize(Seq(f1, f2, f3)))
    val coverageDs: CoverageDataset = featureDs.toCoverage
    val coverage = coverageDs.flatten

    assert(coverage.rdd.count == 19)
  }

  sparkTest("use broadcast join to pull down features mapped to targets") {
    val featuresPath = testFile("small.1.narrowPeak")
    val targetsPath = testFile("small.1.bed")

    val features = sc.loadFeatures(featuresPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = features.broadcastRegionJoin(targets)

    assert(jRdd.rdd.count === 5L)
  }

  sparkTest("use right outer broadcast join to pull down features mapped to targets") {
    val featuresPath = testFile("small.1.narrowPeak")
    val targetsPath = testFile("small.1.bed")

    val features = sc.loadFeatures(featuresPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = features.rightOuterBroadcastRegionJoin(targets)

    val c = jRdd.rdd.collect
    assert(c.count(_._1.isEmpty) === 1)
    assert(c.count(_._1.isDefined) === 5)
  }

  def sd = {
    sc.loadBam(testFile("small.1.sam"))
      .references
  }

  sparkTest("use shuffle join to pull down features mapped to targets") {
    val featuresPath = testFile("small.1.narrowPeak")
    val targetsPath = testFile("small.1.bed")

    val features = sc.loadFeatures(featuresPath)
      .transform((rdd: RDD[Feature]) => rdd.repartition(1))
      .replaceReferences(sd)
    val targets = sc.loadFeatures(targetsPath)
      .transform((rdd: RDD[Feature]) => rdd.repartition(1))

    val jRdd = features.shuffleRegionJoin(targets)
    val jRdd0 = features.shuffleRegionJoin(targets, optPartitions = Some(4), 0L)

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    assert(jRdd.rdd.count === 5L)
    assert(jRdd0.rdd.count === 5L)

    val joinedFeatures: FeatureDataset = jRdd
      .transmute[Feature, FeatureProduct, FeatureDataset]((rdd: RDD[(Feature, Feature)]) => {
        rdd.map(_._1)
      })
    val tempPath = tmpLocation(".adam")
    joinedFeatures.saveAsParquet(tempPath)
    assert(sc.loadFeatures(tempPath).rdd.count === 5)
  }

  sparkTest("use right outer shuffle join to pull down features mapped to targets") {
    val featuresPath = testFile("small.1.narrowPeak")
    val targetsPath = testFile("small.1.bed")

    val features = sc.loadFeatures(featuresPath)
      .transform((rdd: RDD[Feature]) => rdd.repartition(1))
      .replaceReferences(sd)
    val targets = sc.loadFeatures(targetsPath)
      .transform((rdd: RDD[Feature]) => rdd.repartition(1))

    val jRdd = features.rightOuterShuffleRegionJoin(targets)
    val jRdd0 = features.rightOuterShuffleRegionJoin(targets, optPartitions = Some(4), 0L)

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(_._1.isEmpty) === 1)
    assert(c0.count(_._1.isEmpty) === 1)
    assert(c.count(_._1.isDefined) === 5)
    assert(c0.count(_._1.isDefined) === 5)
  }

  sparkTest("use left outer shuffle join to pull down features mapped to targets") {
    val featuresPath = testFile("small.1.narrowPeak")
    val targetsPath = testFile("small.1.bed")

    val features = sc.loadFeatures(featuresPath)
      .transform((rdd: RDD[Feature]) => rdd.repartition(1))
      .replaceReferences(sd)
    val targets = sc.loadFeatures(targetsPath)
      .transform((rdd: RDD[Feature]) => rdd.repartition(1))

    val jRdd = features.leftOuterShuffleRegionJoin(targets)
    val jRdd0 = features.leftOuterShuffleRegionJoin(targets, optPartitions = Some(4), 0L)

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(_._2.isEmpty) === 15)
    assert(c0.count(_._2.isEmpty) === 15)
    assert(c.count(_._2.isDefined) === 5)
    assert(c0.count(_._2.isDefined) === 5)
  }

  sparkTest("use full outer shuffle join to pull down features mapped to targets") {
    val featuresPath = testFile("small.1.narrowPeak")
    val targetsPath = testFile("small.1.bed")

    val features = sc.loadFeatures(featuresPath)
      .transform((rdd: RDD[Feature]) => rdd.repartition(1))
      .replaceReferences(sd)
    val targets = sc.loadFeatures(targetsPath)
      .transform((rdd: RDD[Feature]) => rdd.repartition(1))

    val jRdd = features.fullOuterShuffleRegionJoin(targets)
    val jRdd0 = features.fullOuterShuffleRegionJoin(targets, optPartitions = Some(4), 0L)

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(t => t._1.isEmpty && t._2.isEmpty) === 0)
    assert(c0.count(t => t._1.isEmpty && t._2.isEmpty) === 0)
    assert(c.count(t => t._1.isDefined && t._2.isEmpty) === 15)
    assert(c0.count(t => t._1.isDefined && t._2.isEmpty) === 15)
    assert(c.count(t => t._1.isEmpty && t._2.isDefined) === 1)
    assert(c0.count(t => t._1.isEmpty && t._2.isDefined) === 1)
    assert(c.count(t => t._1.isDefined && t._2.isDefined) === 5)
    assert(c0.count(t => t._1.isDefined && t._2.isDefined) === 5)
  }

  sparkTest("use shuffle join with group by to pull down features mapped to targets") {
    val featuresPath = testFile("small.1.narrowPeak")
    val targetsPath = testFile("small.1.bed")

    val features = sc.loadFeatures(featuresPath)
      .transform((rdd: RDD[Feature]) => rdd.repartition(1))
      .replaceReferences(sd)
    val targets = sc.loadFeatures(targetsPath)
      .transform((rdd: RDD[Feature]) => rdd.repartition(1))

    val jRdd = features.shuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = features.shuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4), 0L)

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.size === 5)
    assert(c0.size === 5)
    assert(c.forall(_._2.size == 1))
    assert(c0.forall(_._2.size == 1))
  }

  sparkTest("use right outer shuffle join with group by to pull down features mapped to targets") {
    val featuresPath = testFile("small.1.narrowPeak")
    val targetsPath = testFile("small.1.bed")

    val features = sc.loadFeatures(featuresPath)
      .transform((rdd: RDD[Feature]) => rdd.repartition(1))
      .replaceReferences(sd)
    val targets = sc.loadFeatures(targetsPath)
      .transform((rdd: RDD[Feature]) => rdd.repartition(1))

    val jRdd = features.rightOuterShuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = features.rightOuterShuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4), 0L)

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect

    assert(c.count(_._1.isDefined) === 20)
    assert(c0.count(_._1.isDefined) === 20)
    assert(c.filter(_._1.isDefined).count(_._2.size == 1) === 5)
    assert(c0.filter(_._1.isDefined).count(_._2.size == 1) === 5)
    assert(c.filter(_._1.isDefined).count(_._2.isEmpty) === 15)
    assert(c0.filter(_._1.isDefined).count(_._2.isEmpty) === 15)
    assert(c.count(_._1.isEmpty) === 1)
    assert(c0.count(_._1.isEmpty) === 1)
    assert(c.filter(_._1.isEmpty).forall(_._2.size == 1))
    assert(c0.filter(_._1.isEmpty).forall(_._2.size == 1))
  }

  sparkTest("union two feature rdds together") {
    val features1 = sc.loadGtf(testFile("Homo_sapiens.GRCh37.75.trun100.gtf"))
    val features2 = sc.loadGff3(testFile("dvl1.200.gff3"))
    val union = features1.union(features2)
    assert(union.rdd.count === (features1.rdd.count + features2.rdd.count))
  }

  sparkTest("obtain sequence dictionary contig lengths from header in IntervalList format") {
    val inputPath = testFile("SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    /*
@SQ	SN:chr1	LN:249250621
@SQ	SN:chr2	LN:243199373
     */
    assert(features.references.containsReferenceName("chr1"))
    assert(features.references.apply("chr1").isDefined)
    assert(features.references.apply("chr1").get.length >= 249250621L)

    assert(features.references.containsReferenceName("chr2"))
    assert(features.references.apply("chr2").isDefined)
    assert(features.references.apply("chr2").get.length >= 243199373L)
  }

  sparkTest("don't lose any features when piping as BED format") {
    val inputPath = testFile("dvl1.200.bed")
    val frdd = sc.loadBed(inputPath)

    implicit val tFormatter = BEDInFormatter
    implicit val uFormatter = new BEDOutFormatter

    val pipedRdd: FeatureDataset = frdd.pipe[Feature, FeatureProduct, FeatureDataset, BEDInFormatter](Seq("tee", "/dev/null"))

    assert(pipedRdd.rdd.count >= frdd.rdd.count)
    assert(pipedRdd.rdd.distinct.count === frdd.rdd.distinct.count)
  }

  sparkTest("don't lose any features when piping as GTF format") {
    val inputPath = testFile("Homo_sapiens.GRCh37.75.trun100.gtf")
    val frdd = sc.loadGtf(inputPath)

    implicit val tFormatter = GTFInFormatter
    implicit val uFormatter = new GTFOutFormatter

    val pipedRdd: FeatureDataset = frdd.pipe[Feature, FeatureProduct, FeatureDataset, GTFInFormatter](Seq("tee", "/dev/null"))

    assert(pipedRdd.rdd.count >= frdd.rdd.count)
    assert(pipedRdd.rdd.distinct.count === frdd.rdd.distinct.count)
  }

  sparkTest("don't lose any features when piping as GFF3 format") {
    val inputPath = testFile("dvl1.200.gff3")
    val frdd = sc.loadGff3(inputPath)

    implicit val tFormatter = GFF3InFormatter
    implicit val uFormatter = new GFF3OutFormatter

    val pipedRdd: FeatureDataset = frdd.pipe[Feature, FeatureProduct, FeatureDataset, GFF3InFormatter](Seq("tee", "/dev/null"))

    assert(pipedRdd.rdd.count >= frdd.rdd.count)
    assert(pipedRdd.rdd.distinct.count === frdd.rdd.distinct.count)
  }

  sparkTest("don't lose any features when piping as NarrowPeak format") {
    val inputPath = testFile("wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val frdd = sc.loadNarrowPeak(inputPath)

    implicit val tFormatter = NarrowPeakInFormatter
    implicit val uFormatter = new NarrowPeakOutFormatter

    val pipedRdd: FeatureDataset = frdd.pipe[Feature, FeatureProduct, FeatureDataset, NarrowPeakInFormatter](Seq("tee", "/dev/null"))

    assert(pipedRdd.rdd.count >= frdd.rdd.count)
    assert(pipedRdd.rdd.distinct.count === frdd.rdd.distinct.count)
  }

  sparkTest("load parquet to sql, save, re-read from avro") {
    def testMetadata(fRdd: FeatureDataset) {
      val sequenceRdd = fRdd.addReference(SequenceRecord("aSequence", 1000L))
      val sampleRdd = fRdd.addSample(Sample.newBuilder().setName("Sample").build())
      assert(sequenceRdd.references.containsReferenceName("aSequence"))
      assert(sampleRdd.samples.map(r => r.getName).contains("Sample"))
    }

    val inputPath = testFile("small.1.bed")
    val outputPath = tmpLocation()
    val rrdd = sc.loadFeatures(inputPath)
    testMetadata(rrdd)
    val rdd = rrdd.transformDataset(ds => ds) // no-op but force to ds
    testMetadata(rdd)
    assert(rdd.dataset.count === 4)
    assert(rdd.rdd.count === 4)
    rdd.saveAsParquet(outputPath)
    val rdd2 = sc.loadFeatures(outputPath)
    testMetadata(rdd2)
    assert(rdd2.rdd.count === 4)
    assert(rdd2.dataset.count === 4)
    val outputPath2 = tmpLocation()
    rdd.transform((rdd: RDD[Feature]) => rdd) // no-op but force to rdd
      .saveAsParquet(outputPath2)
    val rdd3 = sc.loadFeatures(outputPath2)
    assert(rdd3.rdd.count === 4)
    assert(rdd3.dataset.count === 4)
  }

  sparkTest("load partitioned parquet to sql, save, re-read from avro") {
    def testMetadata(fRdd: FeatureDataset) {
      val sequenceRdd = fRdd.addReference(SequenceRecord("aSequence", 1000L))
      assert(sequenceRdd.references.containsReferenceName("aSequence"))
    }

    val inputPath = testFile("small.1.bed")
    val outputPath = tmpLocation()
    val rrdd = sc.loadFeatures(inputPath)
    testMetadata(rrdd)
    val rdd = rrdd.transformDataset(ds => ds) // no-op but force to ds
    testMetadata(rdd)
    rdd.saveAsPartitionedParquet(outputPath)
    val rdd2 = sc.loadPartitionedParquetFeatures(outputPath)
    testMetadata(rdd2)
    val outputPath2 = tmpLocation()
    rdd.transform((rdd: RDD[Feature]) => rdd) // no-op but force to rdd
      .saveAsPartitionedParquet(outputPath2)
    val rdd3 = sc.loadPartitionedParquetFeatures(outputPath2)
    assert(rdd3.rdd.count === 4)
    assert(rdd3.dataset.count === 4)
  }

  sparkTest("transform features to slice genomic dataset") {
    val features = sc.loadFeatures(testFile("sample_coverage.bed"))

    def checkSave(slices: SliceDataset) {
      val tempPath = tmpLocation(".adam")
      slices.saveAsParquet(tempPath)

      assert(sc.loadSlices(tempPath).rdd.count === 3)
    }

    val slices: SliceDataset = features.transmute[Slice, SliceProduct, SliceDataset](
      (rdd: RDD[Feature]) => {
        rdd.map(FeatureDatasetSuite.sliceFn)
      })

    checkSave(slices)

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val slicesDs: SliceDataset = features.transmuteDataset[Slice, SliceProduct, SliceDataset](
      (ds: Dataset[FeatureProduct]) => {
        ds.map(r => {
          SliceProduct.fromAvro(
            FeatureDatasetSuite.sliceFn(r.toAvro))
        })
      })

    checkSave(slicesDs)
  }

  sparkTest("transform features to coverage genomic dataset") {
    val features = sc.loadFeatures(testFile("sample_coverage.bed"))

    def checkSave(coverage: CoverageDataset) {
      val tempPath = tmpLocation(".bed")
      coverage.save(tempPath, false, false)

      assert(sc.loadCoverage(tempPath).rdd.count === 3)
    }

    val coverage: CoverageDataset = features.transmute[Coverage, Coverage, CoverageDataset](
      (rdd: RDD[Feature]) => {
        rdd.map(FeatureDatasetSuite.covFn)
      })

    checkSave(coverage)

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val coverageDs: CoverageDataset = features.transmuteDataset[Coverage, Coverage, CoverageDataset](
      (ds: Dataset[FeatureProduct]) => {
        ds.map(r => FeatureDatasetSuite.covFn(r.toAvro))
      })

    checkSave(coverageDs)
  }

  sparkTest("transform features to fragment genomic dataset") {
    val features = sc.loadFeatures(testFile("sample_coverage.bed"))

    def checkSave(fragments: FragmentDataset) {
      val tempPath = tmpLocation(".adam")
      fragments.saveAsParquet(tempPath)

      assert(sc.loadFragments(tempPath).rdd.count === 3)
    }

    val fragments: FragmentDataset = features.transmute[Fragment, FragmentProduct, FragmentDataset](
      (rdd: RDD[Feature]) => {
        rdd.map(FeatureDatasetSuite.fragFn)
      })

    checkSave(fragments)

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val fragmentsDs: FragmentDataset = features.transmuteDataset[Fragment, FragmentProduct, FragmentDataset](
      (ds: Dataset[FeatureProduct]) => {
        ds.map(r => {
          FragmentProduct.fromAvro(
            FeatureDatasetSuite.fragFn(r.toAvro))
        })
      })

    checkSave(fragmentsDs)
  }

  sparkTest("transform features to read genomic dataset") {
    val features = sc.loadFeatures(testFile("sample_coverage.bed"))

    def checkSave(reads: AlignmentDataset) {
      val tempPath = tmpLocation(".adam")
      reads.saveAsParquet(tempPath)

      assert(sc.loadAlignments(tempPath).rdd.count === 3)
    }

    val reads: AlignmentDataset = features.transmute[Alignment, AlignmentProduct, AlignmentDataset](
      (rdd: RDD[Feature]) => {
        rdd.map(FeatureDatasetSuite.readFn)
      })

    checkSave(reads)

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val readsDs: AlignmentDataset = features.transmuteDataset[Alignment, AlignmentProduct, AlignmentDataset](
      (ds: Dataset[FeatureProduct]) => {
        ds.map(r => {
          AlignmentProduct.fromAvro(
            FeatureDatasetSuite.readFn(r.toAvro))
        })
      })

    checkSave(readsDs)
  }

  sparkTest("transform features to genotype genomic dataset") {
    val features = sc.loadFeatures(testFile("sample_coverage.bed"))

    def checkSave(genotypes: GenotypeDataset) {
      val tempPath = tmpLocation(".adam")
      genotypes.saveAsParquet(tempPath)

      assert(sc.loadGenotypes(tempPath).rdd.count === 3)
    }

    val genotypes: GenotypeDataset = features.transmute[Genotype, GenotypeProduct, GenotypeDataset](
      (rdd: RDD[Feature]) => {
        rdd.map(FeatureDatasetSuite.genFn)
      })

    checkSave(genotypes)

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val genotypesDs: GenotypeDataset = features.transmuteDataset[Genotype, GenotypeProduct, GenotypeDataset](
      (ds: Dataset[FeatureProduct]) => {
        ds.map(r => {
          GenotypeProduct.fromAvro(
            FeatureDatasetSuite.genFn(r.toAvro))
        })
      })

    checkSave(genotypesDs)
  }

  sparkTest("transform features to variant genomic dataset") {
    val features = sc.loadFeatures(testFile("sample_coverage.bed"))

    def checkSave(variants: VariantDataset) {
      val tempPath = tmpLocation(".adam")
      variants.saveAsParquet(tempPath)

      assert(sc.loadVariants(tempPath).rdd.count === 3)
    }

    val variants: VariantDataset = features.transmute[Variant, VariantProduct, VariantDataset](
      (rdd: RDD[Feature]) => {
        rdd.map(FeatureDatasetSuite.varFn)
      })

    checkSave(variants)

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val variantsDs: VariantDataset = features.transmuteDataset[Variant, VariantProduct, VariantDataset](
      (ds: Dataset[FeatureProduct]) => {
        ds.map(r => {
          VariantProduct.fromAvro(
            FeatureDatasetSuite.varFn(r.toAvro))
        })
      })

    checkSave(variantsDs)
  }

  sparkTest("transform features to variant context genomic dataset") {
    val features = sc.loadFeatures(testFile("sample_coverage.bed"))

    def checkSave(variantContexts: VariantContextDataset) {
      assert(variantContexts.rdd.count === 3)
    }

    val variantContexts: VariantContextDataset = features.transmute[VariantContext, VariantContextProduct, VariantContextDataset](
      (rdd: RDD[Feature]) => {
        rdd.map(FeatureDatasetSuite.vcFn)
      })

    checkSave(variantContexts)
  }

  sparkTest("filter RDD bound features to reference name") {
    val features = sc.loadFeatures(testFile("dvl1.200.gff3"))
    assert(features.filterToReferenceName("1").rdd.count() === 195)
    assert(features.filterToReferenceName("2").rdd.count() === 0)
  }

  sparkTest("filter dataset bound features to reference name") {
    val features = sc.loadFeatures(testFile("dvl1.200.gff3"))
    val featuresDs = features.transformDataset(ds => ds)
    assert(featuresDs.filterToReferenceName("1").dataset.count() === 195)
    assert(featuresDs.filterToReferenceName("2").dataset.count() === 0)
  }

  sparkTest("filter RDD bound features by feature type") {
    val features = sc.loadFeatures(testFile("dvl1.200.gff3"))
    assert(features.filterToFeatureType("gene").rdd.count() === 22)
  }

  sparkTest("filter dataset bound features by feature type") {
    val features = sc.loadFeatures(testFile("dvl1.200.gff3"))
    val featuresDs = features.transformDataset(ds => ds)
    assert(featuresDs.filterToFeatureType("gene").dataset.count() === 22)
  }

  sparkTest("filter RDD bound features by feature types") {
    val features = sc.loadFeatures(testFile("dvl1.200.gff3"))
    assert(features.filterToFeatureTypes(Seq("gene", "transcript")).rdd.count() === 94)
  }

  sparkTest("filter dataset bound features by feature types") {
    val features = sc.loadFeatures(testFile("dvl1.200.gff3"))
    val featuresDs = features.transformDataset(ds => ds)
    assert(featuresDs.filterToFeatureTypes(Seq("gene", "transcript")).dataset.count() === 94)
  }

  sparkTest("filter RDD bound features by gene") {
    val fb = Feature.newBuilder()
    val f1 = fb.setReferenceName("1").setStart(1L).setEnd(101L).setGeneId("DVL1").build();
    val f2 = fb.setReferenceName("1").setStart(2L).setEnd(102L).setGeneId("CCDS22.1").build();
    val f3 = fb.setReferenceName("1").setStart(3L).setEnd(103L).setGeneId("CCDS22.1").build();
    val features = FeatureDataset(sc.parallelize(Seq(f1, f2, f3)))
    assert(features.filterToGene("CCDS22.1").rdd.count() === 2)
  }

  sparkTest("filter dataset bound features by gene") {
    val fb = Feature.newBuilder()
    val f1 = fb.setReferenceName("1").setStart(1L).setEnd(101L).setGeneId("DVL1").build();
    val f2 = fb.setReferenceName("1").setStart(2L).setEnd(102L).setGeneId("CCDS22.1").build();
    val f3 = fb.setReferenceName("1").setStart(3L).setEnd(103L).setGeneId("CCDS22.1").build();
    val features = FeatureDataset(sc.parallelize(Seq(f1, f2, f3)))
    val featuresDs = features.transformDataset(ds => ds)
    assert(features.filterToGene("CCDS22.1").rdd.count() === 2)
  }

  sparkTest("filter RDD bound features by genes") {
    val fb = Feature.newBuilder()
    val f1 = fb.setReferenceName("1").setStart(1L).setEnd(101L).setGeneId("DVL1").build();
    val f2 = fb.setReferenceName("1").setStart(2L).setEnd(102L).setGeneId("CCDS22.1").build();
    val f3 = fb.setReferenceName("1").setStart(3L).setEnd(103L).setGeneId("CCDS22.2").build();
    val features = FeatureDataset(sc.parallelize(Seq(f1, f2, f3)))
    assert(features.filterToGenes(Seq("CCDS22.1", "CCDS22.2")).rdd.count() === 2)
  }

  sparkTest("filter dataset bound features by genes") {
    val fb = Feature.newBuilder()
    val f1 = fb.setReferenceName("1").setStart(1L).setEnd(101L).setGeneId("DVL1").build();
    val f2 = fb.setReferenceName("1").setStart(2L).setEnd(102L).setGeneId("CCDS22.1").build();
    val f3 = fb.setReferenceName("1").setStart(3L).setEnd(103L).setGeneId("CCDS22.2").build();
    val features = FeatureDataset(sc.parallelize(Seq(f1, f2, f3)))
    val featuresDs = features.transformDataset(ds => ds)
    assert(features.filterToGenes(Seq("CCDS22.1", "CCDS22.2")).rdd.count() === 2)
  }

  sparkTest("filter RDD bound features by transcript") {
    val fb = Feature.newBuilder()
    val f1 = fb.setReferenceName("1").setStart(1L).setEnd(101L).setTranscriptId("ENST00000339381").build();
    val f2 = fb.setReferenceName("1").setStart(2L).setEnd(102L).setTranscriptId("ENST00000445648").build();
    val f3 = fb.setReferenceName("1").setStart(3L).setEnd(103L).setTranscriptId("ENST00000445648").build();
    val features = FeatureDataset(sc.parallelize(Seq(f1, f2, f3)))
    assert(features.filterToTranscript("ENST00000445648").rdd.count() === 2)
  }

  sparkTest("filter dataset bound features by transcript") {
    val fb = Feature.newBuilder()
    val f1 = fb.setReferenceName("1").setStart(1L).setEnd(101L).setTranscriptId("ENST00000339381").build();
    val f2 = fb.setReferenceName("1").setStart(2L).setEnd(102L).setTranscriptId("ENST00000445648").build();
    val f3 = fb.setReferenceName("1").setStart(3L).setEnd(103L).setTranscriptId("ENST00000445648").build();
    val features = FeatureDataset(sc.parallelize(Seq(f1, f2, f3)))
    val featuresDs = features.transformDataset(ds => ds)
    assert(features.filterToTranscript("ENST00000445648").rdd.count() === 2)
  }

  sparkTest("filter RDD bound features by transcripts") {
    val fb = Feature.newBuilder()
    val f1 = fb.setReferenceName("1").setStart(1L).setEnd(101L).setTranscriptId("ENST00000339381").build();
    val f2 = fb.setReferenceName("1").setStart(2L).setEnd(102L).setTranscriptId("ENST00000445648").build();
    val f3 = fb.setReferenceName("1").setStart(3L).setEnd(103L).setTranscriptId("ENST00000445649").build();
    val features = FeatureDataset(sc.parallelize(Seq(f1, f2, f3)))
    assert(features.filterToTranscripts(Seq("ENST00000445648", "ENST00000445649")).rdd.count() === 2)
  }

  sparkTest("filter dataset bound features by transcripts") {
    val fb = Feature.newBuilder()
    val f1 = fb.setReferenceName("1").setStart(1L).setEnd(101L).setTranscriptId("ENST00000339381").build();
    val f2 = fb.setReferenceName("1").setStart(2L).setEnd(102L).setTranscriptId("ENST00000445648").build();
    val f3 = fb.setReferenceName("1").setStart(3L).setEnd(103L).setTranscriptId("ENST00000445649").build();
    val features = FeatureDataset(sc.parallelize(Seq(f1, f2, f3)))
    val featuresDs = features.transformDataset(ds => ds)
    assert(features.filterToTranscripts(Seq("ENST00000445648", "ENST00000445649")).rdd.count() === 2)
  }

  sparkTest("filter RDD bound features by exon") {
    val fb = Feature.newBuilder()
    val f1 = fb.setReferenceName("1").setStart(1L).setEnd(101L).setExonId("ENSE00001691126").build();
    val f2 = fb.setReferenceName("1").setStart(2L).setEnd(102L).setExonId("ENSE00001779983").build();
    val f3 = fb.setReferenceName("1").setStart(3L).setEnd(103L).setExonId("ENSE00001779983").build();
    val features = FeatureDataset(sc.parallelize(Seq(f1, f2, f3)))
    assert(features.filterToExon("ENSE00001779983").rdd.count() === 2)
  }

  sparkTest("filter dataset bound features by exon") {
    val fb = Feature.newBuilder()
    val f1 = fb.setReferenceName("1").setStart(1L).setEnd(101L).setExonId("ENSE00001691126").build();
    val f2 = fb.setReferenceName("1").setStart(2L).setEnd(102L).setExonId("ENSE00001779983").build();
    val f3 = fb.setReferenceName("1").setStart(3L).setEnd(103L).setExonId("ENSE00001779983").build();
    val features = FeatureDataset(sc.parallelize(Seq(f1, f2, f3)))
    val featuresDs = features.transformDataset(ds => ds)
    assert(features.filterToExon("ENSE00001779983").rdd.count() === 2)
  }

  sparkTest("filter RDD bound features by exons") {
    val fb = Feature.newBuilder()
    val f1 = fb.setReferenceName("1").setStart(1L).setEnd(101L).setExonId("ENSE00001691126").build();
    val f2 = fb.setReferenceName("1").setStart(2L).setEnd(102L).setExonId("ENSE00001779983").build();
    val f3 = fb.setReferenceName("1").setStart(3L).setEnd(103L).setExonId("ENSE00001779984").build();
    val features = FeatureDataset(sc.parallelize(Seq(f1, f2, f3)))
    assert(features.filterToExons(Seq("ENSE00001779983", "ENSE00001779984")).rdd.count() === 2)
  }

  sparkTest("filter dataset bound features by exons") {
    val fb = Feature.newBuilder()
    val f1 = fb.setReferenceName("1").setStart(1L).setEnd(101L).setExonId("ENSE00001691126").build();
    val f2 = fb.setReferenceName("1").setStart(2L).setEnd(102L).setExonId("ENSE00001779983").build();
    val f3 = fb.setReferenceName("1").setStart(3L).setEnd(103L).setExonId("ENSE00001779984").build();
    val features = FeatureDataset(sc.parallelize(Seq(f1, f2, f3)))
    val featuresDs = features.transformDataset(ds => ds)
    assert(features.filterToExons(Seq("ENSE00001779983", "ENSE00001779984")).rdd.count() === 2)
  }

  sparkTest("filter RDD bound features by score") {
    val features = sc.loadFeatures(testFile("dvl1.200.bed"))
    assert(features.filterByScore(10.0d).rdd.count() === 23)
  }

  sparkTest("filter dataset bound features by score") {
    val features = sc.loadFeatures(testFile("dvl1.200.bed"))
    val featuresDs = features.transformDataset(ds => ds)
    assert(features.filterByScore(10.0d).rdd.count() === 23)
  }

  sparkTest("filter RDD bound features by parent") {
    val features = sc.loadFeatures(testFile("dvl1.200.gff3"))
    assert(features.filterToParent("ENSG00000107404").rdd.count() === 8)
  }

  sparkTest("filter dataset bound features by parent") {
    val features = sc.loadFeatures(testFile("dvl1.200.gff3"))
    val featuresDs = features.transformDataset(ds => ds)
    assert(features.filterToParent("ENSG00000107404").rdd.count() === 8)
  }

  sparkTest("filter RDD bound features by parents") {
    val features = sc.loadFeatures(testFile("dvl1.200.gff3"))
    assert(features.filterToParents(Seq("ENSG00000107404", "ENSG00000162576")).rdd.count() === 18)
  }

  sparkTest("filter dataset bound features by parents") {
    val features = sc.loadFeatures(testFile("dvl1.200.gff3"))
    val featuresDs = features.transformDataset(ds => ds)
    assert(features.filterToParents(Seq("ENSG00000107404", "ENSG00000162576")).rdd.count() === 18)
  }

  sparkTest("filter RDD bound features by attribute") {
    val features = sc.loadFeatures(testFile("dvl1.200.gff3"))
    assert(features.filterByAttribute("biotype", "protein_coding").rdd.count() === 68)
  }

  sparkTest("filter dataset bound features by attribute") {
    val features = sc.loadFeatures(testFile("dvl1.200.gff3"))
    val featuresDs = features.transformDataset(ds => ds)
    assert(features.filterByAttribute("biotype", "protein_coding").rdd.count() === 68)
  }

  sparkTest("transform dataset via java API") {
    val features = sc.loadFeatures(testFile("dvl1.200.gff3")).sortByReference()

    val transformed = features.transformDataset(new JFunction[Dataset[FeatureProduct], Dataset[FeatureProduct]]() {
      override def call(ds: Dataset[FeatureProduct]): Dataset[FeatureProduct] = {
        ds
      }
    })

    assert(features.dataset.first().start.get === transformed.dataset.first().start.get)
  }
}
