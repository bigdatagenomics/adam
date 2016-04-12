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

import java.io.File
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.Feature
import org.scalactic.{ Equivalence, TypeCheckedTripleEquals }

class FeatureRDDFunctionsSuite extends ADAMFunSuite with TypeCheckedTripleEquals {
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

  sparkTest("save GTF as GTF format") {
    val inputPath = resourcePath("features/Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)
    val outputPath = tempLocation(".gtf")
    features.saveAsGtf(outputPath)
  }

  sparkTest("save GTF as GFF3 format") {
    val inputPath = resourcePath("features/Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)
    val outputPath = tempLocation(".gff3")
    features.saveAsGff3(outputPath)
  }

  sparkTest("save GTF as BED format") {
    val inputPath = resourcePath("features/Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)
    val outputPath = tempLocation(".bed")
    features.saveAsBed(outputPath)
  }

  sparkTest("save GTF as IntervalList format") {
    val inputPath = resourcePath("features/Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)
    val outputPath = tempLocation(".interval_list")
    features.saveAsIntervalList(outputPath)
  }

  sparkTest("save GTF as NarrowPeak format") {
    val inputPath = resourcePath("features/Homo_sapiens.GRCh37.75.trun100.gtf")
    val features = sc.loadGtf(inputPath)
    val outputPath = tempLocation(".narrowPeak")
    features.saveAsNarrowPeak(outputPath)
  }

  sparkTest("round trip GTF format") {
    val inputPath = resourcePath("features/Homo_sapiens.GRCh37.75.trun100.gtf")
    val expected = sc.loadGtf(inputPath)
    val outputPath = tempLocation(".gtf")
    expected.saveAsGtf(outputPath)

    // grab all partitions, may not necessarily be in order; sort by reference
    val actual = sc.loadGtf(outputPath + "/part-*")
    val pairs = expected.coalesce(1).sortByReference().zip(actual.coalesce(1).sortByReference()).collect

    // separate foreach since assert is not serializable
    pairs.foreach({ pair: (Feature, Feature) => assert(pair._1 === pair._2) })
  }

  sparkTest("save GFF3 as GTF format") {
    val inputPath = resourcePath("features/dvl1.200.gff3")
    val features = sc.loadGff3(inputPath)
    val outputPath = tempLocation(".gtf")
    features.saveAsGtf(outputPath)
  }

  sparkTest("save GFF3 as BED format") {
    val inputPath = resourcePath("features/dvl1.200.gff3")
    val features = sc.loadGff3(inputPath)
    val outputPath = tempLocation(".bed")
    features.saveAsBed(outputPath)
  }

  sparkTest("save GFF3 as IntervalList format") {
    val inputPath = resourcePath("features/dvl1.200.gff3")
    val features = sc.loadGff3(inputPath)
    val outputPath = tempLocation(".interval_list")
    features.saveAsIntervalList(outputPath)
  }

  sparkTest("save GFF3 as NarrowPeak format") {
    val inputPath = resourcePath("features/dvl1.200.gff3")
    val features = sc.loadGff3(inputPath)
    val outputPath = tempLocation(".narrowPeak")
    features.saveAsNarrowPeak(outputPath)
  }

  sparkTest("round trip GFF3 format") {
    val inputPath = resourcePath("features/dvl1.200.gff3")
    val expected = sc.loadGff3(inputPath)
    val outputPath = tempLocation(".gff3")
    expected.saveAsGff3(outputPath)

    // grab all partitions, may not necessarily be in order; sort by reference
    val actual = sc.loadGff3(outputPath + "/part-*")
    val pairs = expected.coalesce(1).sortByReference().zip(actual.coalesce(1).sortByReference()).collect

    // separate foreach since assert is not serializable
    pairs.foreach({ pair: (Feature, Feature) => assert(pair._1 === pair._2) })
  }

  sparkTest("save BED as GTF format") {
    val inputPath = resourcePath("features/dvl1.200.bed")
    val features = sc.loadBed(inputPath)
    val outputPath = tempLocation(".gtf")
    features.saveAsGtf(outputPath)
  }

  sparkTest("save BED as GFF3 format") {
    val inputPath = resourcePath("features/dvl1.200.bed")
    val features = sc.loadBed(inputPath)
    val outputPath = tempLocation(".gff3")
    features.saveAsGff3(outputPath)
  }

  sparkTest("save BED as BED format") {
    val inputPath = resourcePath("features/dvl1.200.bed")
    val features = sc.loadBed(inputPath)
    val outputPath = tempLocation(".bed")
    features.saveAsBed(outputPath)
  }

  sparkTest("save BED as IntervalList format") {
    val inputPath = resourcePath("features/dvl1.200.bed")
    val features = sc.loadBed(inputPath)
    val outputPath = tempLocation(".interval_list")
    features.saveAsIntervalList(outputPath)
  }

  sparkTest("save BED as NarrowPeak format") {
    val inputPath = resourcePath("features/dvl1.200.bed")
    val features = sc.loadBed(inputPath)
    val outputPath = tempLocation(".narrowPeak")
    features.saveAsNarrowPeak(outputPath)
  }

  sparkTest("round trip BED format") {
    val inputPath = resourcePath("features/dvl1.200.bed")
    val expected = sc.loadBed(inputPath)
    val outputPath = tempLocation(".bed")
    expected.saveAsBed(outputPath)

    // grab all partitions, may not necessarily be in order; sort by reference
    val actual = sc.loadBed(outputPath + "/part-*")
    val pairs = expected.coalesce(1).sortByReference().zip(actual.coalesce(1).sortByReference()).collect

    // separate since assert is not serializable
    pairs.foreach({ pair: (Feature, Feature) => assert(pair._1 === pair._2) })
  }

  sparkTest("save IntervalList as GTF format") {
    val inputPath = resourcePath("features/SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    val outputPath = tempLocation(".gtf")
    features.saveAsGtf(outputPath)
  }

  sparkTest("save IntervalList as GFF3 format") {
    val inputPath = resourcePath("features/SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    val outputPath = tempLocation(".gff3")
    features.saveAsGff3(outputPath)
  }

  sparkTest("save IntervalList as BED format") {
    val inputPath = resourcePath("features/SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    val outputPath = tempLocation(".bed")
    features.saveAsBed(outputPath)
  }

  sparkTest("save IntervalList as IntervalList format") {
    val inputPath = resourcePath("features/SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    val outputPath = tempLocation(".interval_list")
    features.saveAsIntervalList(outputPath)
  }

  sparkTest("save IntervalList as NarrowPeak format") {
    val inputPath = resourcePath("features/SeqCap_EZ_Exome_v3.hg19.interval_list")
    val features = sc.loadIntervalList(inputPath)
    val outputPath = tempLocation(".narrowPeak")
    features.saveAsNarrowPeak(outputPath)
  }

  ignore("round trip IntervalList format") { // writing IntervalList headers is not yet supported
    val inputPath = resourcePath("features/SeqCap_EZ_Exome_v3.hg19.interval_list")
    val expected = sc.loadIntervalList(inputPath)
    val outputPath = tempLocation(".interval_list")
    expected.saveAsIntervalList(outputPath)

    // grab all partitions, may not necessarily be in order; sort by reference
    val actual = sc.loadIntervalList(outputPath + "/part-*")
    val pairs = expected.coalesce(1).sortByReference().zip(actual.coalesce(1).sortByReference()).collect

    // separate foreach since assert is not serializable
    pairs.foreach({ pair: (Feature, Feature) => assert(pair._1 === pair._2) })
  }

  sparkTest("save NarrowPeak as GTF format") {
    val inputPath = resourcePath("features/wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    val outputPath = tempLocation(".gtf")
    features.saveAsGtf(outputPath)
  }

  sparkTest("save NarrowPeak as GFF3 format") {
    val inputPath = resourcePath("features/wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    val outputPath = tempLocation(".gff3")
    features.saveAsGff3(outputPath)
  }

  sparkTest("save NarrowPeak as BED format") {
    val inputPath = resourcePath("features/wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    val outputPath = tempLocation(".bed")
    features.saveAsBed(outputPath)
  }

  sparkTest("save NarrowPeak as IntervalList format") {
    val inputPath = resourcePath("features/wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    val outputPath = tempLocation(".interval_list")
    features.saveAsIntervalList(outputPath)
  }

  sparkTest("save NarrowPeak as NarrowPeak format") {
    val inputPath = resourcePath("features/wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val features = sc.loadNarrowPeak(inputPath)
    val outputPath = tempLocation(".narrowPeak")
    features.saveAsNarrowPeak(outputPath)
  }

  sparkTest("round trip NarrowPeak format") {
    val inputPath = resourcePath("features/wgEncodeOpenChromDnaseGm19238Pk.trunc10.narrowPeak")
    val expected = sc.loadNarrowPeak(inputPath)
    val outputPath = tempLocation(".narrowPeak")
    expected.saveAsNarrowPeak(outputPath)

    // grab all partitions, may not necessarily be in order; sort by reference
    val actual = sc.loadNarrowPeak(outputPath + "/part-*")
    val pairs = expected.coalesce(1).sortByReference().zip(actual.coalesce(1).sortByReference()).collect

    // separate foreach since assert is not serializable
    pairs.foreach({ pair: (Feature, Feature) => assert(pair._1 === pair._2) })
  }
}
