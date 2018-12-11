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
package org.bdgenomics.adam.converters

import com.google.common.collect.ImmutableList
import com.google.common.collect.ImmutableMap
import htsjdk.samtools.ValidationStringency
import htsjdk.variant.utils.SAMSequenceDictionaryExtractor
import htsjdk.variant.variantcontext.{
  Allele,
  Genotype => HtsjdkGenotype,
  GenotypeBuilder,
  GenotypeType,
  VariantContext => HtsjdkVariantContext,
  VariantContextBuilder
}
import htsjdk.variant.vcf.{
  VCFConstants,
  VCFFormatHeaderLine,
  VCFHeaderLineCount,
  VCFHeaderLineType,
  VCFInfoHeaderLine,
  VCFStandardHeaderLines
}
import java.io.File
import org.bdgenomics.adam.models.{
  SequenceDictionary,
  VariantContext => ADAMVariantContext
}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.{ ADAMFunSuite, PhredUtils }
import org.bdgenomics.formats.avro._
import scala.collection.JavaConversions._

class VariantContextConverterSuite extends ADAMFunSuite {
  val dictionary = {
    val path = testFile("dict_with_accession.dict")
    SequenceDictionary(SAMSequenceDictionaryExtractor.extractDictionary(new File(path)))
  }

  val lenient = ValidationStringency.LENIENT
  val converter = new VariantContextConverter(DefaultHeaderLines.allHeaderLines,
    lenient, false)
  val strictConverter = new VariantContextConverter(DefaultHeaderLines.allHeaderLines,
    ValidationStringency.STRICT, false)

  def htsjdkSNVBuilder: VariantContextBuilder = new VariantContextBuilder()
    .alleles(List(Allele.create("A", true), Allele.create("T")))
    .start(1L)
    .stop(1L)
    .chr("1")

  def htsjdkMultiAllelicSNVBuilder: VariantContextBuilder = new VariantContextBuilder()
    .alleles(List(Allele.create("A", true), Allele.create("T"), Allele.create("G")))
    .start(1L)
    .stop(1L)
    .chr("1")

  def htsjdkRefSNV: VariantContextBuilder = new VariantContextBuilder()
    .alleles(List(Allele.create("A", true), Allele.create("<NON_REF>", false)))
    .start(1L)
    .stop(1L)
    .chr("1")

  def htsjdkCNVBuilder: VariantContextBuilder = new VariantContextBuilder()
    .alleles(List(Allele.create("A", true), Allele.create("<CN0>", false)))
    .start(10L)
    .stop(20L)
    .chr("1")

  def adamSNVBuilder(contig: String = "1"): Variant.Builder = Variant.newBuilder()
    .setReferenceName(contig)
    .setStart(0L)
    .setEnd(1L)
    .setReferenceAllele("A")
    .setAlternateAllele("T")

  test("Convert htsjdk site-only SNV to ADAM") {
    val adamVCs = converter.convert(htsjdkSNVBuilder.make)
    assert(adamVCs.length === 1)
    val adamVC = adamVCs.head

    assert(adamVC.genotypes.size === 0)

    val variant = adamVC.variant.variant
    assert(variant.getReferenceName === "1")

    assert(variant.getReferenceAllele === "A")
    assert(variant.getStart === 0L)
  }

  test("Convert somatic htsjdk site-only SNV to ADAM") {
    val vcb: VariantContextBuilder = new VariantContextBuilder()
      .alleles(List(Allele.create("A", true), Allele.create("T")))
      .start(1L)
      .stop(1L)
      .chr("1")
      .attribute("SOMATIC", true)

    val adamVCs = converter.convert(vcb.make)
    val adamVC = adamVCs.head
    val variant = adamVC.variant.variant
    assert(variant.getAnnotation.getSomatic === true)
  }

  test("Convert htsjdk site-only CNV to ADAM") {
    val adamVCs = converter.convert(htsjdkCNVBuilder.make)

    assert(adamVCs.length === 1)
    val adamVC = adamVCs.head

    assert(adamVC.genotypes.size === 0)

    val variant = adamVC.variant.variant
    assert(variant.getReferenceName === "1")

    assert(variant.getReferenceAllele === "A")
    assert(variant.getAlternateAllele === "<CN0>")
    assert(variant.getStart === 9L)
    assert(variant.getEnd === 20L)
  }

  test("Convert htsjdk SNV w/ genotypes w/ phase information to ADAM") {
    val vcb = htsjdkSNVBuilder

    val genotypeAttributes = Map[String, Object]("PQ" -> new Integer(50), "PS" -> new Integer(1))
    val vc = vcb.genotypes(new GenotypeBuilder(
      GenotypeBuilder.create("NA12878", vcb.getAlleles(), genotypeAttributes))
      .phased(true)
      .make)
      .make()

    val adamVCs = converter.convert(vc)
    assert(adamVCs.length === 1)

    val adamGTs = adamVCs.flatMap(_.genotypes)
    assert(adamGTs.length === 1)
    val adamGT = adamGTs.head
    assert(adamGT.getAlleles.sameElements(List(GenotypeAllele.REF, GenotypeAllele.ALT)))
    assert(adamGT.getPhaseSetId === 1)
    assert(adamGT.getPhaseQuality === 50)
  }

  test("Convert htsjdk SNV with different variant filters to ADAM") {
    val vcb = htsjdkSNVBuilder
    vcb.genotypes(GenotypeBuilder.create("NA12878", vcb.getAlleles))

    { // No filters
      val adamVCs = converter.convert(vcb.make)
      val adamVariant = adamVCs.map(_.variant).head
      assert(adamVariant.variant.getFiltersApplied === false)
      assert(adamVariant.variant.getFiltersPassed === null)
      assert(adamVariant.variant.getFiltersFailed.isEmpty)
    }
    { // PASSing
      vcb.unfiltered.passFilters
      val adamVCs = converter.convert(vcb.make)
      val adamVariant = adamVCs.map(_.variant).head
      assert(adamVariant.variant.getFiltersApplied === true)
      assert(adamVariant.variant.getFiltersPassed === true)
      assert(adamVariant.variant.getFiltersFailed.isEmpty)
    }
    { // not PASSing
      vcb.unfiltered.filter("LowMQ")
      val adamVCs = converter.convert(vcb.make)
      val adamVariant = adamVCs.map(_.variant).head
      assert(adamVariant.variant.getFiltersApplied === true)
      assert(adamVariant.variant.getFiltersPassed === false)
      assert(adamVariant.variant.getFiltersFailed.sameElements(List("LowMQ")))
    }
  }

  test("Convert htsjdk SNV with different genotype filters to ADAM") {
    val vcb = htsjdkSNVBuilder
    val gb = new GenotypeBuilder("NA12878", vcb.getAlleles)

    { // No filters
      gb.unfiltered
      vcb.genotypes(gb.make)
      val adamVCs = converter.convert(vcb.make)
      val adamGT = adamVCs.flatMap(_.genotypes).head
      // htsjdk does not distinguish between filters not applied and filters passed in Genotype
      assert(adamGT.getVariantCallingAnnotations.getFiltersApplied === true)
      assert(adamGT.getVariantCallingAnnotations.getFiltersPassed === true)
      assert(adamGT.getVariantCallingAnnotations.getFiltersFailed.isEmpty)
    }
    { // PASSing
      gb.filter("PASS")
      vcb.genotypes(gb.make)
      val adamVCs = converter.convert(vcb.make)
      val adamGT = adamVCs.flatMap(_.genotypes).head
      assert(adamGT.getVariantCallingAnnotations.getFiltersApplied === true)
      assert(adamGT.getVariantCallingAnnotations.getFiltersPassed === true)
      assert(adamGT.getVariantCallingAnnotations.getFiltersFailed.isEmpty)
    }
    { // not PASSing
      gb.filter("LowMQ")
      vcb.genotypes(gb.make)

      val adamVCs = converter.convert(vcb.make)
      val adamGT = adamVCs.flatMap(_.genotypes).head
      assert(adamGT.getVariantCallingAnnotations.getFiltersApplied === true)
      assert(adamGT.getVariantCallingAnnotations.getFiltersPassed === false)
      assert(adamGT.getVariantCallingAnnotations.getFiltersFailed.sameElements(List("LowMQ")))
    }
  }

  test("Convert ADAM site-only SNV to htsjdk") {
    val vc = ADAMVariantContext(adamSNVBuilder().build)

    val optHtsjdkVC = converter.convert(vc)

    assert(optHtsjdkVC.isDefined)
    val htsjdkVC = optHtsjdkVC.get
    assert(htsjdkVC.getContig === "1")
    assert(htsjdkVC.getStart === 1)
    assert(htsjdkVC.getEnd === 1)
    assert(htsjdkVC.getReference === Allele.create("A", true))
    assert(htsjdkVC.getAlternateAlleles.sameElements(List(Allele.create("T"))))
    assert(!htsjdkVC.hasLog10PError)
    assert(!htsjdkVC.hasID)
    assert(!htsjdkVC.filtersWereApplied)
  }

  test("Convert ADAM SNV w/ genotypes to htsjdk") {
    val variant = adamSNVBuilder().build
    val genotype = Genotype.newBuilder
      .setVariant(variant)
      .setSampleId("NA12878")
      .setStrandBiasComponents(List(0, 2, 4, 6).map(i => i: java.lang.Integer))
      .setAlleles(List(GenotypeAllele.REF, GenotypeAllele.ALT))
      .setVariantCallingAnnotations(VariantCallingAnnotations.newBuilder()
        .setFisherStrandBiasPValue(3.0f)
        .setRmsMapQ(0.0f)
        .setMapq0Reads(5)
        .build)
      .build

    val optHtsjdkVC = converter.convert(ADAMVariantContext(variant, Seq(genotype)))

    assert(optHtsjdkVC.isDefined)
    val htsjdkVC = optHtsjdkVC.get
    assert(htsjdkVC.getNSamples === 1)
    assert(htsjdkVC.hasGenotype("NA12878"))
    val htsjdkGT = htsjdkVC.getGenotype("NA12878")
    assert(htsjdkGT.getType === GenotypeType.HET)
    assert(htsjdkGT.hasAnyAttribute("FS"))
    assert(htsjdkGT.hasAnyAttribute("MQ"))
    assert(htsjdkGT.hasAnyAttribute("MQ0"))
    assert(htsjdkGT.hasAnyAttribute("SB"))
    val sbComponents = htsjdkGT.getAnyAttribute("SB")
      .asInstanceOf[Array[Int]]
    assert(sbComponents(0) === 0)
    assert(sbComponents(1) === 2)
    assert(sbComponents(2) === 4)
    assert(sbComponents(3) === 6)
  }

  test("Convert ADAM SNV w/ genotypes but bad SB to htsjdk with strict validation") {
    val variant = adamSNVBuilder().build
    val genotype = Genotype.newBuilder
      .setVariant(variant)
      .setSampleId("NA12878")
      .setStrandBiasComponents(List(0, 2).map(i => i: java.lang.Integer))
      .setAlleles(List(GenotypeAllele.REF, GenotypeAllele.ALT))
      .setVariantCallingAnnotations(VariantCallingAnnotations.newBuilder()
        .setFisherStrandBiasPValue(3.0f)
        .setRmsMapQ(0.0f)
        .setMapq0Reads(5)
        .build)
      .build

    intercept[IllegalArgumentException] {
      strictConverter.convert(ADAMVariantContext(variant, Seq(genotype)))
    }
  }

  test("Convert ADAM SNV w/ genotypes but bad SB to htsjdk with lenient validation") {
    val variant = adamSNVBuilder().build
    val genotype = Genotype.newBuilder
      .setVariant(variant)
      .setSampleId("NA12878")
      .setStrandBiasComponents(List(0, 2).map(i => i: java.lang.Integer))
      .setAlleles(List(GenotypeAllele.REF, GenotypeAllele.ALT))
      .setVariantCallingAnnotations(VariantCallingAnnotations.newBuilder()
        .setFisherStrandBiasPValue(3.0f)
        .setRmsMapQ(0.0f)
        .setMapq0Reads(5)
        .build)
      .build

    val optHtsjdkVC = converter.convert(ADAMVariantContext(variant, Seq(genotype)))

    assert(optHtsjdkVC.isDefined)
    optHtsjdkVC.foreach(vc => {
      assert(!vc.getGenotype("NA12878").hasAnyAttribute("SB"))
    })
  }

  test("Convert htsjdk multi-allelic sites-only SNVs to ADAM") {
    val vc = htsjdkMultiAllelicSNVBuilder.make
    val adamVCs = converter.convert(vc)
    assert(adamVCs.length === 2)

    for ((allele, idx) <- vc.getAlternateAlleles.zipWithIndex) {
      val adamVC = adamVCs(idx)
      assert(adamVC.variant.variant.getReferenceAllele === vc.getReference.getBaseString)
      assert(adamVC.variant.variant.getAlternateAllele === allele.getBaseString)
    }
  }

  test("Convert htsjdk multi-allelic SNVs to ADAM and back to htsjdk") {
    val gb = new GenotypeBuilder("NA12878", List(Allele.create("T"), Allele.create("G")))
    gb.AD(Array(4, 2, 3)).PL(Array(59, 0, 181, 1, 66, 102))

    val vcb = htsjdkMultiAllelicSNVBuilder
    vcb.genotypes(gb.make)

    val adamVCs = converter.convert(vcb.make)
    assert(adamVCs.length === 2)

    for (adamVC <- adamVCs) {
      assert(adamVC.genotypes.size === 1)
      assert(adamVC.variant.variant.getSplitFromMultiAllelic)
      val adamGT = adamVC.genotypes.head
      assert(adamGT.getSplitFromMultiAllelic)
      assert(adamGT.getReferenceReadDepth === 4)
    }

    val adamGT1 = adamVCs(0).genotypes.head
    val adamGT2 = adamVCs(1).genotypes.head
    assert(adamGT1.getAlleles.sameElements(List(GenotypeAllele.ALT, GenotypeAllele.OTHER_ALT)))
    assert(adamGT1.getAlternateReadDepth === 2)
    assert(adamGT1.getGenotypeLikelihoods
      .map(d => d: scala.Double)
      .map(PhredUtils.logProbabilityToPhred)
      .sameElements(List(59, 0, 181)))

    assert(adamGT2.getAlleles.sameElements(List(GenotypeAllele.OTHER_ALT, GenotypeAllele.ALT)))
    assert(adamGT2.getAlternateReadDepth === 3)
    assert(adamGT2.getGenotypeLikelihoods
      .map(d => d: scala.Double)
      .map(PhredUtils.logProbabilityToPhred)
      .sameElements(List(59, 1, 102)))

    val optHtsjdkVC1 = converter.convert(adamVCs(0))
    val optHtsjdkVC2 = converter.convert(adamVCs(1))
    assert(optHtsjdkVC1.isDefined)
    assert(optHtsjdkVC2.isDefined)
    val htsjdkVC1 = optHtsjdkVC1.get
    val htsjdkVC2 = optHtsjdkVC2.get
    assert(htsjdkVC1.hasGenotype("NA12878"))
    assert(htsjdkVC2.hasGenotype("NA12878"))
    val gt1 = htsjdkVC1.getGenotype("NA12878")
    val gt2 = htsjdkVC2.getGenotype("NA12878")
    assert(gt1.getAllele(0).isNonReference)
    assert(gt1.getAllele(0).getBaseString === "T")
    assert(gt1.getAllele(1).isNoCall)
    assert(gt2.getAllele(0).isNoCall)
    assert(gt2.getAllele(1).isNonReference)
    assert(gt2.getAllele(1).getBaseString === "G")
  }

  test("Convert gVCF reference records to ADAM") {
    val gb = new GenotypeBuilder("NA12878", List(Allele.create("A", true), Allele.create("A", true)))
    gb.PL(Array(0, 1, 2)).DP(44).attribute("MIN_DP", 38)

    val vcb = htsjdkRefSNV
    vcb.genotypes(gb.make)

    val adamVCs = converter.convert(vcb.make)
    assert(adamVCs.length == 1)

    val adamGTs = adamVCs.flatMap(_.genotypes)
    assert(adamGTs.length === 1)
    val adamGT = adamGTs.head
    assert(adamGT.getVariant.getAlternateAllele === null)
    assert(adamGT.getAlleles.sameElements(List(GenotypeAllele.REF, GenotypeAllele.REF)))
    assert(adamGT.getMinReadDepth === 38)
    assert(adamGT.getGenotypeLikelihoods.isEmpty)
    assert(adamGT.getNonReferenceLikelihoods
      .map(d => d: scala.Double)
      .map(PhredUtils.logProbabilityToPhred)
      .sameElements(List(0, 1, 2)))
  }

  test("Convert htsjdk variant context with no IDs to ADAM") {
    val vcb = htsjdkSNVBuilder
    vcb.noID()

    val adamVCs = converter.convert(vcb.make)
    assert(adamVCs.length == 1)

    val variant = adamVCs.head.variant
    assert(variant.variant.getNames.isEmpty)
  }

  test("Convert htsjdk variant context with one ID to ADAM") {
    val vcb = htsjdkSNVBuilder
    vcb.id("rs3131972")

    val adamVCs = converter.convert(vcb.make)
    assert(adamVCs.length == 1)

    val variant = adamVCs.head.variant
    assert(variant.variant.getNames.length === 1)
    assert(variant.variant.getNames.get(0) === "rs3131972")
  }

  test("Convert htsjdk variant context with multiple IDs to ADAM") {
    val vcb = htsjdkSNVBuilder
    vcb.id("rs3131972;rs201888535")

    val adamVCs = converter.convert(vcb.make)
    assert(adamVCs.length == 1)

    val variant = adamVCs.head.variant
    assert(variant.variant.getNames.length === 2)
    assert(variant.variant.getNames.get(0) === "rs3131972")
    assert(variant.variant.getNames.get(1) === "rs201888535")
  }

  test("Convert ADAM variant context with no names to htsjdk") {
    val variant = adamSNVBuilder()
      .build

    assert(variant.getNames.isEmpty)

    val optHtsjdkVC = converter.convert(ADAMVariantContext(variant))

    assert(optHtsjdkVC.isDefined)
    val htsjdkVC = optHtsjdkVC.get
    assert(!htsjdkVC.hasID)
  }

  test("Convert ADAM variant context with one name to htsjdk") {
    val variant = adamSNVBuilder()
      .setNames(ImmutableList.of("rs3131972"))
      .build

    val optHtsjdkVC = converter.convert(ADAMVariantContext(variant))

    assert(optHtsjdkVC.isDefined)
    val htsjdkVC = optHtsjdkVC.get
    assert(htsjdkVC.hasID)
    assert(htsjdkVC.getID === "rs3131972")
  }

  test("Convert ADAM variant context with multiple names to htsjdk") {
    val variant = adamSNVBuilder()
      .setNames(ImmutableList.of("rs3131972", "rs201888535"))
      .build

    val optHtsjdkVC = converter.convert(ADAMVariantContext(variant))

    assert(optHtsjdkVC.isDefined)
    val htsjdkVC = optHtsjdkVC.get
    assert(htsjdkVC.hasID)
    assert(htsjdkVC.getID === "rs3131972;rs201888535")
  }

  test("Convert ADAM variant context with null filters applied to htsjdk") {
    val variant = adamSNVBuilder()
      .setFiltersApplied(null)
      .build

    val optHtsjdkVC = converter.convert(ADAMVariantContext(variant))

    assert(optHtsjdkVC.isDefined)
    val htsjdkVC = optHtsjdkVC.get
    assert(!htsjdkVC.filtersWereApplied)
    assert(!htsjdkVC.isFiltered)
    assert(htsjdkVC.getFilters.isEmpty)
  }

  test("Convert ADAM variant context with no filters applied to htsjdk") {
    val variant = adamSNVBuilder()
      .setFiltersApplied(false)
      .build

    val optHtsjdkVC = converter.convert(ADAMVariantContext(variant))

    assert(optHtsjdkVC.isDefined)
    val htsjdkVC = optHtsjdkVC.get
    assert(!htsjdkVC.filtersWereApplied)
    assert(!htsjdkVC.isFiltered)
    assert(htsjdkVC.getFilters.isEmpty)
  }

  test("Convert ADAM variant context with passing filters to htsjdk") {
    val variant = adamSNVBuilder()
      .setFiltersApplied(true)
      .setFiltersPassed(true)
      .build

    val optHtsjdkVC = converter.convert(ADAMVariantContext(variant))

    assert(optHtsjdkVC.isDefined)
    val htsjdkVC = optHtsjdkVC.get
    assert(htsjdkVC.filtersWereApplied)
    assert(!htsjdkVC.isFiltered)
    assert(htsjdkVC.getFilters.isEmpty)
  }

  test("Convert ADAM variant context with failing filters to htsjdk") {
    val variant = adamSNVBuilder()
      .setFiltersApplied(true)
      .setFiltersPassed(false)
      .setFiltersFailed(ImmutableList.of("FILTER1", "FILTER2"))
      .build

    val optHtsjdkVC = converter.convert(ADAMVariantContext(variant))

    assert(optHtsjdkVC.isDefined)
    val htsjdkVC = optHtsjdkVC.get
    assert(htsjdkVC.filtersWereApplied)
    assert(htsjdkVC.isFiltered)
    assert(htsjdkVC.getFilters.size === 2)
    assert(htsjdkVC.getFilters.contains("FILTER1"))
    assert(htsjdkVC.getFilters.contains("FILTER2"))
  }

  def makeGenotype(genotypeAttributes: Map[String, java.lang.Object],
                   fns: Iterable[GenotypeBuilder => GenotypeBuilder]): HtsjdkGenotype = {
    val vcb = htsjdkSNVBuilder
    val gb = fns.foldLeft(new GenotypeBuilder(GenotypeBuilder.create("NA12878",
      vcb.getAlleles(),
      genotypeAttributes)))((bldr, fn) => {
      fn(bldr)
    })
    val vc = vcb.genotypes(gb.make).make()

    vc.getGenotype("NA12878")
  }

  def buildGt(
    objMap: Map[String, java.lang.Object],
    extractor: (HtsjdkGenotype, Genotype.Builder, Int, Array[Int]) => Genotype.Builder,
    fns: Iterable[GenotypeBuilder => GenotypeBuilder] = Iterable.empty): Genotype = {
    val htsjdkGenotype = makeGenotype(objMap, fns)
    extractor(htsjdkGenotype,
      Genotype.newBuilder,
      1,
      Array(0, 1, 2)).build
  }

  test("no phasing set going htsjdk->adam") {
    val gt = buildGt(Map.empty,
      converter.formatPhaseInfo,
      fns = Iterable((gb: GenotypeBuilder) => {
        gb.phased(false)
      }))
    assert(!gt.getPhased)
    assert(gt.getPhaseSetId === null)
    assert(gt.getPhaseQuality === null)
  }

  test("phased but no phase set info going htsjdk->adam") {
    val gt = buildGt(Map.empty,
      converter.formatPhaseInfo,
      fns = Iterable((gb: GenotypeBuilder) => {
        gb.phased(true)
      }))
    assert(gt.getPhased)
    assert(gt.getPhaseSetId === null)
    assert(gt.getPhaseQuality === null)
  }

  test("set phase set and extract going htsjdk->adam") {
    val gt = buildGt(Map(("PS" -> (4: java.lang.Integer).asInstanceOf[java.lang.Object]),
      ("PQ" -> (10: java.lang.Integer).asInstanceOf[java.lang.Object])),
      converter.formatPhaseInfo,
      fns = Iterable((gb: GenotypeBuilder) => {
        gb.phased(true)
      }))
    assert(gt.getPhased)
    assert(gt.getPhaseSetId === 4)
    assert(gt.getPhaseQuality === 10)
  }

  test("no allelic depth going htsjdk->adam") {
    val gt = buildGt(Map.empty,
      converter.formatAllelicDepth,
      fns = Iterable((gb: GenotypeBuilder) => {
        gb.noAD
      }))

    assert(gt.getReferenceReadDepth === null)
    assert(gt.getAlternateReadDepth === null)
  }

  test("set allelic depth going htsjdk->adam") {
    val gt = buildGt(Map.empty,
      converter.formatAllelicDepth,
      fns = Iterable((gb: GenotypeBuilder) => {
        gb.AD(Array(3, 6))
      }))

    assert(gt.getReferenceReadDepth === 3)
    assert(gt.getAlternateReadDepth === 6)
  }

  test("no gt read depth going htsjdk->adam") {
    val gt = buildGt(Map.empty,
      converter.formatReadDepth,
      fns = Iterable((gb: GenotypeBuilder) => {
        gb.noDP
      }))

    assert(gt.getReadDepth === null)
  }

  test("extract gt read depth going htsjdk->adam") {
    val gt = buildGt(Map.empty,
      converter.formatReadDepth,
      fns = Iterable((gb: GenotypeBuilder) => {
        gb.DP(20)
      }))

    assert(gt.getReadDepth === 20)
  }

  test("no min gt read depth going htsjdk->adam") {
    val gt = buildGt(Map.empty,
      converter.formatMinReadDepth,
      fns = Iterable.empty)

    assert(gt.getMinReadDepth === null)
  }

  test("extract min gt read depth going htsjdk->adam") {
    val gt = buildGt(Map(("MIN_DP" -> (20: java.lang.Integer).asInstanceOf[java.lang.Object])),
      converter.formatMinReadDepth,
      fns = Iterable.empty)

    assert(gt.getMinReadDepth === 20)
  }

  test("no genotype quality going htsjdk->adam") {
    val gt = buildGt(Map.empty,
      converter.formatGenotypeQuality,
      fns = Iterable((gb: GenotypeBuilder) => {
        gb.noGQ()
      }))

    assert(gt.getGenotypeQuality === null)
  }

  test("extract genotype quality going htsjdk->adam") {
    val gt = buildGt(Map.empty,
      converter.formatGenotypeQuality,
      fns = Iterable((gb: GenotypeBuilder) => {
        gb.GQ(50)
      }))

    assert(gt.getGenotypeQuality === 50)
  }

  test("no phred likelihood going htsjdk->adam") {
    val gt = buildGt(Map.empty,
      converter.formatGenotypeLikelihoods,
      fns = Iterable((gb: GenotypeBuilder) => {
        gb.noPL()
      }))

    assert(gt.getGenotypeLikelihoods.isEmpty)
  }

  test("extract phred likelihoods going htsjdk->adam") {
    val gt = buildGt(Map.empty,
      converter.formatGenotypeLikelihoods,
      fns = Iterable((gb: GenotypeBuilder) => {
        gb.PL(Array(10, 30, 60))
      }))

    val gls = gt.getGenotypeLikelihoods
    assert(gls.size === 3)
    assert(gls(0) < -0.99e-1 && gls(0) > -1.1e-1)
    assert(gls(1) < -0.99e-3 && gls(1) > -1.1e-3)
    assert(gls(2) < -0.99e-6 && gls(2) > -1.1e-6)
  }

  test("no strand bias info going htsjdk->adam") {
    val gt = buildGt(Map.empty,
      converter.formatStrandBiasComponents,
      fns = Iterable.empty)

    assert(gt.getStrandBiasComponents.isEmpty)
  }

  test("extract strand bias info going htsjdk->adam") {
    val gt = buildGt(Map(("SB" -> Array(10, 12, 14, 16)
      .map(i => i: java.lang.Integer))),
      converter.formatStrandBiasComponents,
      fns = Iterable.empty)

    val sb = gt.getStrandBiasComponents
    assert(sb.size === 4)
    assert(sb(0) === 10)
    assert(sb(1) === 12)
    assert(sb(2) === 14)
    assert(sb(3) === 16)
  }

  def buildVca(
    objMap: Map[String, java.lang.Object],
    extractor: (HtsjdkGenotype, VariantCallingAnnotations.Builder, Int, Array[Int]) => VariantCallingAnnotations.Builder,
    fns: Iterable[GenotypeBuilder => GenotypeBuilder] = Iterable.empty): VariantCallingAnnotations = {
    val htsjdkGenotype = makeGenotype(objMap, fns)
    extractor(htsjdkGenotype,
      VariantCallingAnnotations.newBuilder,
      0,
      Array(0, 1, 2)).build
  }

  test("no filters going htsjdk->adam") {
    val vca = buildVca(Map.empty,
      converter.formatFilters,
      fns = Iterable.empty)

    assert(vca.getFiltersApplied) // sigh
    assert(vca.getFiltersPassed) // sigh
  }

  test("filters passed going htsjdk->adam") {
    val vca = buildVca(Map.empty,
      converter.formatFilters,
      fns = Iterable((gb: GenotypeBuilder) => {
        gb.filter("PASS")
      }))

    assert(vca.getFiltersApplied)
    assert(vca.getFiltersPassed)
  }

  test("extract single filter going htsjdk->adam") {
    val vca = buildVca(Map.empty,
      converter.formatFilters,
      fns = Iterable((gb: GenotypeBuilder) => {
        gb.filter("FAILED_FILTER")
      }))

    assert(vca.getFiltersApplied)
    assert(!vca.getFiltersPassed)
    val failedFilters = vca.getFiltersFailed
    assert(failedFilters.size === 1)
    assert(failedFilters(0) === "FAILED_FILTER")
  }

  test("extract multiple filters going htsjdk->adam") {
    val vca = buildVca(Map.empty,
      converter.formatFilters,
      fns = Iterable((gb: GenotypeBuilder) => {
        gb.filters("FAILED_FILTER1", "FAILED_FILTER2", "FAILED_FILTER3")
      }))

    assert(vca.getFiltersApplied)
    assert(!vca.getFiltersPassed)
    val failedFilters = vca.getFiltersFailed
    assert(failedFilters.size === 3)
    assert(failedFilters(0) === "FAILED_FILTER1")
    assert(failedFilters(1) === "FAILED_FILTER2")
    assert(failedFilters(2) === "FAILED_FILTER3")
  }

  test("no fisher strand bias going htsjdk->adam") {
    val vca = buildVca(Map.empty,
      converter.formatFisherStrandBias,
      fns = Iterable.empty)

    assert(vca.getFisherStrandBiasPValue === null)
  }

  test("extract fisher strand bias going htsjdk->adam") {
    val vca = buildVca(Map(("FS" -> (0.25f: java.lang.Float).asInstanceOf[java.lang.Object])),
      converter.formatFisherStrandBias,
      fns = Iterable.empty)

    assert(vca.getFisherStrandBiasPValue > 0.249f && vca.getFisherStrandBiasPValue < 0.251f)
  }

  test("no rms mapping quality going htsjdk->adam") {
    val vca = buildVca(Map.empty,
      converter.formatRmsMapQ,
      fns = Iterable.empty)

    assert(vca.getRmsMapQ === null)
  }

  test("extract rms mapping quality going htsjdk->adam") {
    val vca = buildVca(Map(("MQ" -> (40.0f: java.lang.Float).asInstanceOf[java.lang.Object])),
      converter.formatRmsMapQ,
      fns = Iterable.empty)

    assert(vca.getRmsMapQ > 39.9f && vca.getRmsMapQ < 40.1f)
  }

  test("no mq0 going htsjdk->adam") {
    val vca = buildVca(Map.empty,
      converter.formatMapQ0,
      fns = Iterable.empty)

    assert(vca.getMapq0Reads === null)
  }

  test("extract mq0 going htsjdk->adam") {
    val vca = buildVca(Map(("MQ0" -> (100: java.lang.Integer).asInstanceOf[java.lang.Object])),
      converter.formatMapQ0,
      fns = Iterable.empty)

    assert(vca.getMapq0Reads === 100)
  }

  def emptyGt: Genotype = Genotype.newBuilder.build
  def newGb: GenotypeBuilder = new GenotypeBuilder

  test("no gt read depth going adam->htsjdk") {
    val g = converter.extractAllelicDepth(emptyGt, newGb)
      .make

    assert(!g.hasAD)
  }

  test("extract gt read depth going adam->htsjdk") {
    val g = converter.extractAllelicDepth(Genotype.newBuilder
      .setReferenceReadDepth(10)
      .setAlternateReadDepth(15)
      .build, newGb)
      .make

    assert(g.hasAD)
    val attr = g.getAD
    assert(attr.length === 2)
    assert(attr(0) === 10)
    assert(attr(1) === 15)
  }

  test("throw iae if missing one component of gt read depth going adam->htsjdk") {
    intercept[IllegalArgumentException] {
      val g = converter.extractAllelicDepth(Genotype.newBuilder
        .setAlternateReadDepth(15)
        .build, newGb)
    }

    intercept[IllegalArgumentException] {
      val g = converter.extractAllelicDepth(Genotype.newBuilder
        .setReferenceReadDepth(10)
        .build, newGb)
    }
  }

  test("no depth going adam->htsjdk") {
    val g = converter.extractReadDepth(emptyGt, newGb)
      .make

    assert(!g.hasDP)
  }

  test("extract depth going adam->htsjdk") {
    val g = converter.extractReadDepth(Genotype.newBuilder
      .setReadDepth(100)
      .build, newGb)
      .make

    assert(g.hasDP)
    assert(g.getDP === 100)
  }

  test("no min depth going adam->htsjdk") {
    val g = converter.extractMinReadDepth(emptyGt, newGb)
      .make

    assert(!g.hasExtendedAttribute("MIN_DP"))
  }

  test("extract min depth going adam->htsjdk") {
    val g = converter.extractMinReadDepth(Genotype.newBuilder
      .setMinReadDepth(1234)
      .build, newGb)
      .make

    assert(g.hasExtendedAttribute("MIN_DP"))
    val attr = g.getExtendedAttribute("MIN_DP")
      .asInstanceOf[java.lang.Integer]
    assert(attr === 1234)
  }

  test("no quality going adam->htsjdk") {
    val g = converter.extractGenotypeQuality(emptyGt, newGb)
      .make

    assert(!g.hasGQ)
  }

  test("extract quality going adam->htsjdk") {
    val g = converter.extractGenotypeQuality(Genotype.newBuilder
      .setGenotypeQuality(10)
      .build, newGb)
      .make

    assert(g.hasGQ)
    assert(g.getGQ === 10)
  }

  test("no genotype likelihoods going adam->htsjdk") {
    val g = converter.extractGenotypeLikelihoods(emptyGt, newGb)
      .make

    assert(!g.hasPL)
  }

  test("extract genotype likelihoods going adam->htsjdk") {
    val g = converter.extractGenotypeLikelihoods(Genotype.newBuilder
      .setGenotypeLikelihoods(Seq(-0.1, -0.001, -0.000001)
        .map(d => d: java.lang.Double))
      .build, newGb)
      .make

    assert(g.hasPL)
    val pls = g.getPL
    assert(pls.size === 3)
    assert(pls(0) <= 11 && pls(0) >= 9)
    assert(pls(1) <= 31 && pls(1) >= 29)
    assert(pls(2) <= 61 && pls(2) >= 59)
  }

  test("no strand bias going adam->htsjdk") {
    val g = converter.extractStrandBiasComponents(emptyGt, newGb)
      .make

    assert(!g.hasExtendedAttribute("SB"))
  }

  test("malformed strand bias going adam->htsjdk") {

    intercept[IllegalArgumentException] {
      val g = converter.extractStrandBiasComponents(Genotype.newBuilder
        .setStrandBiasComponents(Seq(0, 10)
          .map(i => i: java.lang.Integer))
        .build, newGb)
    }
  }

  test("extract strand bias going adam->htsjdk") {
    val g = converter.extractStrandBiasComponents(Genotype.newBuilder
      .setStrandBiasComponents(Seq(0, 10, 5, 3)
        .map(i => i: java.lang.Integer))
      .build, newGb)
      .make

    assert(g.hasExtendedAttribute("SB"))
    val sb = g.getExtendedAttribute("SB").asInstanceOf[Array[Int]]
    assert(sb.length === 4)
    assert(sb(0) === 0)
    assert(sb(1) === 10)
    assert(sb(2) === 5)
    assert(sb(3) === 3)
  }

  test("no phasing info going adam->htsjdk") {
    val g = converter.extractPhaseInfo(emptyGt, newGb)
      .make

    assert(!g.isPhased)
  }

  test("unphased going adam->htsjdk") {
    val g = converter.extractPhaseInfo(Genotype.newBuilder
      .setPhased(false)
      .build, newGb)
      .make

    assert(!g.isPhased)
  }

  test("phased but no ps/pq going adam->htsjdk") {
    val g = converter.extractPhaseInfo(Genotype.newBuilder
      .setPhased(true)
      .build, newGb)
      .make

    assert(g.isPhased)
    assert(!g.hasExtendedAttribute("PS"))
    assert(!g.hasExtendedAttribute("PQ"))
  }

  test("phased but no pq going adam->htsjdk") {
    val g = converter.extractPhaseInfo(Genotype.newBuilder
      .setPhased(true)
      .setPhaseSetId(54321)
      .build, newGb)
      .make

    assert(g.isPhased)
    assert(g.hasExtendedAttribute("PS"))
    assert(g.getExtendedAttribute("PS").asInstanceOf[java.lang.Integer] === 54321)
    assert(!g.hasExtendedAttribute("PQ"))
  }

  test("phased but no ps going adam->htsjdk") {
    val g = converter.extractPhaseInfo(Genotype.newBuilder
      .setPhased(true)
      .setPhaseQuality(65)
      .build, newGb)
      .make

    assert(g.isPhased)
    assert(!g.hasExtendedAttribute("PS"))
    assert(g.hasExtendedAttribute("PQ"))
    assert(g.getExtendedAttribute("PQ").asInstanceOf[java.lang.Integer] === 65)
  }

  test("phased going adam->htsjdk") {
    val g = converter.extractPhaseInfo(Genotype.newBuilder
      .setPhased(true)
      .setPhaseSetId(4444)
      .setPhaseQuality(10)
      .build, newGb)
      .make

    assert(g.isPhased)
    assert(g.hasExtendedAttribute("PS"))
    assert(g.getExtendedAttribute("PS").asInstanceOf[java.lang.Integer] === 4444)
    assert(g.hasExtendedAttribute("PS"))
    assert(g.getExtendedAttribute("PQ").asInstanceOf[java.lang.Integer] === 10)
  }

  def emptyVca = VariantCallingAnnotations.newBuilder.build

  test("no filter info going adam->htsjdk") {
    val g = converter.extractFilters(emptyVca, newGb)
      .make

    assert(!g.isFiltered)
  }

  test("if filters applied, must set passed/failed going adam->htsjdk") {

    intercept[IllegalArgumentException] {
      val g = converter.extractFilters(VariantCallingAnnotations.newBuilder
        .setFiltersApplied(true)
        .build, newGb)
        .make
    }
  }

  test("filters passed going adam->htsjdk") {
    val g = converter.extractFilters(VariantCallingAnnotations.newBuilder
      .setFiltersApplied(true)
      .setFiltersPassed(true)
      .build, newGb)
      .make

    assert(!g.isFiltered)
    // yahtzee! should be "PASS", but htsjdk has weird conventions.
    assert(g.getFilters === null)
  }

  test("if filters failed, must set filters failed going adam->htsjdk") {

    intercept[IllegalArgumentException] {
      val g = converter.extractFilters(VariantCallingAnnotations.newBuilder
        .setFiltersApplied(true)
        .setFiltersPassed(false)
        .build, newGb)
        .make
    }
  }

  test("single filter failed going adam->htsjdk") {
    val g = converter.extractFilters(VariantCallingAnnotations.newBuilder
      .setFiltersApplied(true)
      .setFiltersPassed(false)
      .setFiltersFailed(Seq("lowmq"))
      .build, newGb)
      .make

    assert(g.isFiltered)
    assert(g.getFilters === "lowmq")
  }

  test("multiple filters failed going adam->htsjdk") {
    val g = converter.extractFilters(VariantCallingAnnotations.newBuilder
      .setFiltersApplied(true)
      .setFiltersPassed(false)
      .setFiltersFailed(Seq("lowmq", "lowdp"))
      .build, newGb)
      .make

    assert(g.isFiltered)
    assert(g.getFilters === "lowmq;lowdp")
  }

  test("no fisher strand bias going adam->htsjdk") {
    val g = converter.extractFisherStrandBias(emptyVca, newGb)
      .make

    assert(!g.hasExtendedAttribute("FS"))
  }

  test("extract fisher strand bias going adam->htsjdk") {
    val g = converter.extractFisherStrandBias(VariantCallingAnnotations.newBuilder
      .setFisherStrandBiasPValue(20.0f)
      .build, newGb)
      .make

    assert(g.hasExtendedAttribute("FS"))
    val fs = g.getExtendedAttribute("FS").asInstanceOf[java.lang.Float]
    assert(fs > 19.9f && fs < 20.1f)
  }

  test("no rms mapping quality going adam->htsjdk") {
    val g = converter.extractRmsMapQ(emptyVca, newGb)
      .make

    assert(!g.hasExtendedAttribute("MQ"))
  }

  test("extract rms mapping quality going adam->htsjdk") {
    val g = converter.extractRmsMapQ(VariantCallingAnnotations.newBuilder
      .setRmsMapQ(40.0f)
      .build, newGb)
      .make

    assert(g.hasExtendedAttribute("MQ"))
    val mq = g.getExtendedAttribute("MQ").asInstanceOf[java.lang.Float]
    assert(mq > 39.9f && mq < 40.1f)
  }

  test("no mapping quality 0 reads going adam->htsjdk") {
    val g = converter.extractMapQ0(emptyVca, newGb)
      .make

    assert(!g.hasExtendedAttribute("MQ0"))
  }

  test("extract mapping quality 0 reads going adam->htsjdk") {
    val g = converter.extractMapQ0(VariantCallingAnnotations.newBuilder
      .setMapq0Reads(5)
      .build, newGb)
      .make

    assert(g.hasExtendedAttribute("MQ0"))
    assert(g.getExtendedAttribute("MQ0").asInstanceOf[java.lang.Integer] === 5)
  }

  def makeVariant(variantAttributes: Map[String, java.lang.Object],
                  fns: Iterable[VariantContextBuilder => VariantContextBuilder]): HtsjdkVariantContext = {
    val vcb = fns.foldLeft(htsjdkSNVBuilder)((bldr, fn) => {
      fn(bldr)
    })
    variantAttributes.foreach(kv => vcb.attribute(kv._1, kv._2))
    vcb.make
  }

  def buildVariant(
    objMap: Map[String, java.lang.Object],
    extractor: (HtsjdkVariantContext, Variant.Builder) => Variant.Builder,
    fns: Iterable[VariantContextBuilder => VariantContextBuilder] = Iterable.empty): Variant = {
    val vc = makeVariant(objMap, fns)
    extractor(vc,
      Variant.newBuilder).build
  }

  test("no names set going htsjdk->adam") {
    val v = buildVariant(Map.empty,
      converter.formatNames,
      fns = Iterable((vcb: VariantContextBuilder) => {
        vcb.noID
      }))
    assert(v.getNames.isEmpty)
  }

  test("single name set going htsjdk->adam") {
    val v = buildVariant(Map.empty,
      converter.formatNames,
      fns = Iterable((vcb: VariantContextBuilder) => {
        vcb.id("singleName")
      }))
    assert(v.getNames.length === 1)
    assert(v.getNames.get(0) === "singleName")
  }

  test("multiple names set going htsjdk->adam") {
    val v = buildVariant(Map.empty,
      converter.formatNames,
      fns = Iterable((vcb: VariantContextBuilder) => {
        vcb.id("firstName;secondName")
      }))
    assert(v.getNames.length === 2)
    assert(v.getNames.get(0) === "firstName")
    assert(v.getNames.get(1) === "secondName")
  }

  test("no quality going htsjdk->adam") {
    val v = buildVariant(Map.empty,
      converter.formatQuality)
    assert(v.getQuality === null)
  }

  test("quality set going htsjdk->adam") {
    val v = buildVariant(Map.empty,
      converter.formatQuality,
      fns = Iterable((vcb: VariantContextBuilder) => {
        vcb.log10PError(-10.0)
      }))
    assert(v.getQuality > 99.9)
    assert(v.getQuality < 100.1)
  }

  test("no filters applied going htsjdk->adam") {
    val v = buildVariant(Map.empty,
      converter.formatFilters,
      fns = Iterable((vcb: VariantContextBuilder) => {
        vcb.unfiltered
      }))
    assert(!v.getFiltersApplied)
  }

  test("filters applied and passed going htsjdk->adam") {
    val v = buildVariant(Map.empty,
      converter.formatFilters,
      fns = Iterable((vcb: VariantContextBuilder) => {
        vcb.passFilters
      }))
    assert(v.getFiltersApplied)
    assert(v.getFiltersPassed)
    assert(v.getFiltersFailed.isEmpty)
  }

  test("single filter applied and failed going htsjdk->adam") {
    val v = buildVariant(Map.empty,
      converter.formatFilters,
      fns = Iterable((vcb: VariantContextBuilder) => {
        vcb.filter("FAILED")
      }))
    assert(v.getFiltersApplied)
    assert(!v.getFiltersPassed)
    assert(v.getFiltersFailed.size === 1)
    assert(v.getFiltersFailed.get(0) === "FAILED")
  }

  test("multiple filters applied and failed going htsjdk->adam") {
    val v = buildVariant(Map.empty,
      converter.formatFilters,
      fns = Iterable((vcb: VariantContextBuilder) => {
        vcb.filters(Set("FAILED1", "FAILED2", "FAILED3"))
      }))
    assert(v.getFiltersApplied)
    assert(!v.getFiltersPassed)
    assert(v.getFiltersFailed.size === 3)
    val failedSet = v.getFiltersFailed.toSet
    assert(failedSet("FAILED1"))
    assert(failedSet("FAILED2"))
    assert(failedSet("FAILED3"))
  }

  val emptyV = Variant.newBuilder
    .build

  test("no names set adam->htsjdk") {
    val vc = converter.extractNames(emptyV, htsjdkSNVBuilder)
      .make

    assert(!vc.hasID)
  }

  test("set a single name adam->htsjdk") {
    val vc = converter.extractNames(Variant.newBuilder
      .setNames(Seq("name"))
      .build, htsjdkSNVBuilder)
      .make

    assert(vc.hasID)
    assert(vc.getID === "name")
  }

  test("set multiple names adam->htsjdk") {
    val vc = converter.extractNames(Variant.newBuilder
      .setNames(Seq("name1", "name2"))
      .build, htsjdkSNVBuilder)
      .make

    assert(vc.hasID)
    assert(vc.getID === "name1;name2")
  }

  test("no qual set adam->htsjdk") {
    val vc = converter.extractQuality(emptyV, htsjdkSNVBuilder)
      .make

    assert(!vc.hasLog10PError)
  }

  test("qual is set adam->htsjdk") {
    val vc = converter.extractQuality(Variant.newBuilder
      .setQuality(100.0)
      .build, htsjdkSNVBuilder)
      .make

    assert(vc.hasLog10PError)
    assert(vc.getPhredScaledQual > 99.9)
    assert(vc.getPhredScaledQual < 100.1)
  }

  test("no filters applied adam->htsjdk") {
    val vc = converter.extractFilters(Variant.newBuilder
      .setFiltersApplied(false)
      .build, htsjdkSNVBuilder)
      .make

    assert(!vc.filtersWereApplied)
  }

  test("null filters applied adam->htsjdk") {
    val vc = converter.extractFilters(Variant.newBuilder
      .setFiltersApplied(null)
      .build, htsjdkSNVBuilder)
      .make

    assert(!vc.filtersWereApplied)
  }

  test("filters passed adam->htsjdk") {
    val vc = converter.extractFilters(Variant.newBuilder
      .setFiltersApplied(true)
      .setFiltersPassed(true)
      .build, htsjdkSNVBuilder)
      .make

    assert(vc.filtersWereApplied)
    assert(vc.isNotFiltered)
  }

  test("if filter failed, must have filters adam->htsjdk") {
    intercept[IllegalArgumentException] {
      converter.extractFilters(Variant.newBuilder
        .setFiltersApplied(true)
        .setFiltersPassed(false)
        .build, htsjdkSNVBuilder)
        .make
    }
  }

  test("single filter failed adam->htsjdk") {
    val vc = converter.extractFilters(Variant.newBuilder
      .setFiltersApplied(true)
      .setFiltersPassed(false)
      .setFiltersFailed(Seq("FAILED"))
      .build, htsjdkSNVBuilder)
      .make

    assert(vc.filtersWereApplied)
    assert(vc.isFiltered)
    assert(vc.getFilters.size === 1)
    assert(vc.getFilters.contains("FAILED"))
  }

  test("multiple filters failed adam->htsjdk") {
    val vc = converter.extractFilters(Variant.newBuilder
      .setFiltersApplied(true)
      .setFiltersPassed(false)
      .setFiltersFailed(Seq("FAILED1", "FAILED2"))
      .build, htsjdkSNVBuilder)
      .make

    assert(vc.filtersWereApplied)
    assert(vc.isFiltered)
    assert(vc.getFilters.size === 2)
    assert(vc.getFilters.contains("FAILED1"))
    assert(vc.getFilters.contains("FAILED2"))
  }

  def buildVariantAnnotation(
    objMap: Map[String, java.lang.Object],
    extractor: (HtsjdkVariantContext, VariantAnnotation.Builder, Variant, Int) => VariantAnnotation.Builder,
    fns: Iterable[VariantContextBuilder => VariantContextBuilder] = Iterable.empty,
    idx: Int = 0): VariantAnnotation = {
    val vc = makeVariant(objMap, fns)
    extractor(vc,
      VariantAnnotation.newBuilder,
      emptyV,
      idx).build
  }

  test("no ancestral allele set going htsjdk->adam") {
    val va = buildVariantAnnotation(Map.empty,
      converter.formatAncestralAllele)
    assert(va.getAncestralAllele === null)
  }

  test("ancestral allele set going htsjdk->adam") {
    val va = buildVariantAnnotation(Map(("AA" -> "ABCD")),
      converter.formatAncestralAllele)
    assert(va.getAncestralAllele === "ABCD")
  }

  test("no dbsnp membership set going htsjdk->adam") {
    val va = buildVariantAnnotation(Map.empty,
      converter.formatDbSnp)
    assert(va.getDbSnp === null)
  }

  test("dbsnp membership set going htsjdk->adam") {
    val va = buildVariantAnnotation(Map(("DB", true: java.lang.Boolean)),
      converter.formatDbSnp)
    assert(va.getDbSnp)
  }

  test("no hapmap2 membership set going htsjdk->adam") {
    val va = buildVariantAnnotation(Map.empty,
      converter.formatHapMap2)
    assert(va.getHapMap2 === null)
  }

  test("hapmap2 membership set going htsjdk->adam") {
    val va = buildVariantAnnotation(Map(("H2", true: java.lang.Boolean)),
      converter.formatHapMap2)
    assert(va.getHapMap2)
  }

  test("no hapmap3 membership set going htsjdk->adam") {
    val va = buildVariantAnnotation(Map.empty,
      converter.formatHapMap3)
    assert(va.getHapMap3 === null)
  }

  test("hapmap3 membership set going htsjdk->adam") {
    val va = buildVariantAnnotation(Map(("H3", true: java.lang.Boolean)),
      converter.formatHapMap3)
    assert(va.getHapMap3)
  }

  test("no validated set going htsjdk->adam") {
    val va = buildVariantAnnotation(Map.empty,
      converter.formatValidated)
    assert(va.getValidated === null)
  }

  test("validated set going htsjdk->adam") {
    val va = buildVariantAnnotation(Map(("VALIDATED", true: java.lang.Boolean)),
      converter.formatValidated)
    assert(va.getValidated)
  }

  test("no 1000G membership set going htsjdk->adam") {
    val va = buildVariantAnnotation(Map.empty,
      converter.formatThousandGenomes)
    assert(va.getThousandGenomes === null)
  }

  test("1000G membership set going htsjdk->adam") {
    val va = buildVariantAnnotation(Map(("1000G", true: java.lang.Boolean)),
      converter.formatThousandGenomes)
    assert(va.getThousandGenomes)
  }

  test("not somatic going htsjdk->adam") {
    val va = buildVariantAnnotation(Map.empty,
      converter.formatSomatic)
    assert(!va.getSomatic)
  }

  test("somatic going htsjdk->adam") {
    val va = buildVariantAnnotation(Map(("SOMATIC", true: java.lang.Boolean)),
      converter.formatSomatic)
    assert(va.getSomatic)
  }

  test("no allele count going htsjdk->adam") {
    val va = buildVariantAnnotation(Map.empty,
      converter.formatAlleleCount)
    assert(va.getAlleleCount === null)
  }

  test("single allele count going htsjdk->adam") {
    val acList: java.util.List[java.lang.Integer] = List(
      10).map(i => i: java.lang.Integer)
    val va = buildVariantAnnotation(Map(("AC", acList)),
      converter.formatAlleleCount)
    assert(va.getAlleleCount === 10)
  }

  test("multiple allele counts going htsjdk->adam") {
    val acList: java.util.List[java.lang.Integer] = List(
      10, 13, 16).map(i => i: java.lang.Integer)
    val va = buildVariantAnnotation(Map(("AC", acList)),
      converter.formatAlleleCount,
      idx = 2)
    assert(va.getAlleleCount === 16)
  }

  test("no allele frequency going htsjdk->adam") {
    val va = buildVariantAnnotation(Map.empty,
      converter.formatAlleleFrequency)
    assert(va.getAlleleFrequency === null)
  }

  test("single allele frequency going htsjdk->adam") {
    val acList: java.util.List[java.lang.Float] = List(
      0.1f).map(i => i: java.lang.Float)
    val va = buildVariantAnnotation(Map(("AF", acList)),
      converter.formatAlleleFrequency)
    assert(va.getAlleleFrequency > 0.09f && va.getAlleleFrequency < 0.11f)
  }

  test("single allele frequency is +Inf going htsjdk->adam") {
    val acList: java.util.List[String] = List("+Inf")
    val va = buildVariantAnnotation(Map(("AF", acList)),
      converter.formatAlleleFrequency)
    assert(va.getAlleleFrequency == Float.PositiveInfinity)
  }

  test("single allele frequency is -Inf going htsjdk->adam") {
    val acList: java.util.List[String] = List("-Inf")
    val va = buildVariantAnnotation(Map(("AF", acList)),
      converter.formatAlleleFrequency)
    assert(va.getAlleleFrequency == Float.NegativeInfinity)
  }

  test("multiple allele frequencies going htsjdk->adam") {
    val acList: java.util.List[java.lang.Float] = List(
      0.1f, 0.01f, 0.001f).map(i => i: java.lang.Float)
    val va = buildVariantAnnotation(Map(("AF", acList)),
      converter.formatAlleleFrequency,
      idx = 2)
    assert(va.getAlleleFrequency < 0.0011f && va.getAlleleFrequency > 0.0009f)
  }

  test("no CIGAR going htsjdk->adam") {
    val va = buildVariantAnnotation(Map.empty,
      converter.formatCigar)
    assert(va.getCigar === null)
  }

  test("single CIGAR going htsjdk->adam") {
    val acList: java.util.List[String] = List("10D90M")
    val va = buildVariantAnnotation(Map(("CIGAR", acList)),
      converter.formatCigar)
    assert(va.getCigar === "10D90M")
  }

  test("multiple CIGARs going htsjdk->adam") {
    val acList: java.util.List[String] = List("10D90M", "100M", "90M10D")
    val va = buildVariantAnnotation(Map(("CIGAR", acList)),
      converter.formatCigar,
      idx = 2)
    assert(va.getCigar === "90M10D")
  }

  test("no read depth going htsjdk->adam") {
    val va = buildVariantAnnotation(Map.empty,
      converter.formatReadDepth)
    assert(va.getReferenceReadDepth === null)
    assert(va.getReadDepth === null)
  }

  test("single read depth going htsjdk->adam") {
    val acList: java.util.List[java.lang.Integer] = List(
      5, 10).map(i => i: java.lang.Integer)
    val va = buildVariantAnnotation(Map(("AD", acList)),
      converter.formatReadDepth)
    assert(va.getReferenceReadDepth === 5)
    assert(va.getReadDepth === 10)
  }

  test("multiple read depths going htsjdk->adam") {
    val acList: java.util.List[java.lang.Integer] = List(
      5, 10, 13, 16).map(i => i: java.lang.Integer)
    val va = buildVariantAnnotation(Map(("AD", acList)),
      converter.formatReadDepth,
      idx = 2)
    assert(va.getReferenceReadDepth === 5)
    assert(va.getReadDepth === 16)
  }

  test("no forward read depth going htsjdk->adam") {
    val va = buildVariantAnnotation(Map.empty,
      converter.formatForwardReadDepth)
    assert(va.getReferenceForwardReadDepth === null)
    assert(va.getForwardReadDepth === null)
  }

  test("single forward read depth going htsjdk->adam") {
    val acList: java.util.List[java.lang.Integer] = List(
      5, 10).map(i => i: java.lang.Integer)
    val va = buildVariantAnnotation(Map(("ADF", acList)),
      converter.formatForwardReadDepth)
    assert(va.getReferenceForwardReadDepth === 5)
    assert(va.getForwardReadDepth === 10)
  }

  test("multiple forward read depths going htsjdk->adam") {
    val acList: java.util.List[java.lang.Integer] = List(
      5, 10, 13, 16).map(i => i: java.lang.Integer)
    val va = buildVariantAnnotation(Map(("ADF", acList)),
      converter.formatForwardReadDepth,
      idx = 2)
    assert(va.getReferenceForwardReadDepth === 5)
    assert(va.getForwardReadDepth === 16)
  }

  test("no reverse read depth going htsjdk->adam") {
    val va = buildVariantAnnotation(Map.empty,
      converter.formatReverseReadDepth)
    assert(va.getReferenceReverseReadDepth === null)
    assert(va.getReverseReadDepth === null)
  }

  test("single reverse read depth going htsjdk->adam") {
    val acList: java.util.List[java.lang.Integer] = List(
      5, 10).map(i => i: java.lang.Integer)
    val va = buildVariantAnnotation(Map(("ADR", acList)),
      converter.formatReverseReadDepth)
    assert(va.getReferenceReverseReadDepth === 5)
    assert(va.getReverseReadDepth === 10)
  }

  test("multiple reverse read depths going htsjdk->adam") {
    val acList: java.util.List[java.lang.Integer] = List(
      5, 10, 13, 16).map(i => i: java.lang.Integer)
    val va = buildVariantAnnotation(Map(("ADR", acList)),
      converter.formatReverseReadDepth,
      idx = 2)
    assert(va.getReferenceReverseReadDepth === 5)
    assert(va.getReverseReadDepth === 16)
  }

  val emptyVa = VariantAnnotation.newBuilder
    .build

  test("no ancestral allele set adam->htsjdk") {
    val vc = converter.extractAncestralAllele(emptyVa, htsjdkSNVBuilder)
      .make

    assert(!vc.hasAttribute("AA"))
  }

  test("ancestral allele set adam->htsjdk") {
    val vc = converter.extractAncestralAllele(VariantAnnotation.newBuilder
      .setAncestralAllele("ACGT")
      .build, htsjdkSNVBuilder)
      .make

    assert(vc.hasAttribute("AA"))
    assert(vc.getAttributeAsString("AA", null) === "ACGT")
  }

  test("no dbsnp membership set adam->htsjdk") {
    val vc = converter.extractDbSnp(emptyVa, htsjdkSNVBuilder)
      .make

    assert(!vc.hasAttribute("DB"))
  }

  test("dbsnp membership set adam->htsjdk") {
    val vc = converter.extractDbSnp(VariantAnnotation.newBuilder
      .setDbSnp(true)
      .build, htsjdkSNVBuilder)
      .make

    assert(vc.hasAttribute("DB"))
    assert(vc.getAttributeAsBoolean("DB", false))
  }

  test("no hapmap2 membership set adam->htsjdk") {
    val vc = converter.extractHapMap2(emptyVa, htsjdkSNVBuilder)
      .make

    assert(!vc.hasAttribute("H2"))
  }

  test("hapmap2 membership set adam->htsjdk") {
    val vc = converter.extractHapMap2(VariantAnnotation.newBuilder
      .setHapMap2(true)
      .build, htsjdkSNVBuilder)
      .make

    assert(vc.hasAttribute("H2"))
    assert(vc.getAttributeAsBoolean("H2", false))
  }

  test("no hapmap3 membership set adam->htsjdk") {
    val vc = converter.extractHapMap3(emptyVa, htsjdkSNVBuilder)
      .make

    assert(!vc.hasAttribute("H3"))
  }

  test("hapmap3 membership set adam->htsjdk") {
    val vc = converter.extractHapMap3(VariantAnnotation.newBuilder
      .setHapMap3(true)
      .build, htsjdkSNVBuilder)
      .make

    assert(vc.hasAttribute("H3"))
    assert(vc.getAttributeAsBoolean("H3", false))
  }

  test("no validated set adam->htsjdk") {
    val vc = converter.extractValidated(emptyVa, htsjdkSNVBuilder)
      .make

    assert(!vc.hasAttribute("VALIDATED"))
  }

  test("validated set adam->htsjdk") {
    val vc = converter.extractValidated(VariantAnnotation.newBuilder
      .setValidated(true)
      .build, htsjdkSNVBuilder)
      .make

    assert(vc.hasAttribute("VALIDATED"))
    assert(vc.getAttributeAsBoolean("VALIDATED", true))
  }

  test("no 1000G membership set adam->htsjdk") {
    val vc = converter.extractThousandGenomes(emptyVa, htsjdkSNVBuilder)
      .make

    assert(!vc.hasAttribute("1000G"))
  }

  test("1000G membership set adam->htsjdk") {
    val vc = converter.extractThousandGenomes(VariantAnnotation.newBuilder
      .setThousandGenomes(true)
      .build, htsjdkSNVBuilder)
      .make

    assert(vc.hasAttribute("1000G"))
    assert(vc.getAttributeAsBoolean("1000G", false))
  }

  test("no allele count set adam->htsjdk") {
    val vc = converter.extractAlleleCount(emptyVa, htsjdkSNVBuilder)
      .make

    assert(!vc.hasAttribute("AC"))
  }

  test("allele count set adam->htsjdk") {
    val vc = converter.extractAlleleCount(VariantAnnotation.newBuilder
      .setAlleleCount(42)
      .build, htsjdkSNVBuilder)
      .make

    assert(vc.hasAttribute("AC"))
    assert(vc.getAttributeAsList("AC").size === 1)
    assert(vc.getAttributeAsList("AC").get(0) === "42")
  }

  test("no allele frequency set adam->htsjdk") {
    val vc = converter.extractAlleleFrequency(emptyVa, htsjdkSNVBuilder)
      .make

    assert(!vc.hasAttribute("AF"))
  }

  test("allele frequency set adam->htsjdk") {
    val vc = converter.extractAlleleFrequency(VariantAnnotation.newBuilder
      .setAlleleFrequency(0.1f)
      .build, htsjdkSNVBuilder)
      .make

    assert(vc.hasAttribute("AF"))
    assert(vc.getAttributeAsList("AF").size === 1)
    assert(vc.getAttributeAsList("AF").get(0) === "0.1")
  }

  test("no cigar set adam->htsjdk") {
    val vc = converter.extractCigar(emptyVa, htsjdkSNVBuilder)
      .make

    assert(!vc.hasAttribute("CIGAR"))
  }

  test("cigar set adam->htsjdk") {
    val vc = converter.extractCigar(VariantAnnotation.newBuilder
      .setCigar("10D10M")
      .build, htsjdkSNVBuilder)
      .make

    assert(vc.hasAttribute("CIGAR"))
    assert(vc.getAttributeAsList("CIGAR").size === 1)
    assert(vc.getAttributeAsList("CIGAR").get(0) === "10D10M")
  }

  test("no read depth set adam->htsjdk") {
    val vc = converter.extractReadDepth(emptyVa, htsjdkSNVBuilder)
      .make

    assert(!vc.hasAttribute("AD"))
  }

  test("read depth set adam->htsjdk") {
    val vc = converter.extractReadDepth(VariantAnnotation.newBuilder
      .setReferenceReadDepth(5)
      .setReadDepth(10)
      .build, htsjdkSNVBuilder)
      .make

    assert(vc.hasAttribute("AD"))
    assert(vc.getAttributeAsList("AD").size === 2)
    assert(vc.getAttributeAsList("AD").get(0) === "5")
    assert(vc.getAttributeAsList("AD").get(1) === "10")
  }

  test("read depth without reference read depth") {
    intercept[IllegalArgumentException] {
      val vc = converter.extractReadDepth(VariantAnnotation.newBuilder
        .setReadDepth(10)
        .build, htsjdkSNVBuilder)
        .make
    }
  }

  test("reference read depth without read depth") {
    intercept[IllegalArgumentException] {
      val vc = converter.extractReadDepth(VariantAnnotation.newBuilder
        .setReferenceReadDepth(10)
        .build, htsjdkSNVBuilder)
        .make
    }
  }

  test("no forward read depth set adam->htsjdk") {
    val vc = converter.extractForwardReadDepth(emptyVa, htsjdkSNVBuilder)
      .make

    assert(!vc.hasAttribute("ADF"))
  }

  test("forward read depth set adam->htsjdk") {
    val vc = converter.extractForwardReadDepth(VariantAnnotation.newBuilder
      .setReferenceForwardReadDepth(5)
      .setForwardReadDepth(10)
      .build, htsjdkSNVBuilder)
      .make

    assert(vc.hasAttribute("ADF"))
    assert(vc.getAttributeAsList("ADF").size === 2)
    assert(vc.getAttributeAsList("ADF").get(0) === "5")
    assert(vc.getAttributeAsList("ADF").get(1) === "10")
  }

  test("reference forward read depth without forward read depth") {
    intercept[IllegalArgumentException] {
      val vc = converter.extractForwardReadDepth(VariantAnnotation.newBuilder
        .setReferenceForwardReadDepth(10)
        .build, htsjdkSNVBuilder)
        .make
    }
  }

  test("forward read depth without reference forward read depth") {
    intercept[IllegalArgumentException] {
      val vc = converter.extractForwardReadDepth(VariantAnnotation.newBuilder
        .setForwardReadDepth(10)
        .build, htsjdkSNVBuilder)
        .make
    }
  }

  test("no reverse read depth set adam->htsjdk") {
    val vc = converter.extractReverseReadDepth(emptyVa, htsjdkSNVBuilder)
      .make

    assert(!vc.hasAttribute("ADR"))
  }

  test("reverse read depth set adam->htsjdk") {
    val vc = converter.extractReverseReadDepth(VariantAnnotation.newBuilder
      .setReferenceReverseReadDepth(5)
      .setReverseReadDepth(10)
      .build, htsjdkSNVBuilder)
      .make

    assert(vc.hasAttribute("ADR"))
    assert(vc.getAttributeAsList("ADR").size === 2)
    assert(vc.getAttributeAsList("ADR").get(0) === "5")
    assert(vc.getAttributeAsList("ADR").get(1) === "10")
  }

  test("reference reverse read depth without reverse read depth") {
    intercept[IllegalArgumentException] {
      val vc = converter.extractReverseReadDepth(VariantAnnotation.newBuilder
        .setReferenceReverseReadDepth(10)
        .build, htsjdkSNVBuilder)
        .make
    }
  }

  test("reverse read depth without reference reverse read depth") {
    intercept[IllegalArgumentException] {
      val vc = converter.extractReverseReadDepth(VariantAnnotation.newBuilder
        .setReverseReadDepth(10)
        .build, htsjdkSNVBuilder)
        .make
    }
  }

  val v = Variant.newBuilder
    .setReferenceName("1")
    .setStart(0L)
    .setEnd(1L)
    .setReferenceAllele("A")
    .setAlternateAllele("T")
    .build

  def annV(a: VariantAnnotation): Variant = Variant.newBuilder(v)
    .setAnnotation(a)
    .build

  test("VCF INFO attribute Number=0 Type=Flag adam->htsjdk") {
    val va = VariantAnnotation.newBuilder
      .setAttributes(ImmutableMap.of("FLAG", "true"))
      .build

    val adamVc = ADAMVariantContext(annV(va), None)

    val flagHeader = new VCFInfoHeaderLine("FLAG",
      0,
      VCFHeaderLineType.Flag,
      "Flag")

    val vc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ flagHeader, lenient, false)
      .convert(adamVc).orNull

    assert(vc.hasAttribute("FLAG"))
    assert(vc.getAttributeAsBoolean("FLAG", false))
  }

  ignore("VCF INFO attribute Number=4 Type=Flag adam->htsjdk unsupported, strict") {
    val va = VariantAnnotation.newBuilder
      .setAttributes(ImmutableMap.of("FLAG", "true,false,true,false"))
      .build

    val adamVc = ADAMVariantContext(annV(va), None)

    val flagHeader = new VCFInfoHeaderLine("FLAG",
      4,
      VCFHeaderLineType.Flag,
      "Flag")

    /**
     * This test is meant to exercise the validation stringency wrapping around
     * parsing header lines. However, we can't actually exercise these cases, due
     * to HTSJDK's internal validation. We have two error cases:
     *
     * * Format line with a Flag
     * * Info line with a multi-valued Flag
     *
     * HTSJDK won't let you create the former (throws an exception in the header line
     * constructor) and it overwrites your count value to 0 in the latter case.
     *
     * As such, I've added this test but made it ignored as a placeholder to document
     * this behavior.
     */
    intercept[IllegalArgumentException] {
      new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ flagHeader,
        ValidationStringency.STRICT, false)
    }
  }

  test("VCF INFO attribute Number=1 Type=Integer adam->htsjdk") {
    val va = VariantAnnotation.newBuilder
      .setAttributes(ImmutableMap.of("ONE_INT", "42"))
      .build

    val adamVc = ADAMVariantContext(annV(va), None)

    val oneIntHeader = new VCFInfoHeaderLine("ONE_INT",
      1,
      VCFHeaderLineType.Integer,
      "Number=1 Type=Integer")

    val vc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ oneIntHeader, lenient, false)
      .convert(adamVc).orNull

    assert(vc.hasAttribute("ONE_INT"))
    assert(vc.getAttribute("ONE_INT", -1) === 42)
  }

  test("VCF INFO attribute Number=4 Type=Integer adam->htsjdk") {
    val va = VariantAnnotation.newBuilder
      .setAttributes(ImmutableMap.of("FOUR_INTS", "5,10,15,20"))
      .build

    val adamVc = ADAMVariantContext(annV(va), None)

    val fourIntsHeader = new VCFInfoHeaderLine("FOUR_INTS",
      4,
      VCFHeaderLineType.Integer,
      "Number=4 Type=Integer")

    val vc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ fourIntsHeader, lenient, false)
      .convert(adamVc).orNull

    assert(vc.hasAttribute("FOUR_INTS"))
    assert(vc.getAttributeAsList("FOUR_INTS").size === 4)
    assert(vc.getAttributeAsList("FOUR_INTS").get(0) === 5)
    assert(vc.getAttributeAsList("FOUR_INTS").get(1) === 10)
    assert(vc.getAttributeAsList("FOUR_INTS").get(2) === 15)
    assert(vc.getAttributeAsList("FOUR_INTS").get(3) === 20)
  }

  test("VCF INFO attribute Number=A Type=Integer adam->htsjdk") {
    val va = VariantAnnotation.newBuilder
      .setAttributes(ImmutableMap.of("A_INT", "42"))
      .build

    val adamVc = ADAMVariantContext(annV(va), None)

    val aIntHeader = new VCFInfoHeaderLine("A_INT",
      VCFHeaderLineCount.A,
      VCFHeaderLineType.Integer,
      "Number=A Type=Integer")

    val vc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ aIntHeader, lenient, false)
      .convert(adamVc).orNull

    assert(vc.hasAttribute("A_INT"))
    assert(vc.getAttribute("A_INT", -1) === Array(42))
  }

  test("VCF INFO attribute Number=R Type=Integer adam->htsjdk") {
    val va = VariantAnnotation.newBuilder
      .setAttributes(ImmutableMap.of("R_INT", "5,10"))
      .build

    val adamVc = ADAMVariantContext(annV(va), None)

    val rIntHeader = new VCFInfoHeaderLine("R_INT",
      VCFHeaderLineCount.R,
      VCFHeaderLineType.Integer,
      "Number=R Type=Integer")

    val vc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ rIntHeader, lenient, false)
      .convert(adamVc).orNull

    assert(vc.hasAttribute("R_INT"))
    assert(vc.getAttributeAsList("R_INT").size === 2)
    assert(vc.getAttributeAsList("R_INT").get(0) === 5)
    assert(vc.getAttributeAsList("R_INT").get(1) === 10)
  }

  test("VCF INFO attribute Number=R Type=String adam->htsjdk") {
    val va = VariantAnnotation.newBuilder
      .setAttributes(ImmutableMap.of("R_STRING", "foo,bar"))
      .build

    val adamVc = ADAMVariantContext(annV(va), None)

    val rStringHeader = new VCFInfoHeaderLine("R_STRING",
      VCFHeaderLineCount.R,
      VCFHeaderLineType.String,
      "Number=R Type=String")

    val vc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ rStringHeader, lenient, false)
      .convert(adamVc).orNull

    assert(vc.hasAttribute("R_STRING"))
    assert(vc.getAttributeAsList("R_STRING").size === 2)
    assert(vc.getAttributeAsList("R_STRING").get(0) === "foo")
    assert(vc.getAttributeAsList("R_STRING").get(1) === "bar")
  }

  test("VCF INFO attribute Number=G Type=String adam->htsjdk not supported") {
    val va = VariantAnnotation.newBuilder
      .setAttributes(ImmutableMap.of("STRING_G", "foo,bar"))
      .build

    val adamVc = ADAMVariantContext(annV(va), None)

    val gStringHeader = new VCFInfoHeaderLine("STRING_G",
      VCFHeaderLineCount.G,
      VCFHeaderLineType.String,
      "Number=G Type=String")

    intercept[IllegalArgumentException] {
      val vc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ gStringHeader, ValidationStringency.STRICT, false)
        .convert(adamVc).orNull
    }
  }

  test("VCF INFO attribute Number=0 Type=Flag htsjdk->adam") {
    val vc = htsjdkSNVBuilder
      .attribute("FLAG", true)
      .make

    val flagHeader = new VCFInfoHeaderLine("FLAG",
      0,
      VCFHeaderLineType.Flag,
      "Flag")

    val adamVc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ flagHeader, lenient, false)
      .convert(vc)
      .head

    val v = adamVc.variant.variant
    assert(v.getAnnotation.getAttributes.containsKey("FLAG"))
    assert(v.getAnnotation.getAttributes.get("FLAG") === "true")
  }

  test("VCF INFO attribute Number=1 Type=Integer htsjdk->adam") {
    val vc = htsjdkSNVBuilder
      .attribute("ONE_INT", "42")
      .make

    val oneIntHeader = new VCFInfoHeaderLine("ONE_INT",
      1,
      VCFHeaderLineType.Integer,
      "Number=1 Type=Integer")

    val adamVc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ oneIntHeader, lenient, false)
      .convert(vc)
      .head

    val v = adamVc.variant.variant
    assert(v.getAnnotation.getAttributes.containsKey("ONE_INT"))
    assert(v.getAnnotation.getAttributes.get("ONE_INT") === "42")
  }

  test("VCF INFO attribute Number=4 Type=Integer htsjdk->adam") {
    val vc = htsjdkSNVBuilder
      // in htsjdk INFO multivalue attributes are List<String>, even for Type=Integer and Type=Float
      .attribute("FOUR_INTS", ImmutableList.of("5", "10", "15", "20"))
      .make

    val fourIntsHeader = new VCFInfoHeaderLine("FOUR_INTS",
      4,
      VCFHeaderLineType.Integer,
      "Number=4 Type=Integer")

    val adamVc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ fourIntsHeader, lenient, false)
      .convert(vc)
      .head

    val v = adamVc.variant.variant
    assert(v.getAnnotation.getAttributes.containsKey("FOUR_INTS"))
    // in bdg-formats INFO multivalue attributes are comma-separated values in a String
    assert(v.getAnnotation.getAttributes.get("FOUR_INTS") === "5,10,15,20")
  }

  test("VCF INFO attribute Number=4 Type=Float htsjdk->adam") {
    val vc = htsjdkSNVBuilder
      .attribute("FOUR_FLOATS", ImmutableList.of("5.0", "10.1", "15.2", "20.3"))
      .make

    val fourFloatsHeader = new VCFInfoHeaderLine("FOUR_FLOATS",
      4,
      VCFHeaderLineType.Float,
      "Number=4 Type=Float")

    val adamVc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ fourFloatsHeader, lenient, false)
      .convert(vc)
      .head

    val v = adamVc.variant.variant
    assert(v.getAnnotation.getAttributes.containsKey("FOUR_FLOATS"))
    assert(v.getAnnotation.getAttributes.get("FOUR_FLOATS") === "5.0,10.1,15.2,20.3")
  }

  test("VCF INFO attribute Number=A Type=Integer htsjdk->adam") {
    val vc = htsjdkSNVBuilder
      .attribute("A_INT", ImmutableList.of("5", "10", "15", "20"))
      .make

    val aIntHeader = new VCFInfoHeaderLine("A_INT",
      VCFHeaderLineCount.A,
      VCFHeaderLineType.Integer,
      "Number=A Type=Integer")

    val adamVc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ aIntHeader, lenient, false)
      .convert(vc)
      .head

    val v = adamVc.variant.variant
    assert(v.getAnnotation.getAttributes.containsKey("A_INT"))
    assert(v.getAnnotation.getAttributes.get("A_INT") === "5")
  }

  test("VCF INFO attribute Number=R Type=Integer htsjdk->adam") {
    val vc = htsjdkSNVBuilder
      .attribute("R_INT", ImmutableList.of("5", "10", "15", "20"))
      .make

    val rIntHeader = new VCFInfoHeaderLine("R_INT",
      VCFHeaderLineCount.R,
      VCFHeaderLineType.Integer,
      "Number=R Type=Integer")

    val adamVc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ rIntHeader, lenient, false)
      .convert(vc)
      .head

    val v = adamVc.variant.variant
    assert(v.getAnnotation.getAttributes.containsKey("R_INT"))
    assert(v.getAnnotation.getAttributes.get("R_INT") === "5,10")
  }

  test("VCF INFO attribute Number=R Type=String htsjdk->adam") {
    val vc = htsjdkSNVBuilder
      .attribute("R_STRING", ImmutableList.of("foo", "bar", "baz"))
      .make

    val rStringHeader = new VCFInfoHeaderLine("R_STRING",
      VCFHeaderLineCount.R,
      VCFHeaderLineType.String,
      "Number=R Type=String")

    val adamVc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ rStringHeader, lenient, false)
      .convert(vc)
      .head

    val v = adamVc.variant.variant
    assert(v.getAnnotation.getAttributes.containsKey("R_STRING"))
    assert(v.getAnnotation.getAttributes.get("R_STRING") === "foo,bar")
  }

  test("VCF INFO attribute Number=G Type=String htsjdk->adam not supported") {
    val vc = htsjdkSNVBuilder
      .attribute("STRING_G", ImmutableList.of("foo", "bar", "baz"))
      .make

    val gStringHeader = new VCFInfoHeaderLine("STRING_G",
      VCFHeaderLineCount.G,
      VCFHeaderLineType.String,
      "Number=G Type=String")

    intercept[IllegalArgumentException] {
      val adamVc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ gStringHeader, ValidationStringency.STRICT, false)
        .convert(vc)
    }
  }

  test("VCF FORMAT attribute Number=0 Type=Flag adam->htsjdk not supported") {
    val vca = VariantCallingAnnotations.newBuilder
      .setAttributes(ImmutableMap.of("FLAG", "true"))
      .build
    val g = Genotype.newBuilder
      .setVariantCallingAnnotations(vca)
      .build

    val adamVc = ADAMVariantContext(v, Some(g))

    intercept[IllegalArgumentException] {
      val flagHeader = new VCFFormatHeaderLine("FLAG",
        0,
        VCFHeaderLineType.Flag,
        "Flag")

      val vc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ flagHeader, lenient, false)
        .convert(adamVc).orNull
    }
  }

  test("VCF FORMAT attribute Number=1 Type=Integer adam->htsjdk") {
    val vca = VariantCallingAnnotations.newBuilder
      .setAttributes(ImmutableMap.of("ONE_INT", "42"))
      .build
    val g = Genotype.newBuilder
      .setSampleId("sample")
      .setVariantCallingAnnotations(vca)
      .build

    val adamVc = ADAMVariantContext(v, Some(g))

    val oneIntHeader = new VCFFormatHeaderLine("ONE_INT",
      1,
      VCFHeaderLineType.Integer,
      "Number=1 Type=Integer")

    val vc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ oneIntHeader, lenient, false)
      .convert(adamVc).orNull

    assert(vc.hasGenotypes)
    val gt = vc.getGenotype("sample")
    assert(gt.hasExtendedAttribute("ONE_INT"))
    assert(gt.getExtendedAttribute("ONE_INT") === 42)
  }

  test("VCF FORMAT attribute Number=4 Type=Integer adam->htsjdk") {
    val vca = VariantCallingAnnotations.newBuilder
      .setAttributes(ImmutableMap.of("FOUR_INTS", "5,10,15,20"))
      .build
    val g = Genotype.newBuilder
      .setSampleId("sample")
      .setVariantCallingAnnotations(vca)
      .build

    val adamVc = ADAMVariantContext(v, Some(g))

    val fourIntsHeader = new VCFFormatHeaderLine("FOUR_INTS",
      4,
      VCFHeaderLineType.Integer,
      "Number=4 Type=Integer")

    val vc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ fourIntsHeader, lenient, false)
      .convert(adamVc).orNull

    assert(vc.hasGenotypes)
    val gt = vc.getGenotype("sample")
    assert(gt.hasExtendedAttribute("FOUR_INTS"))
    val fourInts = gt.getExtendedAttribute("FOUR_INTS").asInstanceOf[Array[java.lang.Integer]]
    assert(fourInts.size === 4)
    assert(fourInts(0) === 5)
    assert(fourInts(1) === 10)
    assert(fourInts(2) === 15)
    assert(fourInts(3) === 20)
  }

  test("VCF FORMAT attribute Number=A Type=Integer adam->htsjdk") {
    val vca = VariantCallingAnnotations.newBuilder
      .setAttributes(ImmutableMap.of("A_INT", "42"))
      .build
    val g = Genotype.newBuilder
      .setSampleId("sample")
      .setVariantCallingAnnotations(vca)
      .build

    val adamVc = ADAMVariantContext(v, Some(g))

    val aIntHeader = new VCFFormatHeaderLine("A_INT",
      VCFHeaderLineCount.A,
      VCFHeaderLineType.Integer,
      "Number=A Type=Integer")

    val vc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ aIntHeader, lenient, false)
      .convert(adamVc).orNull

    assert(vc.hasGenotypes)
    val gt = vc.getGenotype("sample")
    assert(gt.hasExtendedAttribute("A_INT"))
    val aInt = gt.getExtendedAttribute("A_INT").asInstanceOf[Array[java.lang.Integer]]
    assert(aInt.size === 1)
    assert(aInt(0) === 42)
  }

  test("VCF FORMAT attribute Number=R Type=Integer adam->htsjdk") {
    val vca = VariantCallingAnnotations.newBuilder
      .setAttributes(ImmutableMap.of("R_INT", "5,10"))
      .build
    val g = Genotype.newBuilder
      .setSampleId("sample")
      .setVariantCallingAnnotations(vca)
      .build

    val adamVc = ADAMVariantContext(v, Some(g))

    val rIntHeader = new VCFFormatHeaderLine("R_INT",
      VCFHeaderLineCount.R,
      VCFHeaderLineType.Integer,
      "Number=R Type=Integer")

    val vc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ rIntHeader, lenient, false)
      .convert(adamVc).orNull

    assert(vc.hasGenotypes)
    val gt = vc.getGenotype("sample")
    assert(gt.hasExtendedAttribute("R_INT"))
    val rInt = gt.getExtendedAttribute("R_INT").asInstanceOf[Array[java.lang.Integer]]
    assert(rInt.size === 2)
    assert(rInt(0) === 5)
    assert(rInt(1) === 10)
  }

  test("VCF FORMAT attribute Number=R Type=String adam->htsjdk") {
    val vca = VariantCallingAnnotations.newBuilder
      .setAttributes(ImmutableMap.of("R_STRING", "foo,bar"))
      .build
    val g = Genotype.newBuilder
      .setSampleId("sample")
      .setVariantCallingAnnotations(vca)
      .build

    val adamVc = ADAMVariantContext(v, Some(g))

    val rStringHeader = new VCFFormatHeaderLine("R_STRING",
      VCFHeaderLineCount.R,
      VCFHeaderLineType.String,
      "Number=R Type=String")

    val vc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ rStringHeader, lenient, false)
      .convert(adamVc).orNull

    assert(vc.hasGenotypes)
    val gt = vc.getGenotype("sample")
    assert(gt.hasExtendedAttribute("R_STRING"))
    val rString = gt.getExtendedAttribute("R_STRING").asInstanceOf[Array[String]]
    assert(rString.size === 2)
    assert(rString(0) === "foo")
    assert(rString(1) === "bar")
  }

  test("VCF FORMAT attribute Number=0 Type=Flag htsjdk->adam is not supported") {
    val vc = htsjdkSNVBuilder
      .attribute("FLAG", true)
      .make

    intercept[IllegalArgumentException] {
      val flagHeader = new VCFFormatHeaderLine("FLAG",
        0,
        VCFHeaderLineType.Flag,
        "Flag")

      val adamVc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ flagHeader, lenient, false)
        .convert(vc)
        .head

      val v = adamVc.variant.variant
      assert(v.getAnnotation.getAttributes.containsKey("FLAG"))
      assert(v.getAnnotation.getAttributes.get("FLAG") === "true")
    }
  }

  test("VCF FORMAT attribute Number=1 Type=Integer htsjdk->adam") {
    val vcb = htsjdkSNVBuilder
    val gt = GenotypeBuilder.create("NA12878",
      vcb.getAlleles(),
      Map[String, java.lang.Object](("ONE_INT", "42")))
    val vc = vcb.genotypes(gt)
      .make

    val oneIntHeader = new VCFFormatHeaderLine("ONE_INT",
      1,
      VCFHeaderLineType.Integer,
      "Number=1 Type=Integer")

    val adamVc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ oneIntHeader, lenient, false)
      .convert(vc)
      .head

    assert(adamVc.genotypes.size === 1)
    val adamGt = adamVc.genotypes.head
    assert(adamGt.getVariantCallingAnnotations.getAttributes.containsKey("ONE_INT"))
    assert(adamGt.getVariantCallingAnnotations.getAttributes.get("ONE_INT") === "42")
  }

  test("VCF FORMAT attribute Number=4 Type=Integer htsjdk->adam") {
    val vcb = htsjdkSNVBuilder
    val gt = GenotypeBuilder.create("NA12878",
      vcb.getAlleles(),
      Map[String, java.lang.Object](("FOUR_INTS", "5,10,15,20")))
    val vc = vcb.genotypes(gt)
      .make

    val fourIntsHeader = new VCFFormatHeaderLine("FOUR_INTS",
      4,
      VCFHeaderLineType.Integer,
      "Number=4 Type=Integer")

    val adamVc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ fourIntsHeader, lenient, false)
      .convert(vc)
      .head

    assert(adamVc.genotypes.size === 1)
    val adamGt = adamVc.genotypes.head
    assert(adamGt.getVariantCallingAnnotations.getAttributes.containsKey("FOUR_INTS"))
    assert(adamGt.getVariantCallingAnnotations.getAttributes.get("FOUR_INTS") === "5,10,15,20")
  }

  test("VCF FORMAT attribute Number=4 Type=Float htsjdk->adam") {
    val vcb = htsjdkSNVBuilder
    val gt = GenotypeBuilder.create("NA12878",
      vcb.getAlleles(),
      Map[String, java.lang.Object](("FOUR_FLOATS", "5.0,10.1,15.2,20.3")))
    val vc = vcb.genotypes(gt)
      .make

    val fourFloatsHeader = new VCFFormatHeaderLine("FOUR_FLOATS",
      4,
      VCFHeaderLineType.Float,
      "Number=4 Type=Float")

    val adamVc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ fourFloatsHeader, lenient, false)
      .convert(vc)
      .head

    assert(adamVc.genotypes.size === 1)
    val adamGt = adamVc.genotypes.head
    assert(adamGt.getVariantCallingAnnotations.getAttributes.containsKey("FOUR_FLOATS"))
    assert(adamGt.getVariantCallingAnnotations.getAttributes.get("FOUR_FLOATS") === "5.0,10.1,15.2,20.3")
  }

  test("VCF FORMAT attribute Number=A Type=Integer htsjdk->adam") {
    val vcb = htsjdkSNVBuilder
    val gt = GenotypeBuilder.create("NA12878",
      vcb.getAlleles(),
      Map[String, java.lang.Object](("A_INT", "10,15,20")))
    val vc = vcb.genotypes(gt)
      .make

    val aIntHeader = new VCFFormatHeaderLine("A_INT",
      VCFHeaderLineCount.A,
      VCFHeaderLineType.Integer,
      "Number=A Type=Integer")

    val adamVc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ aIntHeader, lenient, false)
      .convert(vc)
      .head

    assert(adamVc.genotypes.size === 1)
    val adamGt = adamVc.genotypes.head
    assert(adamGt.getVariantCallingAnnotations.getAttributes.containsKey("A_INT"))
    assert(adamGt.getVariantCallingAnnotations.getAttributes.get("A_INT") === "10")
  }

  test("VCF FORMAT attribute Number=R Type=Integer htsjdk->adam") {
    val vcb = htsjdkSNVBuilder
    val gt = GenotypeBuilder.create("NA12878",
      vcb.getAlleles(),
      Map[String, java.lang.Object](("R_INT", "5,10,15,20")))
    val vc = vcb.genotypes(gt)
      .make

    val rIntHeader = new VCFFormatHeaderLine("R_INT",
      VCFHeaderLineCount.R,
      VCFHeaderLineType.Integer,
      "Number=R Type=Integer")

    val adamVc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ rIntHeader, lenient, false)
      .convert(vc)
      .head

    assert(adamVc.genotypes.size === 1)
    val adamGt = adamVc.genotypes.head
    assert(adamGt.getVariantCallingAnnotations.getAttributes.containsKey("R_INT"))
    assert(adamGt.getVariantCallingAnnotations.getAttributes.get("R_INT") === "5,10")
  }

  test("VCF FORMAT attribute Number=R Type=String htsjdk->adam") {
    val vcb = htsjdkSNVBuilder
    val gt = GenotypeBuilder.create("NA12878",
      vcb.getAlleles(),
      Map[String, java.lang.Object](("R_STRING", "foo,bar,baz")))
    val vc = vcb.genotypes(gt)
      .make

    val rStringHeader = new VCFFormatHeaderLine("R_STRING",
      VCFHeaderLineCount.R,
      VCFHeaderLineType.String,
      "Number=R Type=String")

    val adamVc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ rStringHeader, lenient, false)
      .convert(vc)
      .head

    assert(adamVc.genotypes.size === 1)
    val adamGt = adamVc.genotypes.head
    assert(adamGt.getVariantCallingAnnotations.getAttributes.containsKey("R_STRING"))
    assert(adamGt.getVariantCallingAnnotations.getAttributes.get("R_STRING") === "foo,bar")
  }

  test("VCF FORMAT attribute Number=G Type=String htsjdk->adam") {
    val vcb = htsjdkSNVBuilder
    val gt = GenotypeBuilder.create("NA12878",
      vcb.getAlleles(),
      Map[String, java.lang.Object](("STRING_G", "foo,bar,baz")))
    val vc = vcb.genotypes(gt)
      .make

    val gStringHeader = new VCFFormatHeaderLine("STRING_G",
      VCFHeaderLineCount.G,
      VCFHeaderLineType.String,
      "Number=G Type=String")

    val adamVc = new VariantContextConverter(DefaultHeaderLines.allHeaderLines :+ gStringHeader, lenient, false)
      .convert(vc)
      .head

    assert(adamVc.genotypes.size === 1)
    val adamGt = adamVc.genotypes.head
    assert(adamGt.getVariantCallingAnnotations.getAttributes.containsKey("STRING_G"))
    assert(adamGt.getVariantCallingAnnotations.getAttributes.get("STRING_G") === "foo,bar,baz")
  }

  sparkTest("respect end position for symbolic alts") {
    val vcRecords = sc.loadVcf(testFile("gvcf_dir/gvcf_multiallelic.g.vcf"))
      .rdd
      .collect()

    val symbolic = vcRecords.filter(_.variant.variant.getStart == 16157520L)
      .head
    val optHtsjdkVc = converter.convert(symbolic)

    assert(optHtsjdkVc.isDefined)
    assert(optHtsjdkVc.get.getEnd === 16157602)
  }
}
