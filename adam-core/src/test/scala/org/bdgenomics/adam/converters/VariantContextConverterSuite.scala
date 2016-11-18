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
import htsjdk.variant.utils.SAMSequenceDictionaryExtractor
import htsjdk.variant.variantcontext.{
  Allele,
  Genotype => HtsjdkGenotype,
  GenotypeBuilder,
  GenotypeType,
  VariantContextBuilder
}
import java.io.File
import org.bdgenomics.adam.models.{
  SequenceDictionary,
  VariantContext => ADAMVariantContext
}
import org.bdgenomics.adam.util.{ ADAMFunSuite, PhredUtils }
import org.bdgenomics.formats.avro._
import scala.collection.JavaConversions._

class VariantContextConverterSuite extends ADAMFunSuite {
  val dictionary = {
    val path = testFile("dict_with_accession.dict")
    SequenceDictionary(SAMSequenceDictionaryExtractor.extractDictionary(new File(path)))
  }

  val converter = new VariantContextConverter
  val adamToHtsjdkConvFn = converter.makeBdgGenotypeConverter(
    SupportedHeaderLines.allHeaderLines)
  val htsjdkToAdamConvFn = converter.makeHtsjdkGenotypeConverter(
    SupportedHeaderLines.allHeaderLines)

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
    .setContigName(contig)
    .setStart(0L)
    .setReferenceAllele("A")
    .setAlternateAllele("T")

  test("Convert htsjdk site-only SNV to ADAM") {
    val adamVCs = converter.convert(htsjdkSNVBuilder.make, htsjdkToAdamConvFn)
    assert(adamVCs.length === 1)
    val adamVC = adamVCs.head

    assert(adamVC.genotypes.size === 0)

    val variant = adamVC.variant.variant
    assert(variant.getContigName === "1")

    assert(variant.getReferenceAllele === "A")
    assert(variant.getStart === 0L)
    assert(variant.getSomatic === false)
  }

  test("Convert somatic htsjdk site-only SNV to ADAM") {
    val converter = new VariantContextConverter

    val vcb: VariantContextBuilder = new VariantContextBuilder()
      .alleles(List(Allele.create("A", true), Allele.create("T")))
      .start(1L)
      .stop(1L)
      .chr("1")
      .attribute("SOMATIC", true)

    val adamVCs = converter.convert(vcb.make, htsjdkToAdamConvFn)
    val adamVC = adamVCs.head
    val variant = adamVC.variant.variant
    assert(variant.getSomatic === true)
  }

  test("Convert htsjdk site-only CNV to ADAM") {
    val adamVCs = converter.convert(htsjdkCNVBuilder.make, htsjdkToAdamConvFn)
    assert(adamVCs.length === 1)
    val adamVC = adamVCs.head

    assert(adamVC.genotypes.size === 0)

    val variant = adamVC.variant.variant
    assert(variant.getContigName === "1")

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

    val adamVCs = converter.convert(vc, htsjdkToAdamConvFn)
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
      val adamVCs = converter.convert(vcb.make, htsjdkToAdamConvFn)
      val adamVariant = adamVCs.map(_.variant).head
      assert(adamVariant.variant.getFiltersApplied === false)
      assert(adamVariant.variant.getFiltersPassed === null)
      assert(adamVariant.variant.getFiltersFailed.isEmpty)
    }
    { // PASSing
      vcb.unfiltered.passFilters
      val adamVCs = converter.convert(vcb.make, htsjdkToAdamConvFn)
      val adamVariant = adamVCs.map(_.variant).head
      assert(adamVariant.variant.getFiltersApplied === true)
      assert(adamVariant.variant.getFiltersPassed === true)
      assert(adamVariant.variant.getFiltersFailed.isEmpty)
    }
    { // not PASSing
      vcb.unfiltered.filter("LowMQ")
      val adamVCs = converter.convert(vcb.make, htsjdkToAdamConvFn)
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
      val adamVCs = converter.convert(vcb.make, htsjdkToAdamConvFn)
      val adamGT = adamVCs.flatMap(_.genotypes).head
      // htsjdk does not distinguish between filters not applied and filters passed in Genotype
      assert(adamGT.getVariantCallingAnnotations.getFiltersApplied === true)
      assert(adamGT.getVariantCallingAnnotations.getFiltersPassed === true)
      assert(adamGT.getVariantCallingAnnotations.getFiltersFailed.isEmpty)
    }
    { // PASSing
      gb.filter("PASS")
      vcb.genotypes(gb.make)
      val adamVCs = converter.convert(vcb.make, htsjdkToAdamConvFn)
      val adamGT = adamVCs.flatMap(_.genotypes).head
      assert(adamGT.getVariantCallingAnnotations.getFiltersApplied === true)
      assert(adamGT.getVariantCallingAnnotations.getFiltersPassed === true)
      assert(adamGT.getVariantCallingAnnotations.getFiltersFailed.isEmpty)
    }
    { // not PASSing
      gb.filter("LowMQ")
      vcb.genotypes(gb.make)
      val adamVCs = converter.convert(vcb.make, htsjdkToAdamConvFn)
      val adamGT = adamVCs.flatMap(_.genotypes).head
      assert(adamGT.getVariantCallingAnnotations.getFiltersApplied === true)
      assert(adamGT.getVariantCallingAnnotations.getFiltersPassed === false)
      assert(adamGT.getVariantCallingAnnotations.getFiltersFailed.sameElements(List("LowMQ")))
    }
  }

  test("Convert ADAM site-only SNV to htsjdk") {
    val vc = ADAMVariantContext(adamSNVBuilder().build)

    val optHtsjdkVC = converter.convert(vc,
      adamToHtsjdkConvFn)

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

    val optHtsjdkVC = converter.convert(ADAMVariantContext(variant, Seq(genotype)),
      adamToHtsjdkConvFn)

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

  test("Convert htsjdk multi-allelic sites-only SNVs to ADAM") {
    val vc = htsjdkMultiAllelicSNVBuilder.make
    val adamVCs = converter.convert(vc, htsjdkToAdamConvFn)
    assert(adamVCs.length === 2)

    for ((allele, idx) <- vc.getAlternateAlleles.zipWithIndex) {
      val adamVC = adamVCs(idx)
      assert(adamVC.variant.variant.getReferenceAllele === vc.getReference.getBaseString)
      assert(adamVC.variant.variant.getAlternateAllele === allele.getBaseString)
    }
  }

  test("Convert htsjdk multi-allelic SNVs to ADAM") {
    val gb = new GenotypeBuilder("NA12878", List(Allele.create("T"), Allele.create("G")))
    gb.AD(Array(4, 2, 3)).PL(Array(59, 0, 181, 1, 66, 102))

    val vcb = htsjdkMultiAllelicSNVBuilder
    vcb.genotypes(gb.make)

    val adamVCs = converter.convert(vcb.make, htsjdkToAdamConvFn)
    assert(adamVCs.length === 2)

    for (adamVC <- adamVCs) {
      assert(adamVC.genotypes.size === 1)
      val adamGT = adamVC.genotypes.head
      assert(adamGT.getSplitFromMultiAllelic)
      assert(adamGT.getReferenceReadDepth === 4)
    }

    val adamGT1 = adamVCs(0).genotypes.head
    val adamGT2 = adamVCs(1).genotypes.head
    assert(adamGT1.getAlleles.sameElements(List(GenotypeAllele.ALT, GenotypeAllele.OTHER_ALT)))
    assert(adamGT1.getAlternateReadDepth === 2)
    assert(adamGT1.getGenotypeLikelihoods
      .map(f => f: scala.Float)
      .map(PhredUtils.logProbabilityToPhred)
      .sameElements(List(59, 0, 256)))

    assert(adamGT2.getAlleles.sameElements(List(GenotypeAllele.OTHER_ALT, GenotypeAllele.ALT)))
    assert(adamGT2.getAlternateReadDepth === 3)
    assert(adamGT2.getGenotypeLikelihoods
      .map(f => f: scala.Float)
      .map(PhredUtils.logProbabilityToPhred)
      .sameElements(List(59, 1, 102)))
  }

  test("Convert gVCF reference records to ADAM") {
    val gb = new GenotypeBuilder("NA12878", List(Allele.create("A", true), Allele.create("A", true)))
    gb.PL(Array(0, 1, 2)).DP(44).attribute("MIN_DP", 38)

    val vcb = htsjdkRefSNV
    vcb.genotypes(gb.make)

    val adamVCs = converter.convert(vcb.make, htsjdkToAdamConvFn)
    assert(adamVCs.length == 1)

    val adamGTs = adamVCs.flatMap(_.genotypes)
    assert(adamGTs.length === 1)
    val adamGT = adamGTs.head
    assert(adamGT.getVariant.getAlternateAllele === null)
    assert(adamGT.getAlleles.sameElements(List(GenotypeAllele.REF, GenotypeAllele.REF)))
    assert(adamGT.getMinReadDepth === 38)
    assert(adamGT.getGenotypeLikelihoods.isEmpty)
    assert(adamGT.getNonReferenceLikelihoods
      .map(f => f: scala.Float)
      .map(PhredUtils.logProbabilityToPhred)
      .sameElements(List(0, 1, 2)))
  }

  test("Convert htsjdk variant context with no IDs to ADAM") {
    val vcb = htsjdkSNVBuilder
    vcb.noID()

    val adamVCs = converter.convert(vcb.make, htsjdkToAdamConvFn)
    assert(adamVCs.length == 1)

    val variant = adamVCs.head.variant
    assert(variant.variant.getNames.isEmpty)
  }

  test("Convert htsjdk variant context with one ID to ADAM") {
    val vcb = htsjdkSNVBuilder
    vcb.id("rs3131972")

    val adamVCs = converter.convert(vcb.make, htsjdkToAdamConvFn)
    assert(adamVCs.length == 1)

    val variant = adamVCs.head.variant
    assert(variant.variant.getNames.length === 1)
    assert(variant.variant.getNames.get(0) === "rs3131972")
  }

  test("Convert htsjdk variant context with multiple IDs to ADAM") {
    val vcb = htsjdkSNVBuilder
    vcb.id("rs3131972;rs201888535")

    val adamVCs = converter.convert(vcb.make, htsjdkToAdamConvFn)
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

    val optHtsjdkVC = converter.convert(ADAMVariantContext(variant),
      adamToHtsjdkConvFn)
    assert(optHtsjdkVC.isDefined)
    val htsjdkVC = optHtsjdkVC.get
    assert(!htsjdkVC.hasID)
  }

  test("Convert ADAM variant context with one name to htsjdk") {
    val variant = adamSNVBuilder()
      .setNames(ImmutableList.of("rs3131972"))
      .build

    val optHtsjdkVC = converter.convert(ADAMVariantContext(variant),
      adamToHtsjdkConvFn)
    assert(optHtsjdkVC.isDefined)
    val htsjdkVC = optHtsjdkVC.get
    assert(htsjdkVC.hasID)
    assert(htsjdkVC.getID === "rs3131972")
  }

  test("Convert ADAM variant context with multiple names to htsjdk") {
    val variant = adamSNVBuilder()
      .setNames(ImmutableList.of("rs3131972", "rs201888535"))
      .build

    val optHtsjdkVC = converter.convert(ADAMVariantContext(variant),
      adamToHtsjdkConvFn)
    assert(optHtsjdkVC.isDefined)
    val htsjdkVC = optHtsjdkVC.get
    assert(htsjdkVC.hasID)
    assert(htsjdkVC.getID === "rs3131972;rs201888535")
  }

  test("Convert ADAM variant context with null filters applied to htsjdk") {
    val variant = adamSNVBuilder()
      .setFiltersApplied(null)
      .build

    val converter = new VariantContextConverter

    val optHtsjdkVC = converter.convert(ADAMVariantContext(variant),
      adamToHtsjdkConvFn)
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

    val converter = new VariantContextConverter

    val optHtsjdkVC = converter.convert(ADAMVariantContext(variant),
      adamToHtsjdkConvFn)
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

    val converter = new VariantContextConverter

    val optHtsjdkVC = converter.convert(ADAMVariantContext(variant),
      adamToHtsjdkConvFn)
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

    val converter = new VariantContextConverter

    val optHtsjdkVC = converter.convert(ADAMVariantContext(variant),
      adamToHtsjdkConvFn)
    assert(optHtsjdkVC.isDefined)
    val htsjdkVC = optHtsjdkVC.get
    assert(htsjdkVC.filtersWereApplied)
    assert(htsjdkVC.isFiltered)
    assert(htsjdkVC.getFilters.contains("FILTER1"))
    assert(htsjdkVC.getFilters.contains("FILTER2"))
  }

  test("Convert ADAM variant context with null somatic flag to htsjdk") {
    val variant = adamSNVBuilder()
      .setSomatic(null)
      .build

    val converter = new VariantContextConverter

    val optHtsjdkVC = converter.convert(ADAMVariantContext(variant),
      adamToHtsjdkConvFn)
    assert(optHtsjdkVC.isDefined)
    val htsjdkVC = optHtsjdkVC.get
    assert(!htsjdkVC.hasAttribute("SOMATIC"))
  }

  test("Convert ADAM variant context with non-somatic variant to htsjdk") {
    val variant = adamSNVBuilder()
      .setSomatic(false)
      .build

    val converter = new VariantContextConverter

    val optHtsjdkVC = converter.convert(ADAMVariantContext(variant),
      adamToHtsjdkConvFn)
    assert(optHtsjdkVC.isDefined)
    val htsjdkVC = optHtsjdkVC.get
    assert(!htsjdkVC.hasAttribute("SOMATIC"))
  }

  test("Convert ADAM variant context with somatic variant to htsjdk") {
    val variant = adamSNVBuilder()
      .setSomatic(true)
      .build

    val converter = new VariantContextConverter

    val optHtsjdkVC = converter.convert(ADAMVariantContext(variant),
      adamToHtsjdkConvFn)
    assert(optHtsjdkVC.isDefined)
    val htsjdkVC = optHtsjdkVC.get
    assert(htsjdkVC.hasAttribute("SOMATIC"))
    assert(htsjdkVC.getAttributeAsBoolean("SOMATIC", false) === true)
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

  test("no read depth going htsjdk->adam") {
    val gt = buildGt(Map.empty,
      converter.formatReadDepth,
      fns = Iterable((gb: GenotypeBuilder) => {
        gb.noDP
      }))

    assert(gt.getReadDepth === null)
  }

  test("extract read depth going htsjdk->adam") {
    val gt = buildGt(Map.empty,
      converter.formatReadDepth,
      fns = Iterable((gb: GenotypeBuilder) => {
        gb.DP(20)
      }))

    assert(gt.getReadDepth === 20)
  }

  test("no min read depth going htsjdk->adam") {
    val gt = buildGt(Map.empty,
      converter.formatMinReadDepth,
      fns = Iterable.empty)

    assert(gt.getMinReadDepth === null)
  }

  test("extract min read depth going htsjdk->adam") {
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

  test("no allelic depth going adam->htsjdk") {
    val g = converter.extractAllelicDepth(emptyGt, newGb)
      .make

    assert(!g.hasAD)
  }

  test("extract allelic depth going adam->htsjdk") {
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

  test("throw iae if missing one component of allelic depth going adam->htsjdk") {
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
      .setGenotypeLikelihoods(Seq(-0.1f, -0.001f, -0.000001f)
        .map(f => f: java.lang.Float))
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
}
