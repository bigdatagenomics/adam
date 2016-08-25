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
import htsjdk.samtools.SAMFileReader
import htsjdk.variant.utils.SAMSequenceDictionaryExtractor
import htsjdk.variant.variantcontext.{
  Allele,
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
import org.scalatest.FunSuite
import scala.collection.JavaConversions._

class VariantContextConverterSuite extends ADAMFunSuite {
  val dictionary = {
    val path = testFile("dict_with_accession.dict")
    SequenceDictionary(SAMSequenceDictionaryExtractor.extractDictionary(new File(path)))
  }

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
    val converter = new VariantContextConverter

    val adamVCs = converter.convert(htsjdkSNVBuilder.make)
    assert(adamVCs.length === 1)
    val adamVC = adamVCs.head

    assert(adamVC.genotypes.size === 0)

    val variant = adamVC.variant
    assert(variant.getContigName === "1")

    assert(variant.getReferenceAllele === "A")
    assert(variant.getStart === 0L)
  }

  test("Convert htsjdk site-only SNV to ADAM with contig conversion") {
    val converter = new VariantContextConverter(Some(dictionary))

    val adamVCs = converter.convert(htsjdkSNVBuilder.make)
    assert(adamVCs.length === 1)

    val adamVC = adamVCs.head
    val variant = adamVC.variant
    assert(variant.getContigName === "NC_000001.10")
  }

  test("Convert htsjdk site-only CNV to ADAM") {
    val converter = new VariantContextConverter

    val adamVCs = converter.convert(htsjdkCNVBuilder.make)
    assert(adamVCs.length === 1)
    val adamVC = adamVCs.head

    assert(adamVC.genotypes.size === 0)

    val variant = adamVC.variant
    assert(variant.getContigName === "1")

    assert(variant.getReferenceAllele === "A")
    assert(variant.getAlternateAllele === "<CN0>")
    assert(variant.getStart === 9L)
    assert(variant.getEnd === 20L)
  }

  test("Convert htsjdk SNV w/ genotypes w/ phase information to ADAM") {
    val vcb = htsjdkSNVBuilder

    val genotypeAttributes = Map[String, Object]("PQ" -> new Integer(50), "PS" -> new Integer(1))
    val vc = vcb.genotypes(GenotypeBuilder.create("NA12878", vcb.getAlleles(), genotypeAttributes)).make()

    val converter = new VariantContextConverter

    val adamVCs = converter.convert(vc)
    assert(adamVCs.length === 1)

    val adamGTs = adamVCs.flatMap(_.genotypes)
    assert(adamGTs.length === 1)
    val adamGT = adamGTs.head
    assert(adamGT.getAlleles.sameElements(List(GenotypeAllele.REF, GenotypeAllele.ALT)))
    assert(adamGT.getPhaseSetId === 1)
    assert(adamGT.getPhaseQuality === 50)
  }

  test("Convert htsjdk SNV with different filters to ADAM") {
    val vcb = htsjdkSNVBuilder
    vcb.genotypes(GenotypeBuilder.create("NA12878", vcb.getAlleles))

    val converter = new VariantContextConverter

    { // No filters
      val adamVCs = converter.convert(vcb.make)
      val adamGT = adamVCs.flatMap(_.genotypes).head
      assert(adamGT.getVariantCallingAnnotations.getVariantIsPassing === null)
    }
    { // PASSing
      vcb.unfiltered.passFilters
      val adamVCs = converter.convert(vcb.make)
      val adamGT = adamVCs.flatMap(_.genotypes).head
      assert(adamGT.getVariantCallingAnnotations.getVariantIsPassing)
    }
    { // not PASSing
      vcb.unfiltered.filter("LowMQ")
      val adamVCs = converter.convert(vcb.make)
      val adamGT = adamVCs.flatMap(_.genotypes).head
      assert(adamGT.getVariantCallingAnnotations.getVariantIsPassing === false)
      assert(adamGT.getVariantCallingAnnotations.getVariantFilters.sameElements(List("LowMQ")))
    }
  }

  test("Convert ADAM site-only SNV to htsjdk") {
    val vc = ADAMVariantContext(adamSNVBuilder().build)

    val converter = new VariantContextConverter

    val htsjdkVC = converter.convert(vc)
    assert(htsjdkVC.getContig === "1")
    assert(htsjdkVC.getStart === 1)
    assert(htsjdkVC.getEnd === 1)
    assert(htsjdkVC.getReference === Allele.create("A", true))
    assert(htsjdkVC.getAlternateAlleles.sameElements(List(Allele.create("T"))))
    assert(!htsjdkVC.hasLog10PError)
    assert(!htsjdkVC.hasID)
    assert(!htsjdkVC.filtersWereApplied)
  }

  test("Convert ADAM site-only SNV to htsjdk with contig conversion") {
    val vc = ADAMVariantContext(adamSNVBuilder("NC_000001.10").build)

    val converter = new VariantContextConverter(dict = Some(dictionary))

    val htsjdkVC = converter.convert(vc)
    assert(htsjdkVC.getContig === "1")
  }

  test("Convert ADAM SNV w/ genotypes to htsjdk") {
    val variant = adamSNVBuilder().build
    val genotype = Genotype.newBuilder
      .setVariant(variant)
      .setSampleId("NA12878")
      .setAlleles(List(GenotypeAllele.REF, GenotypeAllele.ALT))
      .setVariantCallingAnnotations(VariantCallingAnnotations.newBuilder()
        .setFisherStrandBiasPValue(3.0f)
        .setRmsMapQ(0.0f)
        .setMapq0Reads(5)
        .build)
      .build

    val converter = new VariantContextConverter

    val htsjdkVC = converter.convert(ADAMVariantContext(variant, Seq(genotype)))
    assert(htsjdkVC.getNSamples === 1)
    assert(htsjdkVC.hasGenotype("NA12878"))
    val htsjdkGT = htsjdkVC.getGenotype("NA12878")
    assert(htsjdkGT.getType === GenotypeType.HET)
    assert(htsjdkVC.hasAttribute("FS"))
    assert(htsjdkVC.hasAttribute("MQ"))
    assert(htsjdkVC.hasAttribute("MQ0"))
  }

  test("Convert htsjdk multi-allelic sites-only SNVs to ADAM") {
    val vc = htsjdkMultiAllelicSNVBuilder.make
    val converter = new VariantContextConverter

    val adamVCs = converter.convert(vc)
    assert(adamVCs.length === 2)

    for ((allele, idx) <- vc.getAlternateAlleles.zipWithIndex) {
      val adamVC = adamVCs(idx);
      assert(adamVC.variant.getReferenceAllele === vc.getReference.getBaseString)
      assert(adamVC.variant.getAlternateAllele === allele.getBaseString)
    }
  }

  test("Convert htsjdk multi-allelic SNVs to ADAM") {
    val gb = new GenotypeBuilder("NA12878", List(Allele.create("T"), Allele.create("G")))
    gb.AD(Array(4, 2, 3)).PL(Array(59, 0, 181, 1, 66, 102))

    val vcb = htsjdkMultiAllelicSNVBuilder
    vcb.genotypes(gb.make)

    val converter = new VariantContextConverter

    val adamVCs = converter.convert(vcb.make)
    assert(adamVCs.length === 2)

    for (adamVC <- adamVCs) {
      assert(adamVC.genotypes.size === 1)
      val adamGT = adamVC.genotypes.head
      assert(adamGT.getSplitFromMultiAllelic)
      assert(adamGT.getReferenceReadDepth === 4)
      assert(adamGT.getPhased)
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
      .sameElements(List(58, 0, 101)))
  }

  test("Convert gVCF reference records to ADAM") {
    val gb = new GenotypeBuilder("NA12878", List(Allele.create("A", true), Allele.create("A", true)))
    gb.PL(Array(0, 1, 2)).DP(44).attribute("MIN_DP", 38)

    val vcb = htsjdkRefSNV
    vcb.genotypes(gb.make)

    val converter = new VariantContextConverter

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
      .map(f => f: scala.Float)
      .map(PhredUtils.logProbabilityToPhred)
      .sameElements(List(0, 1, 2)))
  }

  test("Convert htsjdk variant context with no IDs to ADAM") {
    val vcb = htsjdkSNVBuilder
    vcb.noID()

    val converter = new VariantContextConverter

    val adamVCs = converter.convert(vcb.make)
    assert(adamVCs.length == 1)

    val variant = adamVCs.head.variant
    assert(variant.getNames.isEmpty)
  }

  test("Convert htsjdk variant context with one ID to ADAM") {
    val vcb = htsjdkSNVBuilder
    vcb.id("rs3131972")

    val converter = new VariantContextConverter

    val adamVCs = converter.convert(vcb.make)
    assert(adamVCs.length == 1)

    val variant = adamVCs.head.variant
    assert(variant.getNames.length === 1)
    assert(variant.getNames.get(0) === "rs3131972")
  }

  test("Convert htsjdk variant context with multiple IDs to ADAM") {
    val vcb = htsjdkSNVBuilder
    vcb.id("rs3131972;rs201888535")

    val converter = new VariantContextConverter

    val adamVCs = converter.convert(vcb.make)
    assert(adamVCs.length == 1)

    val variant = adamVCs.head.variant
    assert(variant.getNames.length === 2)
    assert(variant.getNames.get(0) === "rs3131972")
    assert(variant.getNames.get(1) === "rs201888535")
  }

  test("Convert ADAM variant context with no names to htsjdk") {
    val variant = adamSNVBuilder()
      .build

    assert(variant.getNames.isEmpty)

    val converter = new VariantContextConverter

    val htsjdkVC = converter.convert(ADAMVariantContext(variant))
    assert(!htsjdkVC.hasID)
  }

  test("Convert ADAM variant context with one name to htsjdk") {
    val variant = adamSNVBuilder()
      .setNames(ImmutableList.of("rs3131972"))
      .build

    val converter = new VariantContextConverter

    val htsjdkVC = converter.convert(ADAMVariantContext(variant))
    assert(htsjdkVC.hasID)
    assert(htsjdkVC.getID === "rs3131972")
  }

  test("Convert ADAM variant context with multiple names to htsjdk") {
    val variant = adamSNVBuilder()
      .setNames(ImmutableList.of("rs3131972", "rs201888535"))
      .build

    val converter = new VariantContextConverter

    val htsjdkVC = converter.convert(ADAMVariantContext(variant))
    assert(htsjdkVC.hasID)
    assert(htsjdkVC.getID === "rs3131972;rs201888535")
  }
}
