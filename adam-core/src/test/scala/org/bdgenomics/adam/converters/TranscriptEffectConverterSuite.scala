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
import htsjdk.samtools.ValidationStringency
import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.vcf.VCFConstants
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{
  TranscriptEffect,
  Variant
}
import org.mockito.Mockito
import org.mockito.Mockito.when

class TranscriptEffectConverterSuite extends ADAMFunSuite {
  final val EMPTY = ""
  final val INVALID = "T|upstream_gene_variant||TAS1R3|ENSG00000169962|transcript|ENST00000339381.5|protein_coding|1/2|c.-485C>T|||4|1/42|453"
  final val INVALID_NUMBER = "T|upstream_gene_variant||TAS1R3|ENSG00000169962|transcript|ENST00000339381.5|protein_coding|1/2|c.-485C>T|||4|1/42|not a number|"
  final val INVALID_FRACTION = "T|upstream_gene_variant||TAS1R3|ENSG00000169962|transcript|ENST00000339381.5|protein_coding|not a number/2|c.-485C>T|||4|1/42|453|"
  final val VALID = "T|upstream_gene_variant||TAS1R3|ENSG00000169962|transcript|ENST00000339381.5|protein_coding|1/2|c.-485C>T|||4|1/42|453|"

  var variant: Variant = null
  var variantContext: VariantContext = null

  before {
    variant = Variant.newBuilder().build()
    variantContext = Mockito.mock(classOf[VariantContext])
  }

  test("parse empty transcript effect") {
    TranscriptEffectConverter.parseTranscriptEffect(EMPTY, ValidationStringency.SILENT).isEmpty
  }

  test("parse empty transcript effect strict validation stringency") {
    intercept[IllegalArgumentException] {
      TranscriptEffectConverter.parseTranscriptEffect(EMPTY, ValidationStringency.STRICT)
    }
  }

  test("parse invalid transcript effect") {
    TranscriptEffectConverter.parseTranscriptEffect(INVALID, ValidationStringency.SILENT).isEmpty
  }

  test("parse invalid transcript effect strict validation stringency") {
    intercept[IllegalArgumentException] {
      TranscriptEffectConverter.parseTranscriptEffect(INVALID, ValidationStringency.STRICT)
    }
  }

  test("parse transcript effect") {
    val te = TranscriptEffectConverter.parseTranscriptEffect(VALID, ValidationStringency.STRICT).head

    assert(te.getAlternateAllele == "T")
    assert(te.getEffects.contains("upstream_gene_variant"))
    assert(te.getGeneName == "TAS1R3")
    assert(te.getGeneId == "ENSG00000169962")
    assert(te.getFeatureType == "transcript")
    assert(te.getFeatureId == "ENST00000339381.5")
    assert(te.getBiotype == "protein_coding")
    assert(te.getRank == 1)
    assert(te.getTotal == 2)
    assert(te.getTranscriptHgvs == "c.-485C>T")
    assert(te.getProteinHgvs == null)
    assert(te.getCdnaPosition == null)
    assert(te.getCdnaLength == null)
    assert(te.getCdsPosition == 4)
    assert(te.getCdsLength == null)
    assert(te.getProteinPosition == 1)
    assert(te.getProteinLength == 42)
    assert(te.getDistance == 453)
    assert(te.getMessages.isEmpty)
  }

  test("parse empty VCF ANN attribute") {
    TranscriptEffectConverter.parseAnn(EMPTY, ValidationStringency.SILENT).isEmpty
  }

  test("parse empty VCF ANN attribute strict validation stringency") {
    intercept[IllegalArgumentException] {
      TranscriptEffectConverter.parseAnn(EMPTY, ValidationStringency.STRICT)
    }
  }

  test("parse invalid VCF ANN attribute") {
    TranscriptEffectConverter.parseAnn(INVALID, ValidationStringency.SILENT).isEmpty
  }

  test("parse invalid VCF ANN attribute strict validation stringency") {
    intercept[IllegalArgumentException] {
      TranscriptEffectConverter.parseAnn(INVALID, ValidationStringency.STRICT)
    }
  }

  test("parse VCF ANN attribute with one transcript effect") {
    val ann = TranscriptEffectConverter.parseAnn(VALID, ValidationStringency.STRICT)
    assert(ann.length == 1)

    val te = ann.head
    assert(te.getAlternateAllele == "T")
    assert(te.getEffects.contains("upstream_gene_variant"))
    assert(te.getGeneName == "TAS1R3")
    assert(te.getGeneId == "ENSG00000169962")
    assert(te.getFeatureType == "transcript")
    assert(te.getFeatureId == "ENST00000339381.5")
    assert(te.getBiotype == "protein_coding")
    assert(te.getRank == 1)
    assert(te.getTotal == 2)
    assert(te.getTranscriptHgvs == "c.-485C>T")
    assert(te.getProteinHgvs == null)
    assert(te.getCdnaPosition == null)
    assert(te.getCdnaLength == null)
    assert(te.getCdsPosition == 4)
    assert(te.getCdsLength == null)
    assert(te.getProteinPosition == 1)
    assert(te.getProteinLength == 42)
    assert(te.getDistance == 453)
    assert(te.getMessages.isEmpty)
  }

  test("convert to transcript effect from null VCF ANN attribute in variant context") {
    when(variantContext.getAttributeAsString("ANN", null)).thenReturn(null)

    val transcriptEffects = TranscriptEffectConverter.convertToTranscriptEffects(variant, variantContext)
    assert(!transcriptEffects.isDefined)
  }

  test("convert to transcript effect from missing value VCF ANN attribute in variant context") {
    when(variantContext.getAttributeAsString("ANN", null)).thenReturn(VCFConstants.MISSING_VALUE_v4)

    val transcriptEffects = TranscriptEffectConverter.convertToTranscriptEffects(variant, variantContext)
    assert(!transcriptEffects.isDefined)
  }

  test("convert to transcript effect from empty VCF ANN attribute in variant context") {
    when(variantContext.getAttributeAsString("ANN", null)).thenReturn(EMPTY)

    val transcriptEffects = TranscriptEffectConverter.convertToTranscriptEffects(variant, variantContext, ValidationStringency.SILENT)
    assert(!transcriptEffects.isDefined)
  }

  test("convert to transcript effect from empty VCF ANN attribute in variant context strict validation stringency") {
    when(variantContext.getAttributeAsString("ANN", null)).thenReturn(EMPTY)

    intercept[IllegalArgumentException] {
      TranscriptEffectConverter.convertToTranscriptEffects(variant, variantContext, ValidationStringency.STRICT)
    }
  }

  test("convert to transcript effect from invalid VCF ANN attribute in variant context") {
    when(variantContext.getAttributeAsString("ANN", null)).thenReturn(INVALID)

    val transcriptEffects = TranscriptEffectConverter.convertToTranscriptEffects(variant, variantContext, ValidationStringency.SILENT)
    assert(!transcriptEffects.isDefined)
  }

  test("convert to transcript effect from invalid VCF ANN attribute in variant context strict validation stringency") {
    when(variantContext.getAttributeAsString("ANN", null)).thenReturn(INVALID)

    intercept[IllegalArgumentException] {
      TranscriptEffectConverter.convertToTranscriptEffects(variant, variantContext, ValidationStringency.STRICT)
    }
  }

  test("convert to transcript effect from VCF ANN attribute with invalid number in variant context lenient validation stringency") {
    when(variantContext.getAttributeAsString("ANN", null)).thenReturn(INVALID_NUMBER)

    val transcriptEffects = TranscriptEffectConverter.convertToTranscriptEffects(variant, variantContext, ValidationStringency.LENIENT)
    assert(!transcriptEffects.isDefined)
  }

  test("convert to transcript effect from VCF ANN attribute with invalid fraction in variant context lenient validation stringency") {
    when(variantContext.getAttributeAsString("ANN", null)).thenReturn(INVALID_FRACTION)

    val transcriptEffects = TranscriptEffectConverter.convertToTranscriptEffects(variant, variantContext, ValidationStringency.LENIENT)
    assert(!transcriptEffects.isDefined)
  }

  test("convert to transcript effect from VCF ANN attribute with invalid number in variant context strict validation stringency") {
    when(variantContext.getAttributeAsString("ANN", null)).thenReturn(INVALID_NUMBER)

    intercept[NumberFormatException] {
      TranscriptEffectConverter.convertToTranscriptEffects(variant, variantContext, ValidationStringency.STRICT)
    }
  }

  test("convert to transcript effect from VCF ANN attribute with invalid fraction in variant context strict validation stringency") {
    when(variantContext.getAttributeAsString("ANN", null)).thenReturn(INVALID_FRACTION)

    intercept[NumberFormatException] {
      TranscriptEffectConverter.convertToTranscriptEffects(variant, variantContext, ValidationStringency.STRICT)
    }
  }

  test("convert to transcript effect from VCF ANN attribute in variant context different alt allele") {
    variant = Variant.newBuilder()
      .setAlternateAllele("A")
      .build()

    when(variantContext.getAttributeAsString("ANN", null)).thenReturn(VALID)

    val transcriptEffects = TranscriptEffectConverter.convertToTranscriptEffects(variant, variantContext)
    assert(!transcriptEffects.isDefined)
  }

  test("convert to transcript effect from VCF ANN attribute in variant context same alt allele") {
    variant = Variant.newBuilder()
      .setAlternateAllele("T")
      .build()

    when(variantContext.getAttributeAsString("ANN", null)).thenReturn(VALID)

    val transcriptEffectsOpt = TranscriptEffectConverter.convertToTranscriptEffects(variant, variantContext)
    assert(transcriptEffectsOpt.isDefined)

    transcriptEffectsOpt.foreach(transcriptEffects => {
      val te = transcriptEffects(0)
      assert(te.getAlternateAllele == "T")
      assert(te.getEffects.contains("upstream_gene_variant"))
      assert(te.getGeneName == "TAS1R3")
      assert(te.getGeneId == "ENSG00000169962")
      assert(te.getFeatureType == "transcript")
      assert(te.getFeatureId == "ENST00000339381.5")
      assert(te.getBiotype == "protein_coding")
      assert(te.getRank == 1)
      assert(te.getTotal == 2)
      assert(te.getTranscriptHgvs == "c.-485C>T")
      assert(te.getProteinHgvs == null)
      assert(te.getCdnaPosition == null)
      assert(te.getCdnaLength == null)
      assert(te.getCdsPosition == 4)
      assert(te.getCdsLength == null)
      assert(te.getProteinPosition == 1)
      assert(te.getProteinLength == 42)
      assert(te.getDistance == 453)
      assert(te.getMessages.isEmpty)
    })
  }

  test("convert transcript effect to VCF ANN attribute value") {
    val te = TranscriptEffect.newBuilder()
      .setAlternateAllele("T")
      .setEffects(ImmutableList.of("upstream_gene_variant"))
      .setGeneName("TAS1R3")
      .setGeneId("ENSG00000169962")
      .setFeatureType("transcript")
      .setFeatureId("ENST00000339381.5")
      .setBiotype("protein_coding")
      .setTranscriptHgvs("c.-485C>T")
      .setRank(1)
      .setTotal(2)
      .setCdsPosition(4)
      .setProteinPosition(1)
      .setProteinLength(42)
      .setDistance(453)
      .build()

    assert(VALID === TranscriptEffectConverter.convertToVcfInfoAnnValue(Seq(te)))
  }

  test("convert transcript effect with null fields to VCF ANN attribute value") {
    val te = TranscriptEffect.newBuilder()
      .setAlternateAllele("T")
      .setEffects(ImmutableList.of("upstream_gene_variant"))
      .setGeneName("TAS1R3")
      .setGeneId("ENSG00000169962")
      .setFeatureType("transcript")
      .setFeatureId("ENST00000339381.5")
      .setBiotype("protein_coding")
      .setTranscriptHgvs("c.-485C>T")
      .setRank(1)
      .setTotal(2)
      .setCdnaPosition(null)
      .setCdnaLength(null)
      .setCdsPosition(4)
      .setCdsLength(null)
      .setProteinPosition(1)
      .setProteinLength(42)
      .setDistance(453)
      .build()

    assert(VALID === TranscriptEffectConverter.convertToVcfInfoAnnValue(Seq(te)))
  }

  test("convert transcript effect with incorrect fractional value to VCF ANN attribute value") {
    val te = TranscriptEffect.newBuilder()
      .setAlternateAllele("T")
      .setEffects(ImmutableList.of("upstream_gene_variant"))
      .setRank(null)
      .setTotal(2)
      .build()

    // should log warning "Incorrect fractional value ?/2, missing numerator" and set to empty string
    // when ValidationStringency is made available for --> VCF, test STRICT throws exception
    assert(!TranscriptEffectConverter.convertToVcfInfoAnnValue(Seq(te)).contains("2"))
  }
}
