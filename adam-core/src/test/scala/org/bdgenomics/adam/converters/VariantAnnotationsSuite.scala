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

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.vcf.VCFConstants
import htsjdk.variant.variantcontext.VariantContext
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.Variant
import org.mockito.Mockito
import org.mockito.Mockito.when

class VariantAnnotationsSuite extends ADAMFunSuite {
  final val EMPTY = ""
  final val INVALID = "T|upstream_gene_variant|MODIFIER|TAS1R3|ENSG00000169962|transcript|ENST00000339381.5|protein_coding||c.-485C>T|||||453"
  final val VALID = "T|upstream_gene_variant|MODIFIER|TAS1R3|ENSG00000169962|transcript|ENST00000339381.5|protein_coding||c.-485C>T|||||453|"

  var variant: Variant = null
  var variantContext: VariantContext = null

  before {
    variant = Variant.newBuilder().build()
    variantContext = Mockito.mock(classOf[VariantContext])
  }

  test("parse empty transcript effect") {
    VariantAnnotations.parseTranscriptEffect(EMPTY, ValidationStringency.SILENT).isEmpty
  }

  test("parse empty transcript effect strict validation stringency") {
    intercept[IllegalArgumentException] {
      VariantAnnotations.parseTranscriptEffect(EMPTY, ValidationStringency.STRICT)
    }
  }

  test("parse invalid transcript effect") {
    VariantAnnotations.parseTranscriptEffect(INVALID, ValidationStringency.SILENT).isEmpty
  }

  test("parse invalid transcript effect strict validation stringency") {
    intercept[IllegalArgumentException] {
      VariantAnnotations.parseTranscriptEffect(INVALID, ValidationStringency.STRICT)
    }
  }

  test("parse transcript effect") {
    val te = VariantAnnotations.parseTranscriptEffect(VALID, ValidationStringency.STRICT).head

    assert(te.getAlternateAllele == "T")
    assert(te.getEffects.contains("upstream_gene_variant"))
    assert(te.getGeneName == "TAS1R3")
    assert(te.getGeneId == "ENSG00000169962")
    assert(te.getFeatureType == "transcript")
    assert(te.getFeatureId == "ENST00000339381.5")
    assert(te.getBiotype == "protein_coding")
    assert(te.getRank == null)
    assert(te.getTotal == null)
    assert(te.getTranscriptHgvs == "c.-485C>T")
    assert(te.getProteinHgvs == null)
    assert(te.getCdnaPosition == null)
    assert(te.getCdnaLength == null)
    assert(te.getCdsPosition == null)
    assert(te.getCdsLength == null)
    assert(te.getProteinPosition == null)
    assert(te.getProteinLength == null)
    assert(te.getDistance == 453)
    assert(te.getMessages.isEmpty)
  }

  test("parse empty VCF ANN attribute") {
    VariantAnnotations.parseAnn(EMPTY, ValidationStringency.SILENT).isEmpty
  }

  test("parse empty VCF ANN attribute strict validation stringency") {
    intercept[IllegalArgumentException] {
      VariantAnnotations.parseAnn(EMPTY, ValidationStringency.STRICT)
    }
  }

  test("parse invalid VCF ANN attribute") {
    VariantAnnotations.parseAnn(INVALID, ValidationStringency.SILENT).isEmpty
  }

  test("parse invalid VCF ANN attribute strict validation stringency") {
    intercept[IllegalArgumentException] {
      VariantAnnotations.parseAnn(INVALID, ValidationStringency.STRICT)
    }
  }

  test("parse VCF ANN attribute with one transcript effect") {
    val ann = VariantAnnotations.parseAnn(VALID, ValidationStringency.STRICT)
    assert(ann.length == 1)

    val te = ann.head
    assert(te.getAlternateAllele == "T")
    assert(te.getEffects.contains("upstream_gene_variant"))
    assert(te.getGeneName == "TAS1R3")
    assert(te.getGeneId == "ENSG00000169962")
    assert(te.getFeatureType == "transcript")
    assert(te.getFeatureId == "ENST00000339381.5")
    assert(te.getBiotype == "protein_coding")
    assert(te.getRank == null)
    assert(te.getTotal == null)
    assert(te.getTranscriptHgvs == "c.-485C>T")
    assert(te.getProteinHgvs == null)
    assert(te.getCdnaPosition == null)
    assert(te.getCdnaLength == null)
    assert(te.getCdsPosition == null)
    assert(te.getCdsLength == null)
    assert(te.getProteinPosition == null)
    assert(te.getProteinLength == null)
    assert(te.getDistance == 453)
    assert(te.getMessages.isEmpty)
  }

  test("create variant annotation from null VCF ANN attribute in variant context") {
    when(variantContext.getAttributeAsString("ANN", null)).thenReturn(null)

    val ann = VariantAnnotations.createVariantAnnotation(variant, variantContext)
    assert(ann.getVariant() == variant)
    assert(ann.getTranscriptEffects().isEmpty)
  }

  test("create variant annotation from missing value VCF ANN attribute in variant context") {
    when(variantContext.getAttributeAsString("ANN", null)).thenReturn(VCFConstants.MISSING_VALUE_v4)

    val ann = VariantAnnotations.createVariantAnnotation(variant, variantContext)
    assert(ann.getVariant() == variant)
    assert(ann.getTranscriptEffects().isEmpty)
  }

  test("create variant annotation from empty VCF ANN attribute in variant context") {
    when(variantContext.getAttributeAsString("ANN", null)).thenReturn(EMPTY)

    val ann = VariantAnnotations.createVariantAnnotation(variant, variantContext, ValidationStringency.SILENT)
    assert(ann.getVariant() == variant)
    assert(ann.getTranscriptEffects().isEmpty)
  }

  test("create variant annotation from empty VCF ANN attribute in variant context strict validation stringency") {
    when(variantContext.getAttributeAsString("ANN", null)).thenReturn(EMPTY)

    intercept[IllegalArgumentException] {
      VariantAnnotations.createVariantAnnotation(variant, variantContext, ValidationStringency.STRICT)
    }
  }

  test("create variant annotation from invalid VCF ANN attribute in variant context") {
    when(variantContext.getAttributeAsString("ANN", null)).thenReturn(INVALID)

    val ann = VariantAnnotations.createVariantAnnotation(variant, variantContext, ValidationStringency.SILENT)
    assert(ann.getVariant() == variant)
    assert(ann.getTranscriptEffects().isEmpty)
  }

  test("create variant annotation from invalid VCF ANN attribute in variant context strict validation stringency") {
    when(variantContext.getAttributeAsString("ANN", null)).thenReturn(INVALID)

    intercept[IllegalArgumentException] {
      VariantAnnotations.createVariantAnnotation(variant, variantContext, ValidationStringency.STRICT)
    }
  }

  test("create variant annotation from VCF ANN attribute in variant context") {
    when(variantContext.getAttributeAsString("ANN", null)).thenReturn(VALID)

    val ann = VariantAnnotations.createVariantAnnotation(variant, variantContext)
    assert(ann.getVariant() == variant)
    assert(ann.getTranscriptEffects().size == 1)

    val te = ann.getTranscriptEffects().get(0)
    assert(te.getAlternateAllele == "T")
    assert(te.getEffects.contains("upstream_gene_variant"))
    assert(te.getGeneName == "TAS1R3")
    assert(te.getGeneId == "ENSG00000169962")
    assert(te.getFeatureType == "transcript")
    assert(te.getFeatureId == "ENST00000339381.5")
    assert(te.getBiotype == "protein_coding")
    assert(te.getRank == null)
    assert(te.getTotal == null)
    assert(te.getTranscriptHgvs == "c.-485C>T")
    assert(te.getProteinHgvs == null)
    assert(te.getCdnaPosition == null)
    assert(te.getCdnaLength == null)
    assert(te.getCdsPosition == null)
    assert(te.getCdsLength == null)
    assert(te.getProteinPosition == null)
    assert(te.getProteinLength == null)
    assert(te.getDistance == 453)
    assert(te.getMessages.isEmpty)
  }

  test("create java.util.List[Int] from SB tag String value") {
    val sb_tagData = "2,3,4,5"
    val sb_converter = VariantAnnotationConverter.FORMAT_KEYS
      .filter(x => x.adamKey == "strandBiasComponents").head.attrConverter

    val sb_parsed = sb_converter(sb_tagData).asInstanceOf[java.util.List[Int]]
    val sb_component1 = sb_parsed.get(0)
    val sb_component2 = sb_parsed.get(1)
    val sb_component3 = sb_parsed.get(2)
    val sb_component4 = sb_parsed.get(3)

    assert(sb_component1 == 2 &&
      sb_component2 == 3 &&
      sb_component3 == 4 &&
      sb_component4 == 5)

  }
}
