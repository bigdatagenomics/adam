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

import htsjdk.variant.vcf.{
  VCFConstants,
  VCFFormatHeaderLine,
  VCFHeaderLineCount,
  VCFHeaderLineType,
  VCFInfoHeaderLine,
  VCFStandardHeaderLines
}

/**
 * All header lines for VCF INFO and GT format fields that are supported in ADAM.
 */
private[adam] object SupportedHeaderLines {

  lazy val ancestralAllele = new VCFInfoHeaderLine("AA",
    1,
    VCFHeaderLineType.String,
    "Ancestral allele");
  lazy val alleleCount = new VCFInfoHeaderLine("AC",
    VCFHeaderLineCount.A,
    VCFHeaderLineType.Integer,
    "Allele count");
  lazy val readDepth = new VCFInfoHeaderLine("AD",
    VCFHeaderLineCount.R,
    VCFHeaderLineType.Integer,
    "Total read depths for each allele");
  lazy val forwardReadDepth = new VCFInfoHeaderLine("ADF",
    VCFHeaderLineCount.R,
    VCFHeaderLineType.Integer,
    "Read depths for each allele on the forward strand");
  lazy val reverseReadDepth = new VCFInfoHeaderLine("ADR",
    VCFHeaderLineCount.R,
    VCFHeaderLineType.Integer,
    "Read depths for each allele on the reverse strand");
  lazy val alleleFrequency = new VCFInfoHeaderLine("AF",
    VCFHeaderLineCount.A,
    VCFHeaderLineType.Float,
    "Allele frequency for each allele");
  lazy val cigar = new VCFInfoHeaderLine("CIGAR",
    VCFHeaderLineCount.A,
    VCFHeaderLineType.String,
    "Cigar string describing how to align alternate alleles to the reference allele");
  lazy val dbSnp = new VCFInfoHeaderLine("DB",
    0,
    VCFHeaderLineType.Flag,
    "Membership in dbSNP");
  lazy val hapMap2 = new VCFInfoHeaderLine("H2",
    0,
    VCFHeaderLineType.Flag,
    "Membership in HapMap2");
  lazy val hapMap3 = new VCFInfoHeaderLine("H3",
    0,
    VCFHeaderLineType.Flag,
    "Membership in HapMap3");
  lazy val validated = new VCFInfoHeaderLine("VALIDATED",
    0,
    VCFHeaderLineType.Flag,
    "Validated by follow-up experiment");
  lazy val thousandGenomes = new VCFInfoHeaderLine("1000G",
    0,
    VCFHeaderLineType.Flag,
    "Membership in 1000 Genomes");
  lazy val somatic = new VCFInfoHeaderLine("SOMATIC",
    0,
    VCFHeaderLineType.Flag,
    "Somatic event");
  lazy val transcriptEffects = new VCFInfoHeaderLine("ANN",
    VCFHeaderLineCount.UNBOUNDED,
    VCFHeaderLineType.String,
    "Functional annotations: 'Allele | Annotation | Annotation_Impact | Gene_Name | Gene_ID | Feature_Type | Feature_ID | Transcript_BioType | Rank | HGVS.c | HGVS.p | cDNA.pos / cDNA.length | CDS.pos / CDS.length | AA.pos / AA.length | Distance | ERRORS / WARNINGS / INFO'");

  /**
   * All info keys in VCF format.
   */
  lazy val infoHeaderLines = Seq(
    ancestralAllele,
    alleleCount,
    readDepth,
    forwardReadDepth,
    reverseReadDepth,
    alleleFrequency,
    cigar,
    dbSnp,
    hapMap2,
    hapMap3,
    validated,
    thousandGenomes,
    somatic,
    transcriptEffects
  )

  lazy val genotype = VCFStandardHeaderLines.getFormatLine(
    VCFConstants.GENOTYPE_KEY)
  lazy val genotypeQuality = VCFStandardHeaderLines.getFormatLine(
    VCFConstants.GENOTYPE_QUALITY_KEY)
  lazy val allelicDepth = new VCFFormatHeaderLine("AD",
    VCFHeaderLineCount.R,
    VCFHeaderLineType.Integer,
    "Allelic depths for the ref and alt alleles in the order listed")
  lazy val formatReadDepth = VCFStandardHeaderLines.getFormatLine(
    VCFConstants.DEPTH_KEY)
  lazy val minReadDepth = new VCFFormatHeaderLine("MIN_DP",
    1,
    VCFHeaderLineType.Integer,
    "Minimum DP observed within the gVCF block")
  lazy val phredLikelihoods = VCFStandardHeaderLines.getFormatLine(
    VCFConstants.GENOTYPE_PL_KEY)
  lazy val strandBiasComponents = new VCFFormatHeaderLine("SB",
    4,
    VCFHeaderLineType.Integer,
    "Per-sample component statistics which comprise the Fisher's Exact Test to detect strand bias.")
  lazy val phaseSetId = new VCFFormatHeaderLine(VCFConstants.PHASE_SET_KEY,
    1,
    VCFHeaderLineType.Integer,
    "Phase set ID")
  lazy val phaseQuality = new VCFFormatHeaderLine(VCFConstants.PHASE_QUALITY_KEY,
    1,
    VCFHeaderLineType.Float,
    "Read-backed phasing quality")
  lazy val genotypeFilter = VCFStandardHeaderLines.getFormatLine(
    VCFConstants.GENOTYPE_FILTER_KEY)
  lazy val fisherStrand = new VCFFormatHeaderLine("FS",
    1,
    VCFHeaderLineType.Float,
    "Phred-scaled p-value using Fisher's exact test to detect strand bias")
  lazy val rmsMapq = new VCFFormatHeaderLine("MQ",
    1,
    VCFHeaderLineType.Float,
    "Root mean square (RMS) mapping quality")
  lazy val mapq0 = new VCFFormatHeaderLine("MQ0",
    1,
    VCFHeaderLineType.Float,
    "Total number of reads with mapping quality=0")

  /**
   * All format lines in VCF format.
   */
  lazy val formatHeaderLines = Seq(
    genotype,
    genotypeQuality,
    allelicDepth,
    formatReadDepth,
    minReadDepth,
    phredLikelihoods,
    strandBiasComponents,
    phaseSetId,
    phaseQuality,
    genotypeFilter,
    fisherStrand,
    rmsMapq,
    mapq0
  )

  /**
   * All supported header lines in VCF format.
   */
  lazy val allHeaderLines = infoHeaderLines ++ formatHeaderLines
}
