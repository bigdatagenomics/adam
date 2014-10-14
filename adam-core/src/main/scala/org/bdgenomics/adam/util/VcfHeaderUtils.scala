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
package org.bdgenomics.adam.util

import org.bdgenomics.adam.models.{ VariantContext, SequenceDictionary }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variation.VariationContext._
import org.apache.spark.rdd.RDD
import htsjdk.variant.vcf.{
  VCFHeader,
  VCFHeaderLine,
  VCFInfoHeaderLine,
  VCFContigHeaderLine,
  VCFConstants,
  VCFStandardHeaderLines,
  VCFHeaderLineCount,
  VCFHeaderLineType
}

/**
 * Convenience object for building a VCF header from sequence data.
 */
object VcfHeaderUtils {

  /**
   * Builds a VCF header.
   *
   * @param seqDict Sequence dictionary describing the contigs in this callset.
   * @param samples List of samples in this callset.
   * @return A complete VCF header.
   */
  def makeHeader(seqDict: SequenceDictionary, samples: List[String]): VCFHeader = {
    val builder = new VcfHeaderBuilder(samples)

    builder.addContigLines(seqDict)

    builder.build()
  }

  /**
   * Builds a VCF header.
   *
   * @param rdd An RDD of ADAM variant contexts.
   * @return A complete VCF header.
   */
  def makeHeader(rdd: RDD[VariantContext]): VCFHeader = {
    val sequenceDict = rdd.adamGetSequenceDictionary()
    val samples = rdd.getCallsetSamples()

    makeHeader(sequenceDict, samples)
  }

}

private[util] class VcfHeaderBuilder(samples: List[String]) {

  var contigLines = List.empty[VCFContigHeaderLine]

  val formatLines: java.util.Set[VCFHeaderLine] = new java.util.HashSet[VCFHeaderLine]()
  val infoLines: java.util.Set[VCFHeaderLine] = new java.util.HashSet[VCFHeaderLine]()
  val otherLines: Set[VCFHeaderLine] = Set(new VCFInfoHeaderLine(VCFConstants.RMS_BASE_QUALITY_KEY,
    1,
    VCFHeaderLineType.Float,
    "RMS Base Quality"),
    new VCFInfoHeaderLine(VCFConstants.SAMPLE_NUMBER_KEY,
      VCFHeaderLineCount.INTEGER,
      VCFHeaderLineType.Integer,
      "RMS Mapping Quality"))

  /**
   * Creates VCF contig lines from a sequence dictionary.
   *
   * @param dict Sequence dictionary containing contig info.
   */
  def addContigLines(dict: SequenceDictionary) {
    val contigs: List[VCFContigHeaderLine] = dict.records.zipWithIndex.map {
      case (record, index) => new VCFContigHeaderLine(Map("ID" -> record.name), index + 1)
    }.toList
    contigLines = contigs ::: contigLines
  }

  /**
   * Adds standard VCF header lines to header.
   */
  private def addStandardLines() {
    val formatKeys = List(VCFConstants.GENOTYPE_KEY,
      VCFConstants.GENOTYPE_QUALITY_KEY,
      VCFConstants.GENOTYPE_PL_KEY)
    val infoKeys = List(VCFConstants.ALLELE_FREQUENCY_KEY,
      VCFConstants.ALLELE_COUNT_KEY,
      VCFConstants.ALLELE_NUMBER_KEY,
      VCFConstants.STRAND_BIAS_KEY,
      VCFConstants.RMS_MAPPING_QUALITY_KEY,
      VCFConstants.MAPPING_QUALITY_ZERO_KEY,
      VCFConstants.DEPTH_KEY)

    VCFStandardHeaderLines.addStandardFormatLines(formatLines, false, formatKeys)
    VCFStandardHeaderLines.addStandardInfoLines(infoLines, false, infoKeys)
  }

  /**
   * Given current information, builds header.
   *
   * @return Complete VCF header with sample information.
   */
  def build(): VCFHeader = {
    addStandardLines()

    val stdFmtLines: Set[VCFHeaderLine] = formatLines
    val stdInfLines: Set[VCFHeaderLine] = infoLines
    val lines: Set[VCFHeaderLine] = contigLines.toSet ++ stdFmtLines ++ stdInfLines ++ otherLines

    new VCFHeader(lines, samples)
  }

}
