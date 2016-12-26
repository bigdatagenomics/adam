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
package org.bdgenomics.adam.rdd.variant

import htsjdk.samtools.ValidationStringency
import htsjdk.variant.vcf.VCFHeaderLine
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.converters.SupportedHeaderLines
import org.bdgenomics.adam.models.{
  ReferencePosition,
  ReferenceRegion,
  SequenceDictionary,
  VariantContext
}
import org.bdgenomics.adam.rdd.{ JavaSaveArgs, MultisampleAvroGenomicRDD }
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.utils.cli.SaveArgs
import org.bdgenomics.formats.avro.{ Contig, Genotype, Sample }

/**
 * An RDD containing genotypes called in a set of samples against a given
 * reference genome.
 *
 * @param rdd Called genotypes.
 * @param sequences A dictionary describing the reference genome.
 * @param samples The samples called.
 * @param headerLines The VCF header lines that cover all INFO/FORMAT fields
 *   needed to represent this RDD of Genotypes.
 */
case class GenotypeRDD(rdd: RDD[Genotype],
                       sequences: SequenceDictionary,
                       @transient samples: Seq[Sample],
                       @transient headerLines: Seq[VCFHeaderLine] = SupportedHeaderLines.allHeaderLines) extends MultisampleAvroGenomicRDD[Genotype, GenotypeRDD] {

  /**
   * Java-friendly method for saving.
   *
   * @param filePath Path to save file to. If ends in ".vcf", saves as VCF, else
   *   saves as Parquet.
   */
  def save(filePath: java.lang.String) {
    save(new JavaSaveArgs(filePath))
  }

  /**
   * @return Returns this GenotypeRDD squared off as a VariantContextRDD.
   */
  def toVariantContextRDD: VariantContextRDD = {
    val vcIntRdd: RDD[(RichVariant, Genotype)] = rdd.keyBy(g => {
      RichVariant.genotypeToRichVariant(g)
    })
    val vcRdd = vcIntRdd.groupByKey
      .map {
        case (v: RichVariant, g) => {
          new VariantContext(ReferencePosition(v.variant), v, g)
        }
      }

    VariantContextRDD(vcRdd, sequences, samples, headerLines)
  }

  /**
   * Automatically detects the extension and saves to either VCF or Parquet.
   *
   * @param args Arguments configuring how to save the output.
   */
  def save(args: SaveArgs): Boolean = {
    maybeSaveVcf(args) || {
      saveAsParquet(args); true
    }
  }

  /**
   * Explicitly saves to VCF.
   *
   * @param args Arguments configuring how/where to save the output.
   * @param sortOnSave Whether to sort when saving or not.
   */
  def saveAsVcf(args: SaveArgs,
                sortOnSave: Boolean = false) {
    toVariantContextRDD.saveAsVcf(args, sortOnSave)
  }

  /**
   * If the file has a ".vcf" extension, saves to VCF.
   *
   * @param args Arguments defining how/where to save.
   * @return True if file is successfully saved as VCF.
   */
  private def maybeSaveVcf(args: SaveArgs): Boolean = {
    if (args.outputPath.endsWith(".vcf")) {
      saveAsVcf(args)
      true
    } else {
      false
    }
  }

  /**
   * Explicitly saves to VCF.
   *
   * @param filePath The filepath to save to.
   * @param asSingleFile If true, saves the output as a single file by merging
   *   the sharded output after completing the write to HDFS. If false, the
   *   output of this call will be written as shards, where each shard has a
   *   valid VCF header.
   * @param stringency The validation stringency to use when writing the VCF.
   */
  def saveAsVcf(filePath: String,
                asSingleFile: Boolean,
                stringency: ValidationStringency) {
    toVariantContextRDD.saveAsVcf(filePath, asSingleFile, stringency)
  }

  /**
   * @param newRdd An RDD to replace the underlying RDD with.
   * @return Returns a new GenotypeRDD with the underlying RDD replaced.
   */
  protected def replaceRdd(newRdd: RDD[Genotype]): GenotypeRDD = {
    copy(rdd = newRdd)
  }

  /**
   * @param elem The genotype to get a reference region for.
   * @return Returns the singular region this genotype covers.
   */
  protected def getReferenceRegions(elem: Genotype): Seq[ReferenceRegion] = {
    Seq(ReferenceRegion(elem))
  }
}
