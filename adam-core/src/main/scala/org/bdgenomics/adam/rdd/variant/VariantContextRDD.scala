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
import htsjdk.variant.vcf.{ VCFHeader, VCFHeaderLine }
import java.io.OutputStream
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.converters.{
  SupportedHeaderLines,
  VariantContextConverter
}
import org.bdgenomics.adam.models.{
  ReferencePosition,
  ReferenceRegion,
  SequenceDictionary,
  VariantContext
}
import org.bdgenomics.adam.rdd.{
  FileMerger,
  MultisampleGenomicRDD,
  VCFHeaderUtils
}
import org.bdgenomics.adam.rich.RichVariant
import org.bdgenomics.formats.avro.Sample
import org.bdgenomics.utils.misc.Logging
import org.bdgenomics.utils.cli.SaveArgs
import org.seqdoop.hadoop_bam._
import scala.collection.JavaConversions._

/**
 * An RDD containing VariantContexts attached to a reference and samples.
 *
 * @param rdd The underlying RDD of VariantContexts.
 * @param sequences The genome sequence these variants were called against.
 * @param samples The genotyped samples in this RDD of VariantContexts.
 * @param headerLines The VCF header lines that cover all INFO/FORMAT fields
 *   needed to represent this RDD of VariantContexts.
 */
case class VariantContextRDD(rdd: RDD[VariantContext],
                             sequences: SequenceDictionary,
                             @transient samples: Seq[Sample],
                             @transient headerLines: Seq[VCFHeaderLine] = SupportedHeaderLines.allHeaderLines) extends MultisampleGenomicRDD[VariantContext, VariantContextRDD]
    with Logging {

  /**
   * @return Returns a GenotypeRDD containing the Genotypes in this RDD.
   */
  def toGenotypeRDD: GenotypeRDD = {
    GenotypeRDD(rdd.flatMap(_.genotypes),
      sequences,
      samples,
      headerLines)
  }

  /**
   * @return Returns the Variants in this RDD.
   */
  def toVariantRDD: VariantRDD = {
    VariantRDD(rdd.map(_.variant.variant),
      sequences,
      headerLines)
  }

  /**
   * Converts an RDD of ADAM VariantContexts to HTSJDK VariantContexts
   * and saves to disk as VCF.
   *
   * @param filePath The filepath to save to.
   * @param sortOnSave Whether to sort before saving.
   */
  def saveAsVcf(args: SaveArgs,
                sortOnSave: Boolean) {
    saveAsVcf(args.outputPath, sortOnSave)
  }

  /**
   * Converts an RDD of ADAM VariantContexts to HTSJDK VariantContexts
   * and saves to disk as VCF.
   *
   * @param filePath The filepath to save to.
   * @param asSingleFile If true, saves the output as a single file by merging
   *   the sharded output after completing the write to HDFS. If false, the
   *   output of this call will be written as shards, where each shard has a
   *   valid VCF header. Default is false.
   * @param stringency The validation stringency to use when writing the VCF.
   */
  def saveAsVcf(filePath: String,
                asSingleFile: Boolean = false,
                stringency: ValidationStringency = ValidationStringency.LENIENT) {
    val vcfFormat = VCFFormat.inferFromFilePath(filePath)
    assert(vcfFormat == VCFFormat.VCF, "BCF not yet supported") // TODO: Add BCF support

    log.info(s"Writing $vcfFormat file to $filePath")

    // map samples to sample ids
    val sampleIds = samples.map(_.getSampleId)

    // convert the variants to htsjdk VCs
    val converter = new VariantContextConverter(headerLines, stringency)
    val writableVCs: RDD[(LongWritable, VariantContextWritable)] = rdd.flatMap(vc => {
      converter.convert(vc)
        .map(htsjdkVc => {
          val vcw = new VariantContextWritable
          vcw.set(htsjdkVc)
          (new LongWritable(vc.position.pos), vcw)
        })
    })

    // make header
    val header = new VCFHeader(
      headerLines.toSet,
      samples.map(_.getSampleId))
    header.setSequenceDictionary(sequences.toSAMSequenceDictionary)

    // write header
    val headPath = new Path("%s_head".format(filePath))

    // configure things for saving to disk
    val conf = rdd.context.hadoopConfiguration
    val fs = headPath.getFileSystem(conf)

    // write vcf header
    VCFHeaderUtils.write(header,
      headPath,
      fs)

    // set path to header file and the vcf format
    conf.set("org.bdgenomics.adam.rdd.variant.vcf_header_path", headPath.toString)
    conf.set(VCFOutputFormat.OUTPUT_VCF_FORMAT_PROPERTY, vcfFormat.toString)

    if (asSingleFile) {

      // write shards to disk
      val tailPath = "%s_tail".format(filePath)
      writableVCs.saveAsNewAPIHadoopFile(
        tailPath,
        classOf[LongWritable],
        classOf[VariantContextWritable],
        classOf[ADAMHeaderlessVCFOutputFormat[LongWritable]],
        conf
      )

      // merge shards
      FileMerger.mergeFiles(rdd.context.hadoopConfiguration,
        fs,
        new Path(filePath),
        new Path(tailPath),
        Some(headPath))
    } else {

      // write shards
      writableVCs.saveAsNewAPIHadoopFile(
        filePath,
        classOf[LongWritable],
        classOf[VariantContextWritable],
        classOf[ADAMVCFOutputFormat[LongWritable]],
        conf
      )

      // remove header file
      fs.delete(headPath, true)
    }
  }

  /**
   * @param newRdd The RDD of VariantContexts to replace the underlying RDD.
   * @return Returns a new VariantContextRDD where the underlying RDD has
   *   been replaced.
   */
  protected def replaceRdd(newRdd: RDD[VariantContext]): VariantContextRDD = {
    copy(rdd = newRdd)
  }

  /**
   * @param elem The variant context to get a reference region for.
   * @return Returns a seq containing the position key from the variant context.
   */
  protected def getReferenceRegions(elem: VariantContext): Seq[ReferenceRegion] = {
    Seq(ReferenceRegion(elem.position))
  }
}
