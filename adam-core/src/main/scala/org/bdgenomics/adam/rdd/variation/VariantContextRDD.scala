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
package org.bdgenomics.adam.rdd.variation

import org.apache.hadoop.io.LongWritable
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.converters.VariantContextConverter
import org.bdgenomics.adam.models.{
  ReferenceRegion,
  SequenceDictionary,
  VariantContext
}
import org.bdgenomics.adam.rdd.MultisampleGenomicRDD
import org.bdgenomics.utils.cli.SaveArgs
import org.seqdoop.hadoop_bam._

case class VariantContextRDD(rdd: RDD[VariantContext],
                             sequences: SequenceDictionary,
                             samples: Seq[String]) extends MultisampleGenomicRDD[VariantContext, VariantContextRDD]
    with Logging {

  def toDatabaseVariantAnnotationRDD: DatabaseVariantAnnotationRDD = {
    DatabaseVariantAnnotationRDD(rdd.flatMap(_.databases),
      sequences)
  }

  def toGenotypeRDD: GenotypeRDD = {
    GenotypeRDD(rdd.flatMap(_.genotypes),
      sequences,
      samples)
  }

  def toVariantRDD: VariantRDD = {
    VariantRDD(rdd.map(_.variant.variant),
      sequences)
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
   * @param sortOnSave Whether to sort before saving. Default is false (no sort).
   */
  def saveAsVcf(filePath: String,
                sortOnSave: Boolean = false) {
    val vcfFormat = VCFFormat.inferFromFilePath(filePath)
    assert(vcfFormat == VCFFormat.VCF, "BCF not yet supported") // TODO: Add BCF support

    rdd.cache()
    log.info(s"Writing $vcfFormat file to $filePath")

    // Initialize global header object required by Hadoop VCF Writer
    val bcastHeader = rdd.context.broadcast(samples)
    val mp = rdd.mapPartitionsWithIndex((idx, iter) => {
      log.info(s"Setting header for partition $idx")
      synchronized {
        // perform map partition call to ensure that the VCF header is set on all
        // nodes in the cluster; see:
        // https://github.com/bigdatagenomics/adam/issues/353,
        // https://github.com/bigdatagenomics/adam/issues/676
        ADAMVCFOutputFormat.clearHeader()
        ADAMVCFOutputFormat.setHeader(bcastHeader.value, sequences)
        log.info(s"Set VCF header for partition $idx")
      }
      Iterator[Int]()
    }).count()

    // force value check, ensure that computation happens
    if (mp != 0) {
      log.error("Had more than 0 elements after map partitions call to set VCF header across cluster.")
    }

    ADAMVCFOutputFormat.clearHeader()
    ADAMVCFOutputFormat.setHeader(bcastHeader.value, sequences)
    log.info("Set VCF header on driver")

    val keyByPosition = rdd.keyBy(_.position)
    val maybeSortedByKey = if (sortOnSave) {
      keyByPosition.sortByKey()
    } else {
      keyByPosition
    }

    // convert the variants to htsjdk VCs
    val converter = new VariantContextConverter(Some(sequences))
    val writableVCs: RDD[(LongWritable, VariantContextWritable)] = maybeSortedByKey.map(kv => {
      val vcw = new VariantContextWritable
      vcw.set(converter.convert(kv._2))
      (new LongWritable(kv._1.pos), vcw)
    })

    // save to disk
    val conf = rdd.context.hadoopConfiguration
    conf.set(VCFOutputFormat.OUTPUT_VCF_FORMAT_PROPERTY, vcfFormat.toString)
    writableVCs.saveAsNewAPIHadoopFile(
      filePath,
      classOf[LongWritable], classOf[VariantContextWritable], classOf[ADAMVCFOutputFormat[LongWritable]],
      conf
    )

    log.info("Write %d records".format(writableVCs.count()))
    rdd.unpersist()
  }

  protected def replaceRdd(newRdd: RDD[VariantContext]): VariantContextRDD = {
    copy(rdd = newRdd)
  }

  protected def getReferenceRegions(elem: VariantContext): Seq[ReferenceRegion] = {
    Seq(ReferenceRegion(elem.position))
  }
}
