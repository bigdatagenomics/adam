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

import org.bdgenomics.formats.avro.{ ADAMDatabaseVariantAnnotation, ADAMGenotype }
import org.bdgenomics.adam.converters.VariantContextConverter
import org.bdgenomics.adam.models.{ ADAMVariantContext, SequenceDictionary }
import org.bdgenomics.adam.rdd.variation.ADAMVariationContext._
import fi.tkk.ics.hadoop.bam._
import org.apache.hadoop.io.LongWritable
import org.apache.spark.{ SparkContext, Logging }
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import parquet.hadoop.util.ContextUtil
import org.bdgenomics.adam.util.HadoopUtil

object ADAMVariationContext {
  implicit def sparkContextToADAMVariationContext(sc: SparkContext): ADAMVariationContext = new ADAMVariationContext(sc)
  implicit def rddToADAMVariantContextRDD(rdd: RDD[ADAMVariantContext]) = new ADAMVariantContextRDDFunctions(rdd)
  implicit def rddToADAMGenotypeRDD(rdd: RDD[ADAMGenotype]) = new ADAMGenotypeRDDFunctions(rdd)
}

class ADAMVariationContext(sc: SparkContext) extends Serializable with Logging {
  /**
   * This method will create a new RDD of VariantContext objects
   * @param filePath: input VCF file to read
   * @return RDD of variants
   */
  def adamVCFLoad(filePath: String, dict: Option[SequenceDictionary] = None): RDD[ADAMVariantContext] = {
    log.info("Reading VCF file from %s".format(filePath))
    val job = HadoopUtil.newJob(sc)
    val vcc = new VariantContextConverter(dict)
    val records = sc.newAPIHadoopFile(
      filePath,
      classOf[VCFInputFormat], classOf[LongWritable], classOf[VariantContextWritable],
      ContextUtil.getConfiguration(job))
    log.info("Converted %d records".format(records.count()))
    records.flatMap(p => vcc.convert(p._2.get))
  }

  def adamVCFAnnotationLoad(filePath: String, dict: Option[SequenceDictionary] = None): RDD[ADAMDatabaseVariantAnnotation] = {
    log.info("Reading VCF file from %s".format(filePath))
    val job = HadoopUtil.newJob(sc)
    val vcc = new VariantContextConverter(dict)
    val records = sc.newAPIHadoopFile(
      filePath,
      classOf[VCFInputFormat], classOf[LongWritable], classOf[VariantContextWritable],
      ContextUtil.getConfiguration(job))
    log.info("Converted %d records".format(records.count()))
    records.map(p => vcc.convertToAnnotation(p._2.get))
  }

  def adamVCFSave(filePath: String, variants: RDD[ADAMVariantContext], dict: Option[SequenceDictionary] = None) = {
    val vcfFormat = VCFFormat.inferFromFilePath(filePath)
    assert(vcfFormat == VCFFormat.VCF, "BCF not yet supported") // TODO: Add BCF support

    log.info("Writing %s file to %s".format(vcfFormat, filePath))

    // Initialize global header object required by Hadoop VCF Writer
    ADAMVCFOutputFormat.setHeader(variants.adamGetCallsetSamples())

    // TODO: Sort variants according to sequence dictionary (if supplied)
    val converter = new VariantContextConverter(dict)
    val gatkVCs: RDD[VariantContextWritable] = variants.map(v => {
      val vcw = new VariantContextWritable
      vcw.set(converter.convert(v))
      vcw
    })
    val withKey = gatkVCs.keyBy(v => new LongWritable(v.get.getStart))

    val conf = sc.hadoopConfiguration
    conf.set(VCFOutputFormat.OUTPUT_VCF_FORMAT_PROPERTY, vcfFormat.toString)
    withKey.saveAsNewAPIHadoopFile(filePath,
      classOf[LongWritable], classOf[VariantContextWritable], classOf[ADAMVCFOutputFormat[LongWritable]],
      conf)

    log.info("Write %d records".format(gatkVCs.count()))
  }
}

