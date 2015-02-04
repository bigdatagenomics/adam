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

import org.seqdoop.hadoop_bam._
import org.apache.hadoop.io.LongWritable
import org.apache.spark.{ SparkContext, Logging }
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.converters.VariantContextConverter
import org.bdgenomics.adam.models.{ VariantContext, SequenceDictionary }
import org.bdgenomics.adam.util.HadoopUtil
import org.bdgenomics.formats.avro.{ DatabaseVariantAnnotation, Genotype }
import parquet.hadoop.util.ContextUtil

private[rdd] object VariationContext extends Logging {
  /**
   * This method will create a new RDD of VariantContext objects
   * @param filePath: input VCF file to read
   * @return RDD of variants
   */
  def adamVCFLoad(filePath: String, sc: SparkContext, sd: Option[SequenceDictionary] = None): RDD[VariantContext] = {
    log.info("Reading VCF file from %s".format(filePath))
    val job = HadoopUtil.newJob(sc)
    val vcc = new VariantContextConverter(sd)
    val records = sc.newAPIHadoopFile(
      filePath,
      classOf[VCFInputFormat], classOf[LongWritable], classOf[VariantContextWritable],
      ContextUtil.getConfiguration(job))
    log.info("Converted %d records".format(records.count()))
    records.flatMap(p => vcc.convert(p._2.get))
  }

  def adamVCFAnnotationLoad(filePath: String, sc: SparkContext, sd: Option[SequenceDictionary] = None): RDD[DatabaseVariantAnnotation] = {
    log.info("Reading VCF file from %s".format(filePath))

    val job = HadoopUtil.newJob(sc)
    val vcc = new VariantContextConverter(sd)

    val records = sc.newAPIHadoopFile(
      filePath,
      classOf[VCFInputFormat], classOf[LongWritable], classOf[VariantContextWritable],
      ContextUtil.getConfiguration(job))
    log.info("Converted %d records".format(records.count()))
    records.map(p => vcc.convertToAnnotation(p._2.get))
  }
}

