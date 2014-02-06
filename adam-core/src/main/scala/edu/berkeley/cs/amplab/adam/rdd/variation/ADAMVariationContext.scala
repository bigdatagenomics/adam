/*
 * Copyright (c) 2013. Mount Sinai School of Medicine
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.cs.amplab.adam.rdd.variation

import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.models.ADAMVariantContext
import org.apache.spark.{Logging, SparkContext}
import org.apache.hadoop.mapreduce.Job
import edu.berkeley.cs.amplab.adam.converters.VariantContextConverter
import fi.tkk.ics.hadoop.bam.{VariantContextWritable, VCFInputFormat}
import org.apache.hadoop.io.LongWritable
import parquet.hadoop.util.ContextUtil


object ADAMVariationContext {
  implicit def sparkContextToADAMVariationContext(sc: SparkContext): ADAMVariationContext = new ADAMVariationContext(sc)

  implicit def rddToADAMVariantContextRDD(rdd: RDD[ADAMVariantContext]) = new ADAMVariantContextRDDFunctions(rdd)
}

class ADAMVariationContext(sc: SparkContext) extends Serializable with Logging {

  /**
  * This method will create a new RDD of VariantContext objects
  * @param filePath: input VCF file to read
  * @return RDD of variants
  */
  def adamVCFLoad(filePath: String): RDD[ADAMVariantContext] = {
    log.info("Reading VCF file from %s".format(filePath))
    val job = Job.getInstance(sc.hadoopConfiguration)
    val vcc = new VariantContextConverter
    val records = sc.newAPIHadoopFile(
      filePath,
      classOf[VCFInputFormat], classOf[LongWritable], classOf[VariantContextWritable],
      ContextUtil.getConfiguration(job)
    )
    log.info("Converted %d records".format(records.count()))
    records.flatMap(p => vcc.convert(p._2.get))
  }
}


