/*
 * Copyright (c) 2013. Regents of the University of California
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
package edu.berkeley.cs.amplab.adam.rdd

import spark.{Logging, RDD, SparkContext}
import edu.berkeley.cs.amplab.adam.avro.{ADAMPileup, ADAMRecord}
import parquet.hadoop.ParquetInputFormat
import parquet.avro.{AvroParquetInputFormat, AvroReadSupport}
import parquet.hadoop.util.ContextUtil
import org.apache.hadoop.mapreduce.Job
import parquet.filter.UnboundRecordFilter
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord

object AdamContext {
  // Add ADAM Spark context methods
  implicit def sparkContextToAdamContext(sc: SparkContext): AdamContext = new AdamContext(sc)

  // Add methods specific to ADAMRecord RDDs
  implicit def rddToAdamRecordRDD(rdd: RDD[ADAMRecord]) = new AdamRecordRDDFunctions(rdd)

  // Add methods specific to the ADAMPileup RDDs
  implicit def rddToAdamPileupRDD(rdd: RDD[ADAMPileup]) = new AdamPileupRDDFunctions(rdd)

  // Add generic RDD methods for all types of ADAM RDDs
  implicit def rddToAdamRDD[T <% SpecificRecord : Manifest](rdd: RDD[T]) = new AdamRDDFunctions(rdd)

  // Add implicits for the rich adam objects
  implicit def recordToRichRecord(record: ADAMRecord): RichADAMRecord = new RichADAMRecord(record)
}

class AdamContext(sc: SparkContext) extends Serializable with Logging {

  /**
   * This method will create a new RDD.
   * @param filePath The path to the input data
   * @param predicate An optional pushdown predicate to use when reading the data
   * @param projection An option projection schema to use when reading the data
   * @tparam T The type of records to return
   * @return An RDD with records of the specified type
   */
  def adamLoad[T <% SpecificRecord : Manifest, U <: UnboundRecordFilter]
  (filePath: String, predicate: Option[Class[U]] = None, projection: Option[Schema] = None): RDD[T] = {
    log.info("Reading the ADAM file at %s to create RDD".format(filePath))
    val job = new Job(sc.hadoopConfiguration)
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[T]])
    if (predicate.isDefined) {
      log.info("Using the specified push-down predicate")
      ParquetInputFormat.setUnboundRecordFilter(job, predicate.get)
    }
    if (projection.isDefined) {
      log.info("Using the specified projection schema")
      AvroParquetInputFormat.setRequestedProjection(job, projection.get)
    }
    val records = sc.newAPIHadoopFile(filePath,
      classOf[ParquetInputFormat[T]], classOf[Void], manifest[T].erasure.asInstanceOf[Class[T]],
      ContextUtil.getConfiguration(job)).map(p => p._2)
    if (predicate.isDefined) {
      // Strip the nulls that the predicate returns
      records.filter(p => p != null.asInstanceOf[T])
    } else {
      records
    }
  }
}



