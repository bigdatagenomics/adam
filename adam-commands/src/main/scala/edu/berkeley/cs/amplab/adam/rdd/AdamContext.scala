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

import edu.berkeley.cs.amplab.adam.avro.{ADAMPileup, ADAMRecord}
import parquet.hadoop.ParquetInputFormat
import parquet.avro.{AvroParquetInputFormat, AvroReadSupport}
import parquet.hadoop.util.ContextUtil
import org.apache.hadoop.mapreduce.Job
import parquet.filter.UnboundRecordFilter
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import fi.tkk.ics.hadoop.bam.{SAMRecordWritable, AnySAMInputFormat}
import org.apache.hadoop.io.LongWritable
import edu.berkeley.cs.amplab.adam.commands.SAMRecordConverter
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, SparkContext}
import scala.collection.JavaConversions.{asScalaBuffer=>_,_}

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
  
  // implicit java to scala type conversions
  implicit def listToJavaList [A] (list: List[A]): java.util.List[A] = seqAsJavaList (list)

  implicit def javaListToList [A] (list: java.util.List[A]): List[A] = list.toList

  implicit def intListToJavaIntegerList (list: List[Int]): java.util.List[java.lang.Integer] = {
    seqAsJavaList(list.map(i => i : java.lang.Integer))
  }
  
  implicit def charSequenceToString (cs: CharSequence): String = cs.toString

  implicit def charSequenceToList (cs: CharSequence): List[Char] = cs.toList

}

class AdamContext(sc: SparkContext) extends Serializable with Logging {

  private def adamBamLoad(filePath: String): RDD[ADAMRecord] = {
    log.info("Reading legacy BAM file format %s to create RDD".format(filePath))
    val job = new Job(sc.hadoopConfiguration)
    val records = sc.newAPIHadoopFile(filePath, classOf[AnySAMInputFormat], classOf[LongWritable],
      classOf[SAMRecordWritable], ContextUtil.getConfiguration(job))
    val samRecordConverter = new SAMRecordConverter
    records.map(p => samRecordConverter.convert(p._2.get))
  }

  private def adamParquetLoad[T <% SpecificRecord : Manifest, U <: UnboundRecordFilter]
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
    if (filePath.endsWith(".bam") || filePath.endsWith(".sam") && manifest[T].erasure.isInstanceOf[ADAMRecord]) {
      if (predicate.isDefined) {
        log.warn("Predicate is ignored when loading a BAM file")
      }
      if (projection.isDefined) {
        log.warn("Projection is ignored when loading a BAM file")
      }
      adamBamLoad(filePath).asInstanceOf[RDD[T]]
    } else {
      adamParquetLoad(filePath, predicate, projection)
    }
  }
}



