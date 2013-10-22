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
import org.apache.hadoop.io.LongWritable
import fi.tkk.ics.hadoop.bam.{AnySAMInputFormat, SAMRecordWritable}
import net.sf.samtools.{SAMReadGroupRecord, SAMRecord}
import edu.berkeley.cs.amplab.adam.avro.{ADAMPileup, ADAMRecord}
import java.lang.Integer
import scala.collection.JavaConverters._
import parquet.hadoop.ParquetInputFormat
import parquet.avro.{AvroParquetInputFormat, AvroReadSupport}
import parquet.hadoop.util.ContextUtil
import org.apache.hadoop.mapreduce.Job
import parquet.filter.UnboundRecordFilter
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import edu.berkeley.cs.amplab.adam.rich.RichAdamRecord

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
  implicit def recordToRichRecord(record: ADAMRecord): RichAdamRecord = new RichAdamRecord(record)
}

class AdamContext(sc: SparkContext) extends Serializable with Logging {

  private def adamLoadFromBAMorSAM(bamFile: String): RDD[ADAMRecord] = {
    log.info("Converting SAM/BAM file to ADAM format to create an ADAM RDD")
    val bamConverter = new BamConverter
    sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, AnySAMInputFormat](bamFile)
      .map {
      case (_: LongWritable, samRecord: SAMRecordWritable) => bamConverter.convert(samRecord.get)
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
  def adamLoad[T <% SpecificRecord : Manifest, U <: UnboundRecordFilter](filePath: String,
                                                                         predicate: Option[Class[U]] = None,
                                                                         projection: Option[Schema] = None): RDD[T] = {
    if (filePath.endsWith(".bam") || filePath.endsWith(".sam")) {
      // TODO: think about projection and predicates with BAM/SAM
      adamLoadFromBAMorSAM(filePath).asInstanceOf[RDD[T]]
    } else {
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

}

class BamConverter extends Serializable {

  def convert(samRecord: SAMRecord): ADAMRecord = {
    val builder: ADAMRecord.Builder = ADAMRecord.newBuilder
      .setReferenceName(samRecord.getReferenceName)
      .setReferenceId(samRecord.getMateReferenceIndex)
      .setReadName(samRecord.getReadName)
      .setSequence(samRecord.getReadString)
      .setQual(samRecord.getBaseQualityString)
      .setCigar(samRecord.getCigarString)

    val start: Int = samRecord.getAlignmentStart

    if (start != 0) {
      builder.setStart((start - 1).asInstanceOf[Long])
    }

    val mapq: Int = samRecord.getMappingQuality

    if (mapq != SAMRecord.UNKNOWN_MAPPING_QUALITY) {
      builder.setMapq(mapq)
    }

    // Position of the mate/next segment
    val mateReference: Integer = samRecord.getMateReferenceIndex

    if (mateReference.toInt != -1) {
      builder
        .setMateReferenceId(mateReference)
        .setMateReference(samRecord.getMateReferenceName)
      val mateStart = samRecord.getMateAlignmentStart
      if (mateStart > 0) {
        // We subtract one here to be 0-based offset
        builder.setMateAlignmentStart(mateStart - 1)
      }
    }

    // The Avro scheme defines all flags as defaulting to 'false'. We only need to set the flags that are true.
    if (samRecord.getFlags != 0) {
      if (samRecord.getReadPairedFlag) {
        builder.setReadPaired(true)
        if (samRecord.getMateNegativeStrandFlag) {
          builder.setMateNegativeStrand(true)
        }
        if (!samRecord.getMateUnmappedFlag) {
          builder.setMateMapped(true)
        }
        if (samRecord.getProperPairFlag) {
          builder.setProperPair(true)
        }
        if (samRecord.getFirstOfPairFlag) {
          builder.setFirstOfPair(true)
        }
        if (samRecord.getSecondOfPairFlag) {
          builder.setSecondOfPair(true)
        }
      }
      if (samRecord.getDuplicateReadFlag) {
        builder.setDuplicateRead(true)
      }
      if (samRecord.getReadNegativeStrandFlag) {
        builder.setReadNegativeStrand(true)
      }
      if (!samRecord.getNotPrimaryAlignmentFlag) {
        builder.setPrimaryAlignment(true)
      }
      if (samRecord.getReadFailsVendorQualityCheckFlag) {
        builder.setFailedVendorQualityChecks(true)
      }
      if (!samRecord.getReadUnmappedFlag) {
        builder.setReadMapped(true)
      }
    }

    if (samRecord.getAttributes != null) {
      var attrs = List[String]()
      samRecord.getAttributes.asScala.foreach {
        attr =>
          if (attr.tag == "MD") {
            builder.setMismatchingPositions(attr.value.toString)
          } else {
            attrs ::= attr.tag + "=" + attr.value
          }
      }
      builder.setAttributes(attrs.mkString(","))
    }

    val recordGroup: SAMReadGroupRecord = samRecord.getReadGroup
    if (recordGroup != null) {
      Option(recordGroup.getRunDate) match {
        case Some(date) => builder.setRecordGroupRunDateEpoch(date.getTime)
        case None =>
      }
      recordGroup.getId
      builder.setRecordGroupId(recordGroup.getReadGroupId)
        .setRecordGroupSequencingCenter(recordGroup.getSequencingCenter)
        .setRecordGroupDescription(recordGroup.getDescription)
        .setRecordGroupFlowOrder(recordGroup.getFlowOrder)
        .setRecordGroupKeySequence(recordGroup.getKeySequence)
        .setRecordGroupLibrary(recordGroup.getLibrary)
        .setRecordGroupPredictedMedianInsertSize(recordGroup.getPredictedMedianInsertSize)
        .setRecordGroupPlatform(recordGroup.getPlatform)
        .setRecordGroupPlatformUnit(recordGroup.getPlatformUnit)
        .setRecordGroupSample(recordGroup.getSample)
    }

    builder.build
  }

}



