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
package edu.berkeley.cs.amplab.adam.commands

import fi.tkk.ics.hadoop.bam.{AnySAMInputFormat, SAMRecordWritable}
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Job
import parquet.hadoop.util.ContextUtil
import scala.collection.JavaConverters._
import net.sf.samtools.{SAMReadGroupRecord, SAMRecord}
import java.lang.Integer
import edu.berkeley.cs.amplab.adam.util.{Args4jBase, Args4j}
import org.kohsuke.args4j.{Argument, Option => Args4jOption}
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import spark.Partitioner
import spark.SparkContext._
import parquet.hadoop.ParquetOutputFormat
import edu.berkeley.cs.amplab.adam.models.ReferencePosition

object Bam2Adam extends AdamCommandCompanion {

  val commandName = "bam2adam"
  val commandDescription = "Convert a SAM/BAM file to ADAM read-oriented format"

  def apply(cmdLine: Array[String]) = {
    new Bam2Adam(Args4j[Bam2AdamArgs](cmdLine))
  }

}

class Bam2AdamArgs extends Args4jBase with ParquetArgs with SparkArgs {
  @Argument(required = true, metaVar = "BAM", usage = "The SAM or BAM file to convert", index = 0)
  var bamFile: String = null
  @Argument(required = true, metaVar = "ADAM", usage = "Location to write ADAM data", index = 1)
  var outputPath: String = null
  @Args4jOption(required = false, name = "-sort", usage = "Sort the reads by referenceId and read position")
  val sortReads: Boolean = false
  @Args4jOption(required = false, name = "-single_partition", usage = "Write a single partition")
  val singlePartition: Boolean = false
}

class Bam2Adam(args: Bam2AdamArgs) extends AdamCommand with SparkCommand with ParquetCommand {
  val companion = Bam2Adam

  def run() {
    val sc = createSparkContext(args)
    val job = new Job(sc.hadoopConfiguration)
    setupParquetOutputFormat(args, job, ADAMRecord.SCHEMA$)

    val samRecords = sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, AnySAMInputFormat](args.bamFile)

    val converter = new BamConverter
    val adamRecords = if (args.sortReads) {
      // Sorting reads requested
      samRecords.map {
        p =>
          val adamRecord = converter.convert(p._2.get)
          (ReferencePosition(adamRecord), adamRecord)
      }.sortByKey().map(p => (null, p._2))
    } else {
      // No sorting of reads requested
      samRecords.map(p => (null, converter.convert(p._2.get)))
    }

    if (args.singlePartition) {
      adamRecords.partitionBy(new Partitioner {

        def numPartitions: Int = 1

        def getPartition(key: Any): Int = 0

      })
    }

    adamRecords.saveAsNewAPIHadoopFile(args.outputPath,
      classOf[java.lang.Void], classOf[ADAMRecord],
      classOf[ParquetOutputFormat[ADAMRecord]], ContextUtil.getConfiguration(job))
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

    val end: Int = samRecord.getAlignmentEnd

    if (end != 0) {
      builder.setEnd((end - 1).asInstanceOf[Long])
    }

    val mapq: Int = samRecord.getMappingQuality

    if (mapq != SAMRecord.UNKNOWN_MAPPING_QUALITY) {
      builder.setMapq(mapq)
    }

    // Position of the mate/next segment
    val mateReference: Integer = samRecord.getMateReferenceIndex

    if (mateReference.toInt != -1) {
      builder
        .setMateReference(samRecord.getMateReferenceName)
        .setMateAlignmentStart(samRecord.getMateAlignmentStart.asInstanceOf[Long])
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
