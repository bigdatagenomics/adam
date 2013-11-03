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

import edu.berkeley.cs.amplab.adam.util.{Args4j, Args4jBase}
import org.kohsuke.args4j.{Option => Args4jOption, Argument}
import net.sf.samtools._
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import java.io.File
import parquet.avro.AvroParquetWriter
import org.apache.hadoop.fs.Path
import java.util.concurrent._
import scala.Some

object Bam2Adam extends AdamCommandCompanion {
  val commandName: String = "bam2adam"
  val commandDescription: String = "Converts a local BAM file to ADAM/Parquet and writes locally or to HDFS, S3, etc"

  def apply(cmdLine: Array[String]): AdamCommand = {
    new Bam2Adam(Args4j[Bam2AdamArgs](cmdLine))
  }
}

class Bam2AdamArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "BAM", usage = "The SAM or BAM file to convert", index = 0)
  var bamFile: String = null
  @Argument(required = true, metaVar = "ADAM", usage = "Location to write ADAM data", index = 1)
  var outputPath: String = null
  @Args4jOption(required = false, name = "-samtools_validation", usage = "SAM tools validation level")
  var validationStringency = SAMFileReader.ValidationStringency.LENIENT
  @Args4jOption(required = false, name = "-num_threads", usage = "Number of threads/partitions to use (default=4)")
  var numThreads = 4
  @Args4jOption(required = false, name = "-queue_size", usage = "Queue size (default = 10,000)")
  var qSize = 10000
}

class Bam2Adam(args: Bam2AdamArgs) extends AdamCommand {
  val companion = Bam2Adam
  val blockingQueue = new ArrayBlockingQueue[Option[SAMRecord]](args.qSize)

  val writerThreads = (0 until args.numThreads).foldLeft(List[Thread]()) {
    (list, threadNum) => {
      val writerThread = new Thread(new Runnable {

        val parquetWriter = new AvroParquetWriter[ADAMRecord](
          new Path(args.outputPath + "/part%d".format(threadNum)),
          ADAMRecord.SCHEMA$, args.compressionCodec, args.blockSize, args.pageSize, !args.disableDictionary)

        def run(): Unit = {
          try {
            while (true) {
              blockingQueue.take() match {
                // Poison Pill
                case None =>
                  // Close my parquet writer
                  parquetWriter.close()
                  // Notify other threads
                  blockingQueue.add(None)
                  // Exit
                  return
                case Some(samRecord) =>
                  parquetWriter.write(convert(samRecord))
              }
            }
          } catch {
            case ie: InterruptedException =>
              Thread.interrupted()
              return
          }
        }
      })
      writerThread.setName("AdamWriter%d".format(threadNum))
      writerThread
    } :: list
  }

  def run() = {

    val samReader = new SAMFileReader(new File(args.bamFile), null, true)
    samReader.setValidationStringency(args.validationStringency)
    writerThreads.foreach(_.start())
    var i = 0
    for (samRecord <- samReader) {
      i += 1
      blockingQueue.put(Some(samRecord))
      if (i % 1000000 == 0) {
        println("***** Read %d million reads from SAM/BAM file (queue=%d) *****".format(i / 1000000, blockingQueue.size()))
      }
    }
    blockingQueue.put(None)
    samReader.close()
    println("Waiting for writers to finish")
    writerThreads.foreach(_.join())
    System.err.flush()
    System.out.flush()
    println("\nFinished! Converted %d reads total.".format(i))
  }

  def convert(samRecord: SAMRecord): ADAMRecord = {

    val builder: ADAMRecord.Builder = ADAMRecord.newBuilder
      .setReferenceName(samRecord.getReferenceName)
      .setReferenceId(samRecord.getReferenceIndex)
      .setReadName(samRecord.getReadName)
      .setSequence(samRecord.getReadString)
      .setCigar(samRecord.getCigarString)
      .setQual(samRecord.getBaseQualityString)

    val start: Int = samRecord.getAlignmentStart

    if (start != 0) {
      builder.setStart((start - 1).asInstanceOf[Long])
    }

    val mapq: Int = samRecord.getMappingQuality

    if (mapq != SAMRecord.UNKNOWN_MAPPING_QUALITY) {
      builder.setMapq(mapq)
    }

    // Position of the mate/next segment
    val mateReference: Int = samRecord.getMateReferenceIndex

    if (mateReference != SAMRecord.NO_ALIGNMENT_REFERENCE_INDEX) {
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

