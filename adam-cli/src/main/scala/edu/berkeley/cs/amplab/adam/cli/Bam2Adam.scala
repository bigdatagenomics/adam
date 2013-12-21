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
package edu.berkeley.cs.amplab.adam.cli

import edu.berkeley.cs.amplab.adam.util.ParquetLogger
import org.kohsuke.args4j.{Option => Args4jOption, Argument}
import net.sf.samtools._
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import scala.collection.JavaConversions._
import java.io.File
import parquet.avro.AvroParquetWriter
import org.apache.hadoop.fs.Path
import java.util.concurrent._
import scala.Some
import java.util.logging.Level
import edu.berkeley.cs.amplab.adam.models.SequenceDictionary
import edu.berkeley.cs.amplab.adam.converters.SAMRecordConverter

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
  val blockingQueue = new LinkedBlockingQueue[Option[(SAMRecord, SequenceDictionary)]](args.qSize)

  val writerThreads = (0 until args.numThreads).foldLeft(List[Thread]()) {
    (list, threadNum) => {
      val writerThread = new Thread(new Runnable {

        // Quiet parquet...
        ParquetLogger.hadoopLoggerLevel(Level.SEVERE)

        val parquetWriter = new AvroParquetWriter[ADAMRecord](
          new Path(args.outputPath + "/part%d".format(threadNum)),
          ADAMRecord.SCHEMA$, args.compressionCodec, args.blockSize, args.pageSize, !args.disableDictionary)

        val samRecordConverter = new SAMRecordConverter

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
                case Some((samRecord, seqDict)) =>
                  parquetWriter.write(samRecordConverter.convert(samRecord, seqDict))
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

    val seqDict = SequenceDictionary.fromSAMReader(samReader)

    println(seqDict)

    writerThreads.foreach(_.start())
    var i = 0
    for (samRecord <- samReader) {
      i += 1
      blockingQueue.put(Some((samRecord, seqDict)))
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
}

