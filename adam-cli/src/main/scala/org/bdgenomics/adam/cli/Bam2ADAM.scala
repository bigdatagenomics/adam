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
package org.bdgenomics.adam.cli

import org.bdgenomics.adam.util.ParquetLogger
import org.kohsuke.args4j.{ Option => Args4jOption, Argument }
import net.sf.samtools._
import org.bdgenomics.adam.avro.ADAMRecord
import scala.collection.JavaConversions._
import java.io.File
import parquet.avro.AvroParquetWriter
import org.apache.hadoop.fs.Path
import java.util.concurrent._
import scala.Some
import java.util.logging.Level
import org.bdgenomics.adam.models.{ RecordGroupDictionary, SequenceDictionary }
import org.bdgenomics.adam.converters.SAMRecordConverter

object Bam2ADAM extends ADAMCommandCompanion {
  val commandName: String = "bam2adam"
  val commandDescription: String = "Single-node BAM to ADAM converter (Note: the 'transform' command can take SAM or BAM as input)"

  def apply(cmdLine: Array[String]): ADAMCommand = {
    new Bam2ADAM(Args4j[Bam2ADAMArgs](cmdLine))
  }
}

class Bam2ADAMArgs extends Args4jBase with ParquetArgs {
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

class Bam2ADAM(args: Bam2ADAMArgs) extends ADAMCommand {
  val companion = Bam2ADAM
  val blockingQueue = new LinkedBlockingQueue[Option[(SAMRecord, SequenceDictionary, RecordGroupDictionary)]](args.qSize)

  val writerThreads = (0 until args.numThreads).foldLeft(List[Thread]()) {
    (list, threadNum) =>
      {
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
                  case Some((samRecord, seqDict, rgDict)) =>
                    parquetWriter.write(samRecordConverter.convert(samRecord, seqDict, rgDict))
                }
              }
            } catch {
              case ie: InterruptedException =>
                Thread.interrupted()
                return
            }
          }
        })
        writerThread.setName("ADAMWriter%d".format(threadNum))
        writerThread
      } :: list
  }

  def run() = {

    val samReader = new SAMFileReader(new File(args.bamFile), null, true)
    samReader.setValidationStringency(args.validationStringency)

    val seqDict = SequenceDictionary(samReader)
    val rgDict = RecordGroupDictionary.fromSAMReader(samReader)

    println(seqDict)

    writerThreads.foreach(_.start())
    var i = 0
    for (samRecord <- samReader) {
      i += 1
      blockingQueue.put(Some((samRecord, seqDict, rgDict)))
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

