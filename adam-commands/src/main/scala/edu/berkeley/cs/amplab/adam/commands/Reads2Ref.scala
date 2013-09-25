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

import edu.berkeley.cs.amplab.adam.util._
import net.sf.samtools.{CigarOperator, TextCigarCodec}
import spark.{Partitioner, RDD, SparkContext}
import spark.SparkContext._
import parquet.hadoop.util.ContextUtil
import parquet.hadoop.{ParquetOutputFormat, ParquetInputFormat}
import edu.berkeley.cs.amplab.adam.avro.{Base, ADAMPileupEvent, ADAMPileup, ADAMRecord}
import org.apache.hadoop.mapreduce.Job
import parquet.avro.AvroReadSupport
import edu.berkeley.cs.amplab.adam.predicates.LocusPredicate
import scala.collection.JavaConversions._
import org.kohsuke.args4j.{Option, Argument}

import edu.berkeley.cs.amplab.adam.avro.AvroWrapper._
import edu.berkeley.cs.amplab.adam.avro.AvroWrapper

object Reads2Ref {
  val CIGAR_CODEC: TextCigarCodec = TextCigarCodec.getSingleton

  def main(cmdLine: Array[String]) {
    new Reads2Ref().commandExec(cmdLine)
  }
}

object Reads2RefArgs {
  val MIN_MAPQ_DEFAULT: Long = 30L
}

class Reads2RefArgs extends Args4jBase with ParquetArgs with SparkArgs {
  @Argument(metaVar = "ADAMREADS", required = true, usage = "ADAM read-oriented data", index = 0)
  var readInput: String = _

  @Argument(metaVar = "DIR", required = true, usage = "Location to create reference-oriented ADAM data", index = 1)
  var pileupOutput: String = _

  @Option(name = "-mapq", usage = "Minimal mapq value allowed for a read (default = 30)")
  var minMapq: Long = Reads2RefArgs.MIN_MAPQ_DEFAULT
}

class Reads2Ref extends AdamCommand with SparkCommand with ParquetCommand with Serializable {
  val commandName: String = "reads2ref"
  val commandDescription: String = "Convert an ADAM read-oriented file to an ADAM reference-oriented file"

  def commandExec(cmdLine: Array[String]) {
    val args = Args4j[Reads2RefArgs](cmdLine)
    val sc: SparkContext = createSparkContext(args)
    val job = new Job()
    setupParquetOutputFormat(args, job, ADAMPileup.SCHEMA$)

    AvroWrapper.register(classOf[ADAMRecord])
    AvroWrapper.register(classOf[ADAMPileup])

    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[ADAMRecord]])
    ParquetInputFormat.setUnboundRecordFilter(job, classOf[LocusPredicate])
    val reads = sc.newAPIHadoopFile(args.readInput,
      classOf[ParquetInputFormat[ADAMRecord]], classOf[Void], classOf[ADAMRecord],
      ContextUtil.getConfiguration(job))

    val nonNullReads: RDD[AvroWrapper[ADAMRecord]] = reads filter (r => r._2 != null) map (r => r._2)

    val pileups = nonNullReads.flatMap {
      processRecord(_).map(p => (null, AvroWrapper(p)))
    }

    pileups.saveAsNewAPIHadoopFile(args.pileupOutput,
      classOf[Void], classOf[ADAMPileup], classOf[ParquetOutputFormat[ADAMPileup]],
      ContextUtil.getConfiguration(job))
  }

  def sangerQuality(qualities: String, index: Int) {
    qualities charAt index - 33
  }

  def processRecord(record: ADAMRecord): List[ADAMPileup] = {
    if (record == null || record.getCigar == null || record.getMismatchingPositions == null) {
      // TODO: log this later... We can't create a pileup without the CIGAR and MD tag
      // in the future, we can also get reference information from a reference file
      return List.empty
    }

    def baseFromSequence(pos: Int): Base = {
      Base.valueOf(record.getSequence.subSequence(pos, pos + 1).toString)
    }

    var referencePos = record.getStart
    var isReverseStrand = record.getReadNegativeStrand
    var readPos = 0

    val cigar = Reads2Ref.CIGAR_CODEC.decode(record.getCigar.toString)
    val mdTag = MdTag(record.getMismatchingPositions.toString, referencePos)

    var pileupList = List[ADAMPileup]()

    cigar.getCigarElements.foreach(cigarElement =>
      cigarElement.getOperator match {

        // INSERT
        case CigarOperator.I =>
          val pileup = ADAMPileup.newBuilder()
            .setReferenceName(record.getReferenceName)
            .setReferenceId(record.getReferenceId)
            .setMapQuality(record.getMapq)
            .setPosition(referencePos)
            .setInsertedSequence(record.getSequence)
            .setEvent(ADAMPileupEvent.INSERTION)
            .setEventOffset(0)
            .setEventLength(cigarElement.getLength)
            .setReferenceBase(null)
            .setSangerQuality(42) // TODO: we need to send all qualities
            .build()
          pileupList ::= pileup
          // Consumes the read bases but NOT the reference bases
          readPos += cigarElement.getLength

        // MATCH (sequence match or mismatch)
        case CigarOperator.M =>

          for (i <- 0 until cigarElement.getLength) {

            if (mdTag.isMatch(referencePos)) {
              // sequence match
              val mdTagEvent = if (isReverseStrand) ADAMPileupEvent.MATCH_REVERSE_STRAND else ADAMPileupEvent.MATCH
              val pileup = ADAMPileup.newBuilder()
                .setReadName(record.getReadName)
                .setReferenceName(record.getReferenceName)
                .setReferenceId(record.getReferenceId)
                .setEvent(mdTagEvent)
                .setEventOffset(i)
                .setEventLength(cigarElement.getLength)
                .setPosition(referencePos)
                .setReferenceBase(baseFromSequence(readPos))
                .build()
              pileupList ::= pileup

            } else {
              val mismatchBase = mdTag.mismatchedBase(referencePos)
              if (mismatchBase.isEmpty) {
                throw new IllegalArgumentException("Cigar match has no MD (mis)match @" + referencePos + " " + record.getCigar + " " + record.getMismatchingPositions)
              }
              // sequence mismatch
              val mdTagEvent = if (isReverseStrand) ADAMPileupEvent.MISMATCH_REVERSE_STRAND else ADAMPileupEvent.MISMATCH
              val pileup = ADAMPileup.newBuilder()
                .setReadName(record.getReadName)
                .setReferenceName(record.getReferenceName)
                .setReferenceId(record.getReferenceId)
                .setEvent(mdTagEvent)
                .setEventOffset(i)
                .setEventLength(cigarElement.getLength)
                .setPosition(referencePos)
                .setReferenceBase(Base.valueOf(mismatchBase.get.toString))
                .setReadBase(baseFromSequence(readPos))
                .build()

              pileupList ::= pileup

            }

            readPos += 1
            referencePos += 1
          }

        // DELETE
        case CigarOperator.D =>
          for (i <- 0 until cigarElement.getLength) {
            val deletedBase = mdTag.deletedBase(referencePos)
            if (deletedBase.isEmpty) {
              throw new IllegalArgumentException("CIGAR delete but the MD tag is not a delete")
            }
            val pileup = ADAMPileup.newBuilder()
              .setReadName(record.getReadName)
              .setReferenceName(record.getReferenceName)
              .setReferenceId(record.getReferenceId)
              .setPosition(referencePos)
              .setEvent(ADAMPileupEvent.DELETION)
              .setEventOffset(i)
              .setEventLength(cigarElement.getLength)
              .setReferenceBase(Base.valueOf(deletedBase.get.toString))
              .build()

            pileupList ::= pileup
            // Consume reference bases but not read bases
            referencePos += 1
          }

        // All other cases (TODO: add X and EQ?)
        case _ =>
          if (cigarElement.getOperator.consumesReadBases()) {
            readPos += cigarElement.getLength
          }
          if (cigarElement.getOperator.consumesReferenceBases()) {
            referencePos += cigarElement.getLength
          }
      }
    )

    pileupList
  }
}
