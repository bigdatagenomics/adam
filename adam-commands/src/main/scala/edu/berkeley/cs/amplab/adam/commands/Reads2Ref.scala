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
import spark.{RDD, SparkContext}
import spark.SparkContext._
import parquet.hadoop.util.ContextUtil
import parquet.hadoop.{ParquetOutputFormat, ParquetInputFormat}
import org.apache.hadoop.mapreduce.Job
import parquet.avro.AvroReadSupport
import edu.berkeley.cs.amplab.adam.predicates.LocusPredicate
import scala.collection.JavaConversions._
import org.kohsuke.args4j.{Option => option, Argument}
import scala.collection.immutable.StringOps

import edu.berkeley.cs.amplab.adam.avro.{Base, ADAMPileup, ADAMRecord}

object Reads2Ref extends AdamCommandCompanion {
  val commandName: String = "reads2ref"
  val commandDescription: String = "Convert an ADAM read-oriented file to an ADAM reference-oriented file"
  val CIGAR_CODEC: TextCigarCodec = TextCigarCodec.getSingleton

  def apply(cmdLine: Array[String]) = {
    new Reads2Ref(Args4j[Reads2RefArgs](cmdLine))
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

  @option(name = "-mapq", usage = "Minimal mapq value allowed for a read (default = 30)")
  var minMapq: Long = Reads2RefArgs.MIN_MAPQ_DEFAULT
}

class ReadProcessor extends Serializable {

  def readToPileups(record: ADAMRecord): List[ADAMPileup] = {
    if (record == null || record.getCigar == null || record.getMismatchingPositions == null) {
      // TODO: log this later... We can't create a pileup without the CIGAR and MD tag
      // in the future, we can also get reference information from a reference file
      return List.empty
    }

    def baseFromSequence(pos: Int): Base = {
      Base.valueOf(record.getSequence.subSequence(pos, pos + 1).toString)
    }

    def sangerScoreToInt(score: String): Int = score(0).toInt - 33

    def populatePileupFromReference(record: ADAMRecord, referencePos: Long, isReverseStrand: Boolean): ADAMPileup.Builder = {

      var reverseStrandCount = 0

      if (isReverseStrand) {
        reverseStrandCount = 1
      }

      ADAMPileup.newBuilder()
        .setReferenceName(record.getReferenceName)
        .setReferenceId(record.getReferenceId)
        .setMapQuality(record.getMapq)
        .setPosition(referencePos)
        .setRecordGroupSequencingCenter(record.getRecordGroupSequencingCenter)
        .setRecordGroupDescription(record.getRecordGroupDescription)
        .setRecordGroupRunDateEpoch(record.getRecordGroupRunDateEpoch)
        .setRecordGroupFlowOrder(record.getRecordGroupFlowOrder)
        .setRecordGroupKeySequence(record.getRecordGroupKeySequence)
        .setRecordGroupLibrary(record.getRecordGroupLibrary)
        .setRecordGroupPredictedMedianInsertSize(record.getRecordGroupPredictedMedianInsertSize)
        .setRecordGroupPlatform(record.getRecordGroupPlatform)
        .setRecordGroupPlatformUnit(record.getRecordGroupPlatformUnit)
        .setRecordGroupSample(record.getRecordGroupSample)
        .setSangerQuality(sangerScoreToInt(record.getQual.toString))
        .setNumReverseStrand(reverseStrandCount)
        .setNumSoftClipped(0)
        .setCountAtPosition(1)
    }

    var referencePos = record.getStart
    val isReverseStrand = record.getReadNegativeStrand
    var readPos = 0

    val cigar = Reads2Ref.CIGAR_CODEC.decode(record.getCigar.toString)
    val mdTag = MdTag(record.getMismatchingPositions.toString, referencePos)

    var pileupList = List[ADAMPileup]()

    cigar.getCigarElements.foreach(cigarElement =>
      cigarElement.getOperator match {

        // INSERT
        case CigarOperator.I =>
          var insertPos = 0

          for (b <- new StringOps(record.getSequence.toString.substring(readPos, readPos + cigarElement.getLength - 1))) {
            val insertBase = Base.valueOf(b.toString)

            val pileup = populatePileupFromReference(record, referencePos, isReverseStrand)
              .setReadBase(insertBase)
              .setRangeOffset(insertPos)
              .setRangeLength(cigarElement.getLength)
              .setReferenceBase(null)
              .build()
            pileupList ::= pileup
            // Consumes the read bases but NOT the reference bases
            readPos += 1
            insertPos += 1
          }
        // MATCH (sequence match or mismatch)
        case CigarOperator.M =>

          for (i <- 0 until cigarElement.getLength) {

            val readBase = if (mdTag.isMatch(referencePos)) {
              baseFromSequence(readPos)
            } else {
              mdTag.mismatchedBase(referencePos) match {
                case None => throw new IllegalArgumentException("Cigar match has no MD (mis)match @" + referencePos + " " + record.getCigar + " " + record.getMismatchingPositions) fillInStackTrace()
                case Some(read) => Base.valueOf(read.toString)
              }
            }

            // sequence match
            val pileup = populatePileupFromReference(record, referencePos, isReverseStrand)
              .setReadBase(readBase)
              .setReferenceBase(baseFromSequence(readPos))
              .build()
            pileupList ::= pileup

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
            val pileup = populatePileupFromReference(record, referencePos, isReverseStrand)
              .setReferenceBase(Base.valueOf(deletedBase.get.toString))
              .setReadBase(Base.N)
              .build()

            pileupList ::= pileup
            // Consume reference bases but not read bases
            referencePos += 1
          }

        // Soft clip
        case CigarOperator.S =>

          var clipPos = 0

          for (i <- 0 until cigarElement.getLength) {
            val readBase = baseFromSequence(readPos)

            // sequence match
            val pileup = populatePileupFromReference(record, referencePos, isReverseStrand)
              .setReadBase(readBase)
              .setNumSoftClipped(1)
              .setRangeOffset(clipPos)
              .setRangeLength(cigarElement.getLength)
              .setReferenceBase(null)
              .build()
            pileupList ::= pileup

            readPos += 1
            clipPos += 1
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

    def mapPileup(a: ADAMPileup): (Option[Long], Option[Base], Option[java.lang.Integer], Option[CharSequence]) = {
      (Option(a.getPosition), Option(a.getReadBase), Option(a.getRangeOffset), Option(a.getRecordGroupSample))
    }

    def combineEvidence(pileupGroup: List[ADAMPileup]): ADAMPileup = {

      val pileup = pileupGroup.reduce((a: ADAMPileup, b: ADAMPileup) => {
        a.setMapQuality(a.getMapQuality + b.getMapQuality)
        a.setSangerQuality(a.getSangerQuality + b.getSangerQuality)
        a.setCountAtPosition(a.getCountAtPosition + b.getCountAtPosition)
        a.setNumSoftClipped(a.getNumSoftClipped + b.getNumSoftClipped)
        a.setNumReverseStrand(a.getNumReverseStrand + b.getNumReverseStrand)

        a
      })

      val num = pileup.getCountAtPosition

      pileup.setMapQuality(pileup.getMapQuality / num)
      pileup.setSangerQuality(pileup.getSangerQuality / num)

      pileup
    }

    pileupList.groupBy(mapPileup)
      .map((kv: ((Option[Long], Option[Base], Option[java.lang.Integer], Option[CharSequence]), List[ADAMPileup])) => combineEvidence(kv._2)).toList
  }
}

class Reads2Ref(protected val args: Reads2RefArgs) extends AdamSparkCommand[Reads2RefArgs] with ParquetCommand {
  val companion = Reads2Ref

  def run(sc: SparkContext, job: Job) {
    setupParquetOutputFormat(args, job, ADAMPileup.SCHEMA$)

    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[ADAMRecord]])
    ParquetInputFormat.setUnboundRecordFilter(job, classOf[LocusPredicate])
    val reads = sc.newAPIHadoopFile(args.readInput,
      classOf[ParquetInputFormat[ADAMRecord]], classOf[Void], classOf[ADAMRecord],
      ContextUtil.getConfiguration(job))

    val nonNullReads: RDD[ADAMRecord] = reads filter (r => r._2 != null) map (r => r._2)

    val readProcessor = new ReadProcessor
    val pileups = nonNullReads.flatMap {
      readProcessor.readToPileups(_).map(p => (null, p))
    }

    pileups.saveAsNewAPIHadoopFile(args.pileupOutput,
      classOf[Void], classOf[ADAMPileup], classOf[ParquetOutputFormat[ADAMPileup]],
      ContextUtil.getConfiguration(job))
  }

  def sangerQuality(qualities: String, index: Int) {
    qualities charAt index - 33
  }


}
