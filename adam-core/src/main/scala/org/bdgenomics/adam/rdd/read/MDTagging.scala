package org.bdgenomics.adam.rdd.read

import org.bdgenomics.adam.rdd.ADAMContext._
import htsjdk.samtools.{ TextCigarCodec, ValidationStringency }
import org.apache.spark.{ Logging, SparkContext }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ShuffleRegionJoin
import org.bdgenomics.adam.util.MdTag
import org.bdgenomics.formats.avro.{ AlignmentRecord, NucleotideContigFragment }

case class MDTagging(reads: RDD[AlignmentRecord],
                     referenceFragments: RDD[NucleotideContigFragment],
                     broadcast: Boolean = false,
                     partitionSize: Long = 10000,
                     overwriteExistingTags: Boolean = false,
                     validationStringency: ValidationStringency = ValidationStringency.STRICT) extends Logging {
  @transient val sc = reads.sparkContext

  val mdTagsAdded = sc.accumulator(0L, "MDTags Added")
  val mdTagsExtant = sc.accumulator(0L, "MDTags Extant")
  val numUnmappedReads = sc.accumulator(0L, "Unmapped Reads")
  val incorrectMDTags = sc.accumulator(0L, "Incorrect Extant MDTags")

  val taggedReads =
    if (broadcast) {
      addMDTagsBroadcast
    } else {
      addMDTagsShuffle
    }

  def maybeMDTagRead(read: AlignmentRecord, refSeq: String): AlignmentRecord = {

    val cigar = TextCigarCodec.decode(read.getCigar)
    val mdTag = MdTag(read.getSequence, refSeq, cigar, read.getStart)
    //    println(s"maybeMDTagRead: ${read.getSequence}, $refSeq, $cigar, $mdTag, ${read.getMismatchingPositions}")
    if (read.getMismatchingPositions != null) {
      mdTagsExtant += 1
      if (mdTag.toString != read.getMismatchingPositions) {
        incorrectMDTags += 1
        if (overwriteExistingTags) {
          read.setMismatchingPositions(mdTag.toString)
        } else {
          val exception = IncorrectMDTagException(read, mdTag.toString)
          if (validationStringency == ValidationStringency.STRICT) {
            throw exception
          } else if (validationStringency == ValidationStringency.LENIENT) {
            log.warn(exception.getMessage)
          }
        }
      }
    } else {
      read.setMismatchingPositions(mdTag.toString)
      mdTagsAdded += 1
    }
    read
  }

  def addMDTagsBroadcast(): RDD[AlignmentRecord] = {
    val collectedRefMap =
      referenceFragments
        .groupBy(_.getContig.getContigName)
        .mapValues(_.toSeq.sortBy(_.getFragmentStartPosition))
        .collectAsMap
        .toMap

    log.info(s"Found contigs named: ${collectedRefMap.keys.mkString(", ")}")

    val refMapB = sc.broadcast(collectedRefMap)

    def getRefSeq(contigName: String, read: AlignmentRecord): String = {
      val readStart = read.getStart
      val readEnd = readStart + read.referenceLength

      val fragments =
        refMapB
          .value
          .getOrElse(
            contigName,
            throw new Exception(
              s"Contig $contigName not found in reference map with keys: ${refMapB.value.keys.mkString(", ")}"
            )
          )
          .dropWhile(f => f.getFragmentStartPosition + f.getFragmentSequence.length < readStart)
          .takeWhile(_.getFragmentStartPosition < readEnd)

      getReferenceBasesForRead(read, fragments)
    }
    reads.map(read => {
      (for {
        contig <- Option(read.getContig)
        contigName <- Option(contig.getContigName)
        if read.getReadMapped
      } yield {
        maybeMDTagRead(read, getRefSeq(contigName, read))
      }).getOrElse({
        numUnmappedReads += 1
        read
      })
    })
  }

  def addMDTagsShuffle(): RDD[AlignmentRecord] = {
    val fragsWithRegions =
      for {
        fragment <- referenceFragments
        region <- ReferenceRegion(fragment)
      } yield {
        region -> fragment
      }

    val unmappedReads = reads.filter(!_.getReadMapped)
    numUnmappedReads += unmappedReads.count

    val readsWithRegions =
      for {
        read <- reads
        region <- ReferenceRegion.opt(read)
      } yield region -> read

    //    println(s"${readsWithRegions.count} readsWithRegions")

    val sd = reads.adamGetSequenceDictionary()
    //    println(s"${sd.records.length} seqdict records")

    val readsWithFragments =
      ShuffleRegionJoin(sd, partitionSize)
        .partitionAndJoin(readsWithRegions, fragsWithRegions)
        .groupByKey
        .mapValues(_.toSeq.sortBy(_.getFragmentStartPosition))

    //    println(s"${readsWithFragments.count} readsWithFragments")
    //
    //    println("looping..")
    //    val rdd =
    //      readsWithFragments.map(p => {
    //        println(s"in: $p")
    //        val (read, fragments) = p
    //        maybeMDTagRead(read, getReferenceBasesForRead(read, fragments))
    //      }) ++ unmappedReads
    //
    //    val collected = rdd.collect
    //
    //    println(s"${readsWithFragments.count} readsWithFragments, ${collected.length}: $collected")
    //
    //    rdd
    (for {
      (read, fragments) <- readsWithFragments
    } yield {
      //          println("in loop..")
      maybeMDTagRead(read, getReferenceBasesForRead(read, fragments))
    }) ++ unmappedReads
  }

  private def getReferenceBasesForRead(read: AlignmentRecord, fragments: Seq[NucleotideContigFragment]): String = {
    //    val frags = fragments.map(clipFragment(_, read))
    //    println(s"getReferenceBasesForRead: ${read.getSequence}, frags: $frags, ${frags.mkString("")}")
    fragments.map(clipFragment(_, read)).mkString("")
  }

  private def clipFragment(fragment: NucleotideContigFragment, read: AlignmentRecord): String = {
    clipFragment(fragment, read.getStart, read.getStart + read.referenceLength)
  }
  private def clipFragment(fragment: NucleotideContigFragment, start: Long, end: Long): String = {
    val min =
      math.max(
        0L,
        start - fragment.getFragmentStartPosition
      ).toInt

    val max =
      math.min(
        fragment.getFragmentSequence.length,
        end - fragment.getFragmentStartPosition
      ).toInt

    fragment.getFragmentSequence.substring(min, max)
  }
}

object MDTagging {
  def apply(reads: RDD[AlignmentRecord],
            referenceFile: String,
            broadcast: Boolean,
            fragmentLength: Long,
            overwriteExistingTags: Boolean,
            validationStringency: ValidationStringency): RDD[AlignmentRecord] = {
    val sc = reads.sparkContext
    new MDTagging(
      reads,
      sc.loadSequence(referenceFile, fragmentLength = fragmentLength),
      broadcast = broadcast,
      partitionSize = fragmentLength,
      overwriteExistingTags,
      validationStringency
    ).taggedReads
  }
}

case class IncorrectMDTagException(read: AlignmentRecord, mdTag: String) extends Exception {
  override def getMessage: String =
    s"Read: ${read.getReadName}, pos: ${read.getContig.getContigName}:${read.getStart}, cigar: ${read.getCigar}, existing MD tag: ${read.getMismatchingPositions}, correct MD tag: $mdTag"
}
