/**
 * Copyright (c) 2013. Genome Bridge LLC.
 */
package edu.berkeley.cs.amplab.adam.commands

import edu.berkeley.cs.amplab.adam.util._
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import org.apache.spark.SparkContext._
import org.kohsuke.args4j.Argument
import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.Job
import java.util.logging.Level
import edu.berkeley.cs.amplab.adam.projections.{ADAMRecordField, Projection}
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.projections.ADAMRecordField._

import scala.collection._
import scala.Some
import edu.berkeley.cs.amplab.adam.models.{ReferencePosition, SingleReadBucket}

object CompareAdam extends AdamCommandCompanion with Serializable {
  val commandName: String = "compare"
  val commandDescription: String = "Compares two ADAM files based on read name"

  def apply(cmdLine: Array[String]): AdamCommand = {
    new CompareAdam(Args4j[CompareAdamArgs](cmdLine))
  }

  def compareADAM(sc : SparkContext, input1Path : String, input2Path : String,
                  predicateFactory : (Map[Int,Int]) => (SingleReadBucket, SingleReadBucket) => Boolean)
  : (ComparisonResult, ComparisonResult) = {

    val projection = Projection(
      referenceId,
      mateReferenceId,
      readMapped,
      mateMapped,
      readPaired,
      firstOfPair,
      primaryAlignment,
      referenceName,
      mateReference,
      start,
      mateAlignmentStart,
      cigar,
      readNegativeStrand,
      mateNegativeStrand,
      readName)

    val reads1 : RDD[ADAMRecord] = sc.adamLoad(input1Path, projection=Some(projection))
    val reads2 : RDD[ADAMRecord] = sc.adamLoad(input2Path, projection=Some(projection))

    val dict1 = sc.adamDictionaryLoad[ADAMRecord](input1Path)
    val dict2 = sc.adamDictionaryLoad[ADAMRecord](input2Path)

    val map12 : Map[Int,Int] = dict1.mapTo(dict2)

    val predicate = predicateFactory(map12)

    val named1 = reads1.adamSingleReadBuckets().keyBy(_.allReads.head.getReadName)
    val named2 = reads2.adamSingleReadBuckets().keyBy(_.allReads.head.getReadName)

    val countMatch = named1.join(named2).filter {
      case (name, (bucket1, bucket2)) => predicate(bucket1, bucket2)
    }.count()

    // Used to filter the results of an outer-join
    def noPair[U](x : U) : Boolean = {
      x match {
        case (name, (read, None)) => true
        case _ => false
      }
    }

    val numUnique1 = named1.leftOuterJoin(named2).filter(noPair).count()
    val numUnique2 = named2.leftOuterJoin(named1).filter(noPair).count()

    (ComparisonResult(named1.count(), numUnique1, countMatch),
      ComparisonResult(named2.count(), numUnique2, countMatch))
  }

  def samePairLocation(map : Map[Int,Int], read1 : ADAMRecord, read2: ADAMRecord) : Boolean =
    sameLocation(map, read1, read2) && sameMateLocation(map, read1, read2)

  def sameLocation(map : Map[Int,Int], read1 : ADAMRecord, read2 : ADAMRecord) : Boolean = {
    assert(map.contains(read1.getReferenceId),
      "referenceId %d of read %s not in map %s".format(read1.getReferenceId.toInt, read1.getReadName, map))

    map(read1.getReferenceId) == read2.getReferenceId.toInt &&
      read1.getStart == read2.getStart && read1.getReadNegativeStrand == read2.getReadNegativeStrand
  }

  def sameMateLocation(map : Map[Int,Int], read1 : ADAMRecord, read2 : ADAMRecord) : Boolean = {

    assert(map.contains(read1.getMateReferenceId),
      "mateReferenceId %d of read %s not in map %s".format(read1.getMateReferenceId.toInt, read1.getReadName, map))

    map(read1.getMateReferenceId) == read2.getMateReferenceId.toInt &&
      read1.getMateAlignmentStart == read2.getMateAlignmentStart &&
      read1.getMateNegativeStrand == read2.getMateNegativeStrand
  }

  def readLocationsMatchPredicate(map : Map[Int,Int])(bucket1 : SingleReadBucket, bucket2 : SingleReadBucket) : Boolean = {

    val (paired1, single1) = bucket1.primaryMapped.partition(_.getReadPaired)
    val (paired2, single2) = bucket2.primaryMapped.partition(_.getReadPaired)

    if(single1.size != single2.size) return false
    if(single1.size == 1 && !sameLocation(map, single1.head, single2.head)) return false

    // TODO: if there are multiple primary hits for single-ended reads?

    val (firstPairs1, secondPairs1) = paired1.partition(_.getFirstOfPair)
    val (firstPairs2, secondPairs2) = paired2.partition(_.getFirstOfPair)

    if(firstPairs1.size != firstPairs2.size) return false
    if(firstPairs1.size == 1 && !samePairLocation(map, firstPairs1.head, firstPairs2.head)) return false

    // TODO: if there are multiple primary hits for paired-end reads?

    true
  }

}

case class ComparisonResult(total : Long, unique : Long, matching : Long) {}

class CompareAdamArgs extends Args4jBase with SparkArgs with ParquetArgs with Serializable {
  @Argument(required = true, metaVar = "INPUT1", usage = "The first ADAM file to compare", index = 0)
  val input1Path: String = null

  @Argument(required = true, metaVar = "INPUT2", usage = "The second ADAM file to compare", index = 1)
  val input2Path: String = null
}

class CompareAdam(protected val args: CompareAdamArgs) extends AdamSparkCommand[CompareAdamArgs] with Serializable {
  val companion: AdamCommandCompanion = CompareAdam

  def run(sc: SparkContext, job: Job): Unit = {

    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)

    val (comp1, comp2) =
      CompareAdam.compareADAM(sc, args.input1Path, args.input2Path, CompareAdam.readLocationsMatchPredicate)

    assert(comp1.matching == comp2.matching, "Matching numbers should be identical for pairwise comparison")

    println("# Reads in INPUT1:        %d".format(comp1.total))
    println("# Reads in INPUT2:        %d".format(comp2.total))
    println("# Reads Unique to INPUT1: %d".format(comp1.unique))
    println("# Reads Unique to INPUT2: %d".format(comp2.unique))
    println("# Matching Reads:         %d".format(comp1.matching))
  }

}

