package org.bdgenomics.adam.cli

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.kohsuke.args4j.Argument

import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.adam.rdd.ADAMContext._

object RangeFetch extends ADAMCommandCompanion {
  val commandName: String = "RangeFetch"
  val commandDescription: String = "Prints out the entries from an ADAM file that match the given range"

  def apply(cmdLine: Array[String]) = {
    new RangeFetch(Args4j[RangeFetchArgs](cmdLine))
  }
}

class RangeFetchArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "ADAM", usage = "The ADAM file to find entries from", index = 0)
  val input1: String = null

  @Argument(required = true, metaVar = "Chromosome", usage = "Chromosome from which to find nucleotide sequences", index = 1)
  val input2: String = null

  @Argument(required = true, metaVar = "Start", usage = "Starting location", index = 2)
  val input3: Long = -1

  @Argument(required = true, metaVar = "End", usage = "Ending location", index = 3)
  val input4: Long = -1
}

class RangeFetch(protected val args: RangeFetchArgs) extends ADAMSparkCommand[RangeFetchArgs] {

  val companion: ADAMCommandCompanion = RangeFetch

  val filterGen = (a: String, b: Long, c: Long) => (read: AlignmentRecord) => read.getContig.getContigName.toString.equals(a) && read.getStart >= b && read.getEnd <= c

  def run(sc: SparkContext, job: Job) = {

    val adamRDD: RDD[AlignmentRecord] = sc.loadAlignments(args.input1)

    val reads: RDD[AlignmentRecord] = (adamRDD).filter(filterGen(args.input2, args.input3, args.input4))

    reads.foreach(println)
  }
}
