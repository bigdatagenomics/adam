/**
 * Copyright (c) 2013 Genome Bridge LLC
 */
package edu.berkeley.cs.amplab.adam.commands

import edu.berkeley.cs.amplab.adam.util.{ParquetLogger, Args4jBase, Args4j}
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import org.apache.spark.SparkContext._
import org.kohsuke.args4j.Argument
import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.Job
import java.util.logging.Level
import edu.berkeley.cs.amplab.adam.projections.{ADAMRecordField, Projection}
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.models.SequenceRecord

object ListDict extends AdamCommandCompanion {
  val commandName: String = "listdict"
  val commandDescription: String = "Prints the contents of an ADAM sequence dictionary"

  def apply(cmdLine: Array[String]): AdamCommand = {
    new ListDict(Args4j[ListDictArgs](cmdLine))
  }
}

class ListDictArgs extends Args4jBase with SparkArgs with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM sequence dictionary to print", index = 0)
  val inputPath: String = null
}

class ListDict(protected val args: ListDictArgs) extends AdamSparkCommand[ListDictArgs] {
  val companion: AdamCommandCompanion = ListDict

  def run(sc: SparkContext, job: Job): Unit = {
    // Quiet parquet logging...
    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)

    val dict = sc.adamDictionaryLoad[ADAMRecord](args.inputPath)
    dict.records.sortBy(_.id).foreach {
      rec : SequenceRecord =>
        println( "%d\t%s\t%d".format(rec.id, rec.name, rec.length))
    }
  }

}
