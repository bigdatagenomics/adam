/**
 * Copyright 2013 Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.cs.amplab.adam.cli

import edu.berkeley.cs.amplab.adam.util.ParquetLogger
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import org.kohsuke.args4j.Argument
import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.Job
import java.util.logging.Level
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.models.SequenceRecord

object ListDict extends AdamCommandCompanion {
  val commandName: String = "listdict"
  val commandDescription: String = "Print the contents of an ADAM sequence dictionary"

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

    dict.recordsIn.sortBy(_.id).foreach {
      rec: SequenceRecord =>
        println("%d\t%s\t%d".format(rec.id, rec.name, rec.length))
    }
  }

}
