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

import org.apache.avro.generic.IndexedRecord
import scala.collection.JavaConversions._
import edu.berkeley.cs.amplab.adam.util.{ParquetFileTraversable, Args4jBase, Args4j}
import org.kohsuke.args4j.Argument
import java.util
import spark.SparkContext
import org.apache.hadoop.mapreduce.Job

object PrintAdam extends AdamCommandCompanion {
  val commandName: String = "print"
  val commandDescription: String = "Print an ADAM formatted file"

  def apply(cmdLine: Array[String]) = {
    new PrintAdam(Args4j[PrintAdamArgs](cmdLine))
  }
}

class PrintAdamArgs extends Args4jBase with SparkArgs {
  @Argument(required = true, metaVar = "FILE(S)", multiValued = true, usage = "One or more files to print")
  var filesToPrint = new util.ArrayList[String]()
}

class PrintAdam(protected val args: PrintAdamArgs) extends AdamSparkCommand[PrintAdamArgs] {
  val companion = PrintAdam

  def run(sc: SparkContext, job: Job) {
    for (file <- args.filesToPrint) {
      val it = new ParquetFileTraversable[IndexedRecord](sc, file)
      for (pileup <- it) {
        println(pileup)
      }
    }
  }
}
