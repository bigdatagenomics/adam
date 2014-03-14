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

import edu.berkeley.cs.amplab.adam.util.ParquetFileTraversable
import java.util
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.kohsuke.args4j.Argument
import scala.collection.JavaConversions._

object PrintADAM extends ADAMCommandCompanion {
  val commandName: String = "print"
  val commandDescription: String = "Print an ADAM formatted file"

  def apply(cmdLine: Array[String]) = {
    new PrintADAM(Args4j[PrintADAMArgs](cmdLine))
  }
}

class PrintADAMArgs extends Args4jBase with SparkArgs {
  @Argument(required = true, metaVar = "FILE(S)", multiValued = true, usage = "One or more files to print")
  var filesToPrint = new util.ArrayList[String]()
}

class PrintADAM(protected val args: PrintADAMArgs) extends ADAMSparkCommand[PrintADAMArgs] {
  val companion = PrintADAM

  def run(sc: SparkContext, job: Job) {
    for (file <- args.filesToPrint) {
      val it = new ParquetFileTraversable[IndexedRecord](sc, file)
      for (pileup <- it) {
        println(pileup)
      }
    }
  }
}
