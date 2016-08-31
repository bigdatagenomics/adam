/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.bdgenomics.adam.models.SequenceRecord
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.Argument

object ListDict extends BDGCommandCompanion {
  val commandName: String = "listdict"
  val commandDescription: String = "Print the contents of an ADAM sequence dictionary"

  def apply(cmdLine: Array[String]): BDGCommand = {
    new ListDict(Args4j[ListDictArgs](cmdLine))
  }
}

class ListDictArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "The ADAM sequence dictionary to print", index = 0)
  val inputPath: String = null
}

class ListDict(protected val args: ListDictArgs) extends BDGSparkCommand[ListDictArgs] {
  val companion: BDGCommandCompanion = ListDict

  def run(sc: SparkContext): Unit = {
    val gRdd = sc.loadAlignments(args.inputPath)

    println(gRdd.sequences)
  }
}
