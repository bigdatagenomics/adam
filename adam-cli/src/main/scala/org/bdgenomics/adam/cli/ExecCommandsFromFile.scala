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

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{ SparkContext, Logging }
import org.bdgenomics.utils.cli.{ BDGSparkCommand, Args4jBase, BDGCommand, Args4j, BDGCommandCompanion }
import org.kohsuke.args4j.Argument

object ExecCommandsFromFile extends BDGCommandCompanion {
  val commandName = "file"
  val commandDescription = "Run a sequence of ADAM commands provided in a file"

  def apply(cmdLine: Array[String]) = {
    new ExecCommandsFromFile(Args4j[ExecCommandsFromFileArgs](cmdLine))
  }
}

class ExecCommandsFromFileArgs extends Args4jBase {
  @Argument(required = true, metaVar = "INPUT", usage = "A file to read commands from, one per line, including all the arguments and flags that would normally be passed to 'adam-{shell,submit}'.", index = 0)
  var inputPath: String = null
}

class ExecCommandsFromFile(protected val args: ExecCommandsFromFileArgs)
    extends BDGSparkCommand[ExecCommandsFromFileArgs]
    with Logging {
  override val companion: BDGCommandCompanion = ExecCommandsFromFile

  override def run(sc: SparkContext): Unit = {
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
    for {
      line <- sc.textFile(args.inputPath).collect().map(_.trim)
      if line.nonEmpty
      commandName :: args = line.split(' ').toList
      command <- ADAMMain.getCommand(commandName)
    } {
      command.apply(args.toArray) match {
        case sparkCommand: BDGSparkCommand[_] => sparkCommand.run(sc)
        case bdgCommand                       => bdgCommand.run()
      }
    }
  }
}
