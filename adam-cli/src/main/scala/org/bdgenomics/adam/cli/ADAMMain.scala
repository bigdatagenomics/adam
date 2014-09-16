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

import org.apache.spark.Logging
import scala.Some
import scala.collection.mutable.ListBuffer
import org.bdgenomics.adam.util.ParquetLogger
import java.util.logging.Level._

object ADAMMain extends Logging {

  case class CommandGroup(name: String, commands: List[ADAMCommandCompanion])

  private val commandGroups =
    List(
      CommandGroup("ADAM ACTIONS", List(
        CompareADAM,
        FindReads,
        CalculateDepth,
        CountKmers,
        Transform,
        /* TODO (nealsid): Reimplement in terms of new schema
	  ComputeVariants
	*/
        PluginExecutor)),
      CommandGroup("CONVERSION OPERATIONS", List(
        Bam2ADAM,
        Vcf2FlatGenotype,
        Vcf2ADAM,
        VcfAnnotation2ADAM,
        ADAM2Vcf,
        Fasta2ADAM,
        Reads2Ref,
        MpileupCommand,
        Features2ADAM)),
      CommandGroup("PRINT", List(
        PrintADAM,
        FlagStat,
        VizReads,
        PrintTags,
        ListDict,
        SummarizeGenotypes,
        AlleleCount,
        BuildInformation)))

  private def printCommands() {
    println("\n")
    println("""     e            888~-_              e                 e    e
               |    d8b           888   \            d8b               d8b  d8b
               |   /Y88b          888    |          /Y88b             d888bdY88b
               |  /  Y88b         888    |         /  Y88b           / Y88Y Y888b
               | /____Y88b        888   /         /____Y88b         /   YY   Y888b
               |/      Y88b       888_-~         /      Y88b       /          Y888b""".stripMargin('|'))
    println("\nChoose one of the following commands:")
    commandGroups.foreach { grp =>
      println("\n%s".format(grp.name))
      grp.commands.foreach(cmd =>
        println("%20s : %s".format(cmd.commandName, cmd.commandDescription)))
    }
    println("\n")
  }

  def main(args: Array[String]) {
    log.info("ADAM invoked with args: %s".format(argsToString(args)))
    if (args.size < 1) {
      printCommands()
    } else {
      var commands = new ListBuffer[ADAMCommandCompanion]
      commandGroups.foreach(grp =>
        grp.commands.foreach(cmd =>
          commands += cmd))
      commands.find(_.commandName == args(0)) match {
        case None => printCommands()
        case Some(cmd) =>
          init(Args4j[InitArgs](args drop 1, ignoreCmdLineExceptions = true))
          cmd.apply(args drop 1).run()
      }
    }
  }

  // Attempts to format the `args` array into a string in a way
  // suitable for copying and pasting back into the shell.
  private def argsToString(args: Array[String]): String = {
    def escapeArg(s: String) = "\"" + s.replaceAll("\\\"", "\\\\\"") + "\""
    args.map(escapeArg).mkString(" ")
  }

  class InitArgs extends Args4jBase with ParquetArgs {}

  private def init(args: InitArgs) {
    // Set parquet logging (default: severe)
    ParquetLogger.hadoopLoggerLevel(parse(args.logLevel))
  }
}
