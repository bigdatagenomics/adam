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

import org.apache.spark.Logging
import scala.Some
import edu.berkeley.cs.amplab.adam.cli.VcfAnnotation2Adam

object AdamMain extends Logging {

  private val commands = List(Transform,
    PrintTags,
    CalculateDepth,
    FlagStat,
    Reads2Ref,
    MpileupCommand,
    PrintAdam,
    PileupAggregator,
    ListDict,
    CompareAdam,
    /* TODO (nealsid): Reimplement in terms of new schema
    ComputeVariants, 
     */
    Bam2Adam,
    Adam2Vcf,
    Vcf2Adam,
    FindReads,
    Fasta2Adam,
    PluginExecutor,
    BuildInformation,
    VcfAnnotation2Adam
  )

  private def printCommands() {
    println("\n")
    println( """     e            888~-_              e                 e    e
               |    d8b           888   \            d8b               d8b  d8b
               |   /Y88b          888    |          /Y88b             d888bdY88b
               |  /  Y88b         888    |         /  Y88b           / Y88Y Y888b
               | /____Y88b        888   /         /____Y88b         /   YY   Y888b
               |/      Y88b       888_-~         /      Y88b       /          Y888b""".stripMargin('|'))
    println("\nChoose one of the following commands:\n")
    commands.foreach(cmd =>
      println("%20s : %s".format(cmd.commandName, cmd.commandDescription))
    )
    println("\n")
  }

  def main(args: Array[String]) {
    log.info("ADAM invoked with args: %s".format(argsToString(args)))
    if (args.size < 1) {
      printCommands()
    } else {
      commands.find(_.commandName == args(0)) match {
        case None => printCommands()
        case Some(cmd) => cmd.apply(args drop 1).run()
      }
    }
  }

  // Attempts to format the `args` array into a string in a way
  // suitable for copying and pasting back into the shell.
  private def argsToString(args: Array[String]): String = {
    def escapeArg(s: String) = "\"" + s.replaceAll("\\\"", "\\\\\"") + "\""
    args.map(escapeArg).mkString(" ")
  }
}
