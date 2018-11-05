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

import java.util.logging.Level._
import javax.inject.Inject
import com.google.inject.AbstractModule
import net.codingwell.scalaguice.ScalaModule
import org.bdgenomics.utils.misc.Logging
import org.bdgenomics.adam.util.ParquetLogger
import org.bdgenomics.utils.cli._

object ADAMMain {

  val defaultCommandGroups =
    List(
      CommandGroup(
        "ADAM ACTIONS",
        List(
          CountReadKmers,
          CountContigKmers,
          TransformAlignments,
          TransformFeatures,
          TransformGenotypes,
          TransformVariants,
          MergeShards,
          Reads2Coverage
        )
      ),
      CommandGroup(
        "CONVERSION OPERATIONS",
        List(
          Fasta2ADAM,
          ADAM2Fasta,
          ADAM2Fastq,
          TransformFragments
        )
      ),
      CommandGroup(
        "PRINT",
        List(
          PrintADAM,
          FlagStat,
          View
        )
      )
    )

  def main(args: Array[String]) {
    new ADAMMain(defaultCommandGroups)(args)
  }
}

case class CommandGroup(name: String, commands: List[BDGCommandCompanion])

private class InitArgs extends Args4jBase with ParquetArgs {}

class ADAMMain @Inject() (commandGroups: List[CommandGroup]) extends Logging {

  private def printLogo() {
    print("\n")
    println("""       e        888~-_         e            e    e
               |      d8b       888   \       d8b          d8b  d8b
               |     /Y88b      888    |     /Y88b        d888bdY88b
               |    /  Y88b     888    |    /  Y88b      / Y88Y Y888b
               |   /____Y88b    888   /    /____Y88b    /   YY   Y888b
               |  /      Y88b   888_-~    /      Y88b  /          Y888b""".stripMargin('|'))
  }

  private def printVersion() {
    printLogo()
    val about = new About()
    println("\nADAM version: %s".format(about.version))
    if (about.isSnapshot) {
      println("Commit: %s Build: %s".format(about.commit, about.buildTimestamp))
    }
    println("Built for: Apache Spark %s, Scala %s, and Hadoop %s"
      .format(about.sparkVersion, about.scalaVersion, about.hadoopVersion))
  }

  private def printCommands() {
    printLogo()
    println("\nUsage: adam-submit [<spark-args> --] <adam-args>")
    println("\nChoose one of the following commands:")
    commandGroups.foreach { grp =>
      println("\n%s".format(grp.name))
      grp.commands.foreach(cmd =>
        println("%20s : %s".format(cmd.commandName, cmd.commandDescription)))
    }
    println("\n")
  }

  def apply(args: Array[String]) {
    log.info("ADAM invoked with args: %s".format(argsToString(args)))
    if (args.length < 1) {
      printCommands()
    } else if (args.contains("--version") || args.contains("-version")) {
      printVersion()
    } else {

      val commands =
        for {
          grp <- commandGroups
          cmd <- grp.commands
        } yield cmd

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

  private def init(args: InitArgs) {
    // Set parquet logging (default: severe)
    ParquetLogger.hadoopLoggerLevel(parse(args.logLevel))
  }
}

class ADAMModule extends AbstractModule with ScalaModule {
  override def configure() {
    bind[List[CommandGroup]].toInstance(ADAMMain.defaultCommandGroups)
  }
}
