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

package org.bdgenomics.adam.cli

import org.kohsuke.args4j.{ CmdLineException, CmdLineParser, Option }
import scala.collection.JavaConversions._
import scala.util.control.Exception._
import scala.Left
import scala.Right

class Args4jBase {
  @Option(name = "-h", aliases = Array("-help", "--help", "-?"), usage = "Print help")
  var doPrintUsage: Boolean = false
}

object Args4j {
  val helpOptions = Array("-h", "-help", "--help", "-?")

  def apply[T <% Args4jBase: Manifest](args: Array[String]): T = {
    val args4j: T = manifest[T].erasure.asInstanceOf[Class[T]].newInstance()
    val parser = new CmdLineParser(args4j)
    parser.setUsageWidth(150);

    def displayHelp(exitCode: Int = 0) = {
      parser.printUsage(System.out)
      System.exit(exitCode)
    }

    // Work around for help processing in Args4j
    if (args.exists(helpOptions.contains(_))) {
      displayHelp()
    }

    allCatch either parser.parseArgument(args.toList) match {
      case Right(x) => {
        if (args4j.doPrintUsage) {
          displayHelp()
        }
      }
      case Left(e: CmdLineException) => {
        println(e.getMessage)
        displayHelp(1)
      }
    }

    args4j
  }

}

