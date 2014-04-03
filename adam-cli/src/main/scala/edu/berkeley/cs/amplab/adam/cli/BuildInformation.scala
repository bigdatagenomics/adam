/*
 * Copyright (c) 2014. Sebastien Mondet <seb@mondet.org>
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

import scala.collection.JavaConversions._
import scala.Some
import scala.collection.JavaConverters._

object BuildInformation extends ADAMCommandCompanion {
  val commandName: String = "buildinfo"
  val commandDescription: String = "Display build information (use this for bug reports)"

  def apply(cmdLine: Array[String]): ADAMCommand = {
    new BuildInformation()
  }
}

class BuildInformation() extends ADAMCommand {
  val companion = BuildInformation

  def run() = {
    val properties = edu.berkeley.cs.amplab.adam.core.util.BuildInformation.asString();
    println("Build information:\n" + properties);
  }

}
