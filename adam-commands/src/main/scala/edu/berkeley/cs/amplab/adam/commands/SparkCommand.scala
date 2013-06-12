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

import spark.SparkContext
import org.kohsuke.args4j.Option
import edu.berkeley.cs.amplab.adam.util.Args4jBase

trait SparkArgs extends Args4jBase {
  @Option(required = false, name = "-spark_master", usage = "Spark Master (default=local)")
  var master = "local"
  // TODO: add more Spark options
}

trait SparkCommand extends AdamCommand {

  def createSparkContext(args: SparkArgs): SparkContext = {
    new SparkContext(args.master, "adam: " + commandName)
  }

}
