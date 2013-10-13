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
import org.kohsuke.args4j.{Option => Args4jOption}
import edu.berkeley.cs.amplab.adam.util.Args4jBase
import java.util
import scala.collection.JavaConversions._

trait SparkArgs extends Args4jBase {
  @Args4jOption(required = false, name = "-spark_master", usage = "Spark Master (default = \"local[#cores]\")")
  var spark_master = "local[%d]".format(Runtime.getRuntime.availableProcessors())
  @Args4jOption(required = false, name = "-spark_home", metaVar = "PATH", usage = "Spark home")
  var spark_home: String = null
  @Args4jOption(required = false, name = "-spark_jar", metaVar = "JAR", usage = "Add Spark jar")
  var spark_jars = new util.ArrayList[String]()
  @Args4jOption(required = false, name = "-spark_env", metaVar = "KEY=VALUE", usage = "Add Spark environment variable")
  var spark_env_vars = new util.ArrayList[String]()
}

trait SparkCommand extends AdamCommand {

  def createSparkContext(args: SparkArgs): SparkContext = {
    val appName = "adam: " + companion.commandName
    val environment: Map[String, String] = if (args.spark_env_vars.isEmpty) {
      Map()
    } else {
      args.spark_env_vars.map {
        kv =>
          val kvSplit = kv.split("=")
          if (kvSplit.size != 2) {
            throw new IllegalArgumentException("Env variables should be key=value syntax, e.g. -spark_env foo=bar")
          }
          (kvSplit(0), kvSplit(1))
      }.toMap
    }
    val jars: Seq[String] = if (args.spark_jars.isEmpty) Nil else args.spark_jars
    new SparkContext(args.spark_master, appName, args.spark_home, jars, environment)
  }

}
