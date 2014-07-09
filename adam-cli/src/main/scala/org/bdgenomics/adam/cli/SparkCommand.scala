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

import org.kohsuke.args4j.{ Option => Args4jOption }
import java.util
import scala.collection.JavaConversions._
import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.instrumentation.ADAMMetricsListener

trait SparkArgs extends Args4jBase {
  @Args4jOption(required = false, name = "-spark_master", usage = "Spark Master (default = \"local[#cores]\")")
  var spark_master = "local[%d]".format(Runtime.getRuntime.availableProcessors())
  @Args4jOption(required = false, name = "-spark_home", metaVar = "PATH", usage = "Spark home")
  var spark_home: String = null
  @Args4jOption(required = false, name = "-spark_jar", metaVar = "JAR", usage = "Add Spark jar")
  var spark_jars = new util.ArrayList[String]()
  @Args4jOption(required = false, name = "-spark_env", metaVar = "KEY=VALUE", usage = "Add Spark environment variable")
  var spark_env_vars = new util.ArrayList[String]()
  @Args4jOption(required = false, name = "-spark_kryo_buffer_size", usage = "Set the size of the buffer used for serialization in MB. Default size is 4MB.")
  var spark_kryo_buffer_size = 4
  @Args4jOption(required = false, name = "-spark_add_stats_listener", usage = "Register job stat reporter, which is useful for debug/profiling.")
  var spark_add_stats_listener = false
  @Args4jOption(required = false, name = "-coalesce", usage = "Set the number of partitions written to the ADAM output directory")
  var coalesce: Int = -1
  @Args4jOption(required = false, name = "-repartition", usage = "Set the number of partitions to map data to")
  var repartition: Int = -1
}

trait SparkCommand extends ADAMCommand {

  /**
   * Commandline format is -spark_env foo=1 -spark_env bar=2
   * @param envVariables The variables found on the commandline
   * @return
   */
  def parseEnvVariables(envVariables: util.ArrayList[String]): Array[(String, String)] = {
    envVariables.foldLeft(Array[(String, String)]()) {
      (a, kv) =>
        val kvSplit = kv.split("=")
        if (kvSplit.size != 2) {
          throw new IllegalArgumentException("Env variables should be key=value syntax, e.g. -spark_env foo=bar")
        }
        a :+ (kvSplit(0), kvSplit(1))
    }
  }

  def createSparkContext(args: SparkArgs, metricsListener: Option[ADAMMetricsListener]): SparkContext = {
    ADAMContext.createSparkContext(
      name = companion.commandName,
      master = args.spark_master,
      sparkHome = args.spark_home,
      sparkJars = args.spark_jars,
      sparkEnvVars = parseEnvVariables(args.spark_env_vars),
      sparkAddStatsListener = args.spark_add_stats_listener,
      sparkKryoBufferSize = args.spark_kryo_buffer_size,
      sparkMetricsListener = metricsListener)
  }

}
