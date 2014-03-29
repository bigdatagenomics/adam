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

import org.kohsuke.args4j.{Option => Args4jOption}
import java.util
import scala.collection.JavaConversions._
import org.apache.spark.SparkContext
import edu.berkeley.cs.amplab.adam.serialization.AdamKryoProperties
import edu.berkeley.cs.amplab.adam.rdd.AdamContext

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

trait SparkCommand extends AdamCommand {

  def createSparkContext(args: SparkArgs): SparkContext = {
    AdamContext.createSparkContext(
      companion.commandName,
      args.spark_master,
      args.spark_home,
      args.spark_jars,
      args.spark_env_vars,
      args.spark_add_stats_listener,
      args.spark_kryo_buffer_size)
  }

}
