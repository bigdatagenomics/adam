/**
 * Copyright 2014 Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.cli

import org.kohsuke.args4j.{ Argument, Option => Args4jOption }
import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.Job
import org.bdgenomics.adam.plugins.{ AccessControl, ADAMPlugin }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import parquet.filter.UnboundRecordFilter
import org.apache.avro.specific.SpecificRecord
import org.bdgenomics.adam.avro.ADAMRecord

/**
 * This set of classes executes a plugin along with the associated input location.
 *
 * Example usage:
 *   adam plugin org.bdgenomics.adam.plugins.Take10Plugin reads12.sam
 *
 * <code>org.bdgenomics.adam.plugins.Take10Plugin</code> is a simple example plugin. The
 * [[org.bdgenomics.adam.plugins.ADAMPlugin]] interface defines the class that will run using this command.
 */
object PluginExecutor extends ADAMCommandCompanion {
  val commandName: String = "plugin"
  val commandDescription: String = "Executes an ADAMPlugin"

  def apply(cmdLine: Array[String]): ADAMCommand = {
    new PluginExecutor(Args4j[PluginExecutorArgs](cmdLine))
  }
}

class PluginExecutorArgs extends Args4jBase with SparkArgs with ParquetArgs {
  @Argument(required = true, metaVar = "PLUGIN", usage = "The ADAMPlugin to run", index = 0)
  var plugin: String = null

  // Currently, this *must* be an ADAMRecord file, and it is only one.
  @Argument(required = true, metaVar = "INPUT", usage = "The input location", index = 1)
  var input: String = null

  @Args4jOption(name = "-access_control", usage = "Class for access control")
  var accessControl: String = "org.bdgenomics.adam.plugins.EmptyAccessControl"
}

class PluginExecutor(protected val args: PluginExecutorArgs) extends ADAMSparkCommand[PluginExecutorArgs] {
  val companion: ADAMCommandCompanion = PluginExecutor

  def loadPlugin[Input <% SpecificRecord: Manifest, Output](pluginName: String): ADAMPlugin[Input, Output] = {
    Thread.currentThread()
      .getContextClassLoader
      .loadClass(pluginName)
      .newInstance()
      .asInstanceOf[ADAMPlugin[Input, Output]]
  }

  def loadAccessControl[Input <% SpecificRecord: Manifest](accessControl: String): AccessControl[Input] = {
    Thread.currentThread()
      .getContextClassLoader
      .loadClass(accessControl)
      .newInstance()
      .asInstanceOf[AccessControl[Input]]
  }

  def load[Input <% SpecificRecord: Manifest](sc: SparkContext, locations: String, projection: Option[Schema]): RDD[Input] = {
    sc.adamLoad[Input, UnboundRecordFilter](locations, projection = projection)
  }

  def output[Output](sc: SparkContext, output: RDD[Output]) {
    output.map(_.toString).collect().foreach(println)
  }

  def run(sc: SparkContext, job: Job): Unit = {
    val plugin = loadPlugin[ADAMRecord, Any](args.plugin)
    val accessControl = loadAccessControl[ADAMRecord](args.accessControl)

    // Create an optional combined filter so that pass-through is not penalized
    //
    // Eventually, these filters should be passed down through to the adamLoad instead of operating on the RDDs.
    // This would prevent unnecessary loading from disk; for instance, if you are attempting to access multiple ADAM
    // files, but only permissioned for one, you could save a lot of IO by only loading the ones you are permissioned to
    // see. This is related to Issue #62: Predicate to filter conversion.
    val filter = accessControl.predicate match {
      case None => plugin.predicate match {
        case None => None
        case Some(predicateFilter) => Some(predicateFilter)
      }
      case Some(accessControlPredicate) => plugin.predicate match {
        case None => Some(accessControlPredicate)
        case Some(predicateFilter) => Some((value: ADAMRecord) => accessControlPredicate(value) && predicateFilter(value))
      }
    }

    val firstRdd: RDD[ADAMRecord] = load[ADAMRecord](sc, args.input, plugin.projection)

    val input = filter match {
      case None => firstRdd
      case Some(filterFunc) => firstRdd.filter(filterFunc)
    }

    val results = plugin.run(sc, input)

    output(sc, results)
  }

}
