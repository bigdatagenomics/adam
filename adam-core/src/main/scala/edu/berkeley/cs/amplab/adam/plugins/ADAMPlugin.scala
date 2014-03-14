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
package edu.berkeley.cs.amplab.adam.plugins

import org.apache.avro.Schema
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

/**
 * Defines the interface for a Plugin for the AdamSystem.
 *
 * A simple interface is available in [[edu.berkeley.cs.amplab.adam.plugins.Take10Plugin]]
 */
trait AdamPlugin[Input, Output] {
  /**
   * The projection to push down into Parquet
   * @return If all fields are required or the specific projection is not known, None
   *         If a subset of fields on Input are required, an Avro schema with those fields
   */
  def projection : Option[Schema]

  /**
   * The records that are applicable to this Plugin
   * @return If there is no filter for this plugin, None
   *         If a filter is applicable, a true response means record is included; a false means record is excluded
   */
  def predicate : Option[Input => Boolean]

  /**
   * Method to create the transformations on the RDD.
   */
  def run(sc: SparkContext, recs: RDD[Input]): RDD[Output]
}
