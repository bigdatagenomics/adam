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
package org.bdgenomics.adam.apis.java

import org.apache.avro.Schema
import org.apache.spark.SparkContext
import org.apache.spark.api.java.{ JavaRDD, JavaSparkContext }
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.plugins.ADAMPlugin
import org.bdgenomics.adam.rdd.ADAMContext._

private[java] abstract class JavaADAMPlugin[Input, Output] extends ADAMPlugin[Input, Output] with Serializable {
  final override def projection: Option[Schema] = Option(recordProjection)

  /**
   * The fields of the input record that should be populated. If a projection should not
   * be applied, this should be null. Code upstream will check for a null value, so this
   * is safe.
   *
   * @return A schema describing the projection to apply to all records.
   */
  def recordProjection: Schema

  final override def run(sc: SparkContext, recs: RDD[Input], args: String): RDD[Output] = {
    run(new JavaSparkContext(sc), recs.toJavaRDD(), args)
  }

  /**
   * Run method to be implemented by plugin implementor.
   *
   * @param jsc Java Spark Context that this plugin is running inside of.
   * @param recs Input RDD of records.
   * @param args Command line arguments passed to this plugin.
   * @return Output RDD to be saved to disk.
   */
  def run(jsc: JavaSparkContext, recs: JavaRDD[Input], args: java.lang.String): JavaRDD[Output]
}

/**
 * Plugins that seek to apply a predicate on the input records should extend
 * this class.
 */
abstract class JavaADAMPluginWithPredicate[Input, Output] extends JavaADAMPlugin[Input, Output] {
  final override def predicate: Option[Input => Boolean] = Some(recordPredicate)

  /**
   * The predicate function to apply on each input record.
   *
   * @param record The input record to push predicates down on.
   * @return True if this should be filtered, else, false.
   */
  def recordPredicate(record: Input): java.lang.Boolean
}

/**
 * Plugins that do not want to apply a predicate should extend this class.
 */
abstract class JavaADAMPluginWithoutPredicate[Input, Output] extends JavaADAMPlugin[Input, Output] {
  final override def predicate: Option[Input => Boolean] = None
}
