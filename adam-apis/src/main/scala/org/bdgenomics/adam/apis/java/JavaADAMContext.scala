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

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaSparkContext
import org.bdgenomics.adam.instrumentation.ADAMMetricsListener
import org.bdgenomics.adam.predicates.ADAMPredicate
import org.bdgenomics.adam.rdd.ADAMContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro._

import scala.collection.JavaConversions._

object JavaADAMContext {
  // convert to and from java/scala implementations
  implicit def fromADAMContext(ac: ADAMContext): JavaADAMContext = new JavaADAMContext(ac)
  implicit def toADAMContext(jac: JavaADAMContext): ADAMContext = jac.ac
}

class JavaADAMContext(val ac: ADAMContext) extends Serializable {

  /**
   * @return Returns the Java Spark Context associated with this Java ADAM Context.
   */
  def getSparkContext: JavaSparkContext = new JavaSparkContext(ac.sc)

  /**
   * Builds this Java ADAM Context by creating a new Spark Context.
   *
   * @param name Name of the Spark Context to create.
   * @param master The URI of the Spark master.
   * @param sparkHome Path to the Spark home directory.
   * @param sparkJars JAR files to include.
   * @param sparkEnvVars A map containing envars and their settings.
   * @param sparkAddStatsListener Whether or not to include a stats listener.
   * @param sparkKryoBufferSize The size of the kryo serialization buffer in MB.
   * @param sparkMetricsListener A metrics listener to attach. Can be null.
   * @param loadSystemValues Whether to load system values or not.
   * @param sparkDriverPort The port mapping for the Spark driver. Can be null.
   * @return Returns a new Java ADAM Context, built on a new Spark Context.
   */
  def this(name: java.lang.String,
           master: java.lang.String,
           sparkHome: java.lang.String,
           sparkJars: java.util.List[java.lang.String],
           sparkEnvVars: java.util.Map[java.lang.String, java.lang.String],
           sparkAddStatsListener: java.lang.Boolean,
           sparkKryoBufferSize: java.lang.Integer,
           sparkMetricsListener: ADAMMetricsListener,
           loadSystemValues: java.lang.Boolean,
           sparkDriverPort: java.lang.Integer) = {
    this(new ADAMContext(ADAMContext.createSparkContext(name,
      master,
      sparkHome, {
        // force implicit conversion
        val sj: List[java.lang.String] = sparkJars

        sj.map(_.toString)
      }, {
        // force implicit conversion
        val sev: Map[java.lang.String, java.lang.String] = mapAsScalaMap(sparkEnvVars).toMap

        sev.map(kv => (kv._1.toString,
          kv._2.toString)).toSeq
      }, sparkAddStatsListener,
      sparkKryoBufferSize,
      Option(sparkMetricsListener),
      loadSystemValues,
      Option(sparkDriverPort).map(_.toInt))))
  }

  /**
   * Builds this Java ADAM Context by creating a new Spark Context.
   *
   * @param name Name of the Spark Context to create.
   * @param master The URI of the Spark master.
   * @return Returns a new Java ADAM Context, built on a new Spark Context.
   */
  def this(name: java.lang.String,
           master: java.lang.String) = this(new ADAMContext(
    ADAMContext.createSparkContext(name, master)))

  /**
   * Builds this Java ADAM Context using an existing Java Spark Context.
   *
   * @param jsc Java Spark Context to use to build this ADAM Context.
   * @return A new Java ADAM Context.
   */
  def this(jsc: JavaSparkContext) = this(new ADAMContext(jsc))

  /**
   * Builds this Java ADAM Context using an existing Spark Context.
   *
   * @param sc Spark Context to use to build this ADAM Context.
   * @return A new Java ADAM Context.
   */
  def this(sc: SparkContext) = this(new ADAMContext(sc))

  /**
   * Loads in an ADAM read file. This method can load SAM, BAM, and ADAM files.
   *
   * @param filePath Path to load the file from.
   * @return Returns a read RDD.
   *
   * @tparam U A predicate to apply on the load.
   */
  def adamRecordLoad[U <: ADAMPredicate[AlignmentRecord]](filePath: java.lang.String): JavaAlignmentRecordRDD = {
    new JavaAlignmentRecordRDD(ac.adamLoad[AlignmentRecord, U](filePath).toJavaRDD())
  }
}
