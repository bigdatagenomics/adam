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
import org.bdgenomics.adam.models.{ RecordGroupDictionary, SequenceDictionary }
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
   */
  def adamRecordLoad(filePath: java.lang.String): JavaAlignmentRecordRDD = {
    val aRdd = ac.loadAlignments(filePath)
    new JavaAlignmentRecordRDD(aRdd.rdd.toJavaRDD(),
      aRdd.sequences,
      aRdd.recordGroups)
  }
}
