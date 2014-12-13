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
package org.apache.spark.rdd

import scala.reflect.ClassTag

/**
 * Contains implicit conversions which enable instrumentation of Spark operations. This class should be used instead
 * of [[org.apache.spark.SparkContext]] when instrumentation is required.  Usage is as follows:
 *
 * {{{
 * import org.bdgenomics.adam.instrumentation.Metrics._
 * import org.apache.spark.rdd.MetricsContext._
 * Metrics.initialize(sparkContext)
 * val instrumentedRDD = rdd.instrument()
 * }}}
 *
 * Then, when any operations are performed on `instrumentedRDD` the RDD operation will be instrumented, along
 * with any functions that operate on its data. All subsequent RDD operations will be instrumented until
 * the `unInstrument` method is called on an RDD.
 *
 * @note When using this class, it is not a good idea to import `SparkContext._`, as the implicit conversions in there
 *       may conflict with those in here -- instead it is better to import only the specific parts of `SparkContext`
 *       that are needed.
 */
object MetricsContext {

  implicit def rddToInstrumentedRDD[T: ClassTag](rdd: RDD[T]) = new InstrumentedRDDFunctions(rdd)

  implicit def rddToInstrumentedPairRDD[K, V](rdd: RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null) = new InstrumentedPairRDDFunctions(rdd)

  implicit def rddToInstrumentedOrderedRDD[K: Ordering: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) = new InstrumentedOrderedRDDFunctions[K, V](rdd)

}
