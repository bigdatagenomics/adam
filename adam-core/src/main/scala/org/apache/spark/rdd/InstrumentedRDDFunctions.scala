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
 * Functions which permit creation of instrumented RDDs, as well as the ability to stop instrumentation by
 * calling the `unInstrument` method. For more details and usage instructions see the [[MetricsContext]] class.
 */
class InstrumentedRDDFunctions[T: ClassTag](self: RDD[T]) {
  def instrument(): RDD[T] = self match {
    case instrumentedRDD: InstrumentedRDD[T] => self
    case _                                   => InstrumentedRDD.instrument(self)
  }
  def unInstrument(): RDD[T] = self match {
    case instrumentedRDD: InstrumentedRDD[T] => instrumentedRDD.decoratedRDD
    case _                                   => self
  }
}
