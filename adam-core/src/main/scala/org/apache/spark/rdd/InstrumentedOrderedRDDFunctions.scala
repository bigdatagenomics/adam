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

import org.apache.spark.SparkContext.rddToOrderedRDDFunctions
import org.apache.spark.rdd.InstrumentedRDD._
import scala.reflect.ClassTag

/**
 * A version of [[OrderedRDDFunctions]] which enables instrumentation of its operations. For more details
 * and usage instructions see the [[MetricsContext]] class.
 */
class InstrumentedOrderedRDDFunctions[K: Ordering: ClassTag, V: ClassTag](self: RDD[(K, V)]) extends Serializable {

  def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.size): RDD[(K, V)] = self match {
    case instrumented: InstrumentedRDD[_] => recordOperation {
      instrument(self.sortByKey(ascending, numPartitions))
    }
    case _ => self.sortByKey(ascending, numPartitions)
  }

}
