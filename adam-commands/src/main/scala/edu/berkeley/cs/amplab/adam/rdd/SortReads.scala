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

package edu.berkeley.cs.amplab.adam.rdd

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import spark.{Logging, RDD}
import spark.SparkContext._
import edu.berkeley.cs.amplab.adam.models.ReferencePosition

private[rdd] object SortReads {

  def apply(rdd: RDD[ADAMRecord]): RDD[ADAMRecord] = {
    new SortReads().sortReads(rdd)
  }

}

private[rdd] class SortReads extends Serializable with Logging {

  def sortReads(rdd: RDD[ADAMRecord]): RDD[ADAMRecord] = {
    log.info("Sorting reads by reference in ADAM RDD")
    rdd.map(p => (ReferencePosition(p), p)).sortByKey().map(p => p._2)
  }
}

