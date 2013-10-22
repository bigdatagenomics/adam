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

private[rdd] object MarkDuplicates {

  def apply(rdd: RDD[ADAMRecord]): RDD[ADAMRecord] = {
    new MarkDuplicates().markDuplicates(rdd)
  }
}

private[rdd] class MarkDuplicates extends Serializable with Logging {
  initLogging()

  def markDuplicates(rdd: RDD[ADAMRecord]): RDD[ADAMRecord] = {
    log.warn("Mark Duplicates currently not implemented")
    rdd
  }

}
