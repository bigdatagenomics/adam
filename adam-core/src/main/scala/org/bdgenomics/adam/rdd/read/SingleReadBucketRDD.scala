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
package org.bdgenomics.adam.rdd.read

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{AlignmentRecord, SingleReadBucket}

private[read] object SingleReadBucketRDD extends Logging {
  def apply(rdd: RDD[AlignmentRecord]): RDD[SingleReadBucket] = {
    rdd.groupBy(p => (p.getRecordGroupName, p.getReadName))
      .map(kv => {
        val (_, reads) = kv

        // split by mapping
        val (mapped, unmapped) = reads.partition(_.getReadMapped)
        val (primaryMapped, secondaryMapped) = mapped.partition(_.getPrimaryAlignment)

        // TODO: consider doing validation here (e.g. read says mate mapped but it doesn't exist)
        new SingleReadBucket(primaryMapped.toList, secondaryMapped.toList, unmapped.toList)
      })
  }
}


