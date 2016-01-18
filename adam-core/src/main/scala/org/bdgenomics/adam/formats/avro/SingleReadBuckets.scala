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
package org.bdgenomics.adam.formats.avro

import org.bdgenomics.formats.avro.{ AlignmentRecord, Fragment, SingleReadBucket }

import scala.collection.JavaConversions._

object SingleReadBuckets {

  def allReads(bucket: SingleReadBucket): Iterable[AlignmentRecord] = {
    bucket.getPrimaryMapped ++ bucket.getSecondaryMapped ++ bucket.getUnmapped
  }

  def toFragment(bucket: SingleReadBucket): Fragment = {
    // take union of all reads, as we will need this for building and
    // want to pay the cost exactly once
    val unionReads = allReads(bucket)

    // start building fragment
    val builder = Fragment.newBuilder()
      .setReadName(unionReads.head.getReadName)
      .setAlignments(seqAsJavaList(unionReads.toSeq))

    // is an insert size defined for this fragment?
    bucket.getPrimaryMapped.headOption
      .foreach(r => {
        Option(r.getInferredInsertSize).foreach(is => {
          builder.setFragmentSize(is.toInt)
        })
      })

    // set record group name, if known
    Option(unionReads.head.getRecordGroupName)
      .foreach(n => builder.setRunId(n))

    builder.build()
  }
}
