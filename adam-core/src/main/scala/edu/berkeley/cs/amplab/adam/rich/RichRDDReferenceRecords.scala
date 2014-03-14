/*
 * Copyright 2014 Genome Bridge LLC
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

package edu.berkeley.cs.amplab.adam.rich

import ReferenceMappingContext._
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.models.ReferenceMapping
import org.apache.avro.specific.SpecificRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class RichRDDReferenceRecords[T <: SpecificRecord : ClassManifest](rdd: RDD[T],
                                                                   mapping : ReferenceMapping[T])
  extends Serializable {

  def remapReferenceId(map: Map[Int, Int])(implicit sc : SparkContext): RDD[T] = {
    // If the reference IDs match, we don't need to do any remapping, just return the previous RDD
    if (map.forall({case (a, b)=> a == b})) rdd
    else {
      // Broadcast the map variable
      val bc = sc.broadcast(map)
      rdd.map(r => {
        val refId = mapping.getReferenceId(r)
        // If the reference ID is the same, we don't need to create a new ADAMRecord
        if (bc.value(refId) == refId.toInt) r
        else mapping.remapReferenceId(r, bc.value(refId))
      })
    }
  }
}

object RichRDDReferenceRecords extends Serializable {
  implicit def adamRDDToRichADAMRDD(rdd: RDD[ADAMRecord]) : RichRDDReferenceRecords[ADAMRecord] =
    new RichRDDReferenceRecords(rdd, ADAMRecordReferenceMapping)
}
