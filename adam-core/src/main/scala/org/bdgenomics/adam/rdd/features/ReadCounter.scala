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
package org.bdgenomics.adam.rdd.features

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.RegionJoin
import org.bdgenomics.adam.rich.ReferenceMappingContext._
import org.bdgenomics.formats.avro.{ AlignmentRecord, Feature }

object ReadCounter {

  /**
   * Calculates the number of reads which overlap each Feature, in a given RDD.
   *
   * @param features The RDD of Features
   * @param reads The RDD of reads (AlignmentRecord values)
   * @return an RDD which contains each Feature, and an associated integer indicating
   *         the number of reads (from the 'reads' RDD) which overlapped any portion of the
   *         Feature.
   */
  def countReadsByFeature(sc: SparkContext,
                          dict: SequenceDictionary,
                          features: RDD[Feature],
                          reads: RDD[AlignmentRecord]): RDD[(Feature, Int)] = {

    val joined: RDD[(Feature, AlignmentRecord)] =
      RegionJoin.partitionAndJoin(sc, dict, features, reads)

    val counted: RDD[(Feature, Int)] = joined.groupBy(_._1).map {
      case ((key: Feature, values: Iterable[(Feature, AlignmentRecord)])) => key -> values.size
    }.cache()

    //val uncounted: RDD[Feature] = features.subtract(counted.map(_._1))
    //counted.union(uncounted.map(f => (f, 0)))

    counted
  }

}
