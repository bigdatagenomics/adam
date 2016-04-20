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
package org.bdgenomics.adam.models

import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.Feature

/**
 * Converts from avro Feature to Coverage.
 */
object Coverage {

  /**
   * Creates Coverage from ReferenceRegion and coverage count in that ReferenceRegion.
   *
   * @param region ReferenceRegion in which Coverage spans
   * @param count Coverage count for each base pair in region
   * @return Coverage spanning the specified ReferenceRegion
   */
  private[adam] def apply(region: ReferenceRegion, count: Double): Coverage = {
    Coverage(region.referenceName, region.start, region.end, count)
  }

  /**
   * Creates Coverage from Feature, extracting region information and feature score for coverage.
   *
   * @param feature Feature to create coverage from
   * @return Coverage spanning the specified feature
   */
  private[adam] def apply(feature: Feature): Coverage = {
    Coverage(feature.getContigName, feature.getStart, feature.getEnd, feature.getScore)
  }

  /**
   * Creates an RDD of Coverage from RDD of Features.
   *
   * @param rdd RDD of Features to extract Coverage from
   * @return RDD of Coverage spanning all features in rdd
   */
  private[adam] def apply(rdd: RDD[Feature]): RDD[Coverage] = {
    rdd.map(f => Coverage(f))
  }
}

/**
 * Coverage record for CoverageRDD.
 * Contains Region indexed by contig name, start and end, as well as count of coverage at
 * each base pair in that region.
 *
 * @param contigName Specifies chromosomal location of coverage
 * @param start Specifies start position of coverage
 * @param end  Specifies end position of coverage
 * @param count Specifies count of coverage at location
 */
case class Coverage(contigName: String, start: Long, end: Long, count: Double) {

  /**
   * Converts Coverage to Feature, setting Coverage count in the score attribute.
   *
   * @return Feature built from Coverage
   */
  def toFeature: Feature = {
    val fb = Feature.newBuilder()
    fb.setContigName(contigName)
    fb.setStart(start)
    fb.setEnd(end)
    fb.setScore(count)
    fb.build()
  }
}

