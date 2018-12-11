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
import org.bdgenomics.adam.sql.{ Feature => FeatureProduct }
import org.bdgenomics.formats.avro.Feature

/**
 * Singleton object for converting from Avro Feature to Coverage.
 */
private[adam] object Coverage {

  /**
   * Creates Coverage from ReferenceRegion and coverage count in that ReferenceRegion.
   *
   * @param region ReferenceRegion in which Coverage spans
   * @param count Coverage count for each base pair in region
   * @param optSampleId Option of sampleId for this Coverage record
   * @return Coverage spanning the specified ReferenceRegion
   */
  def apply(region: ReferenceRegion, count: Double, optSampleId: Option[String]): Coverage = {
    Coverage(region.referenceName, region.start, region.end, count, optSampleId)
  }

  /**
   * Creates Coverage from Feature, extracting region information and feature score for coverage.
   *
   * @param feature Feature to create coverage from
   * @return Coverage spanning the specified feature
   */
  def apply(feature: Feature): Coverage = {
    require(feature.getReferenceName != null && feature.getReferenceName.length > 0,
      "Features must have reference name to convert to Coverage")
    require(feature.getStart != null && feature.getEnd != null,
      "Features must have valid position data to convert to Coverage")
    require(feature.getScore != null,
      "Features must have valid score to convert to Coverage")

    Coverage(feature.getReferenceName,
      feature.getStart,
      feature.getEnd,
      feature.getScore,
      Option(feature.getSampleId))
  }

  /**
   * Creates an RDD of Coverage from RDD of Features.
   *
   * @param rdd RDD of Features to extract Coverage from
   * @return RDD of Coverage spanning all features in rdd
   */
  def apply(rdd: RDD[Feature]): RDD[Coverage] = {
    rdd.map(f => Coverage(f))
  }
}

/**
 * Coverage record for CoverageDataset.
 *
 * Contains Region indexed by reference name, start and end, as well as the average
 * coverage at each base pair in that region.
 *
 * @param referenceName The chromosome that this coverage was observed on.
 * @param start The start coordinate of the region where this coverage value was
 *   observed.
 * @param end The end coordinate of the region where this coverage value was
 *   observed.
 * @param optSampleId Option of sampleId for this Coverage record
 * @param count The average coverage across this region.
 */
case class Coverage(referenceName: String, start: Long, end: Long, count: Double, optSampleId: Option[String] = None) {

  /**
   * Converts Coverage to Feature, setting Coverage count in the score attribute.
   *
   * @return Feature built from Coverage
   */
  def toFeature: Feature = {
    val featureBuilder = Feature.newBuilder()
      .setReferenceName(referenceName)
      .setStart(start)
      .setEnd(end)
      .setScore(count)

    // set name, if applicable
    if (optSampleId.isDefined) {
      featureBuilder.setSampleId(optSampleId.get)
    }

    featureBuilder.build()
  }
  /**
   * Converts Coverage to a Feature case class, for use with Spark SQL.
   */
  def toSqlFeature: FeatureProduct = {
    new FeatureProduct(featureId = None,
      sampleId = optSampleId,
      name = None,
      source = None,
      featureType = None,
      referenceName = Some(referenceName),
      start = Some(start),
      end = Some(end),
      strand = None,
      phase = None,
      frame = None,
      score = Some(count),
      geneId = None,
      transcriptId = None,
      exonId = None,
      aliases = Seq.empty,
      parentIds = Seq.empty,
      target = None,
      gap = None,
      derivesFrom = None,
      notes = Seq.empty,
      dbxrefs = Seq.empty,
      ontologyTerms = Seq.empty,
      circular = None,
      attributes = Map.empty)
  }
}

