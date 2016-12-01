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
package org.bdgenomics.adam.projections

import com.google.common.collect.ImmutableList
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.projections.VariantAnnotationField._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.TestSaveArgs
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ TranscriptEffect, VariantAnnotation }

class VariantAnnotationFieldSuite extends ADAMFunSuite {

  sparkTest("Use projection when reading parquet variant annotations") {
    val path = tmpFile("variantAnnotations.parquet")
    val rdd = sc.parallelize(Seq(VariantAnnotation.newBuilder()
      .setAncestralAllele("T")
      .setAlleleCount(42)
      .setReadDepth(10)
      .setForwardReadDepth(4)
      .setReverseReadDepth(13)
      .setAlleleFrequency(20.0f)
      .setCigar("M")
      .setDbSnp(true)
      .setHapMap2(true)
      .setHapMap3(true)
      .setValidated(true)
      .setThousandGenomes(true)
      .setSomatic(false)
      .setTranscriptEffects(ImmutableList.of(TranscriptEffect.newBuilder()
        .setEffects(ImmutableList.of("SO:0002012"))
        .setFeatureType("transcript")
        .setFeatureId("ENST00000396634.5")
        .build()))
      .build()))
    rdd.saveAsParquet(TestSaveArgs(path))

    val projection = Projection(
      ancestralAllele,
      alleleCount,
      readDepth,
      forwardReadDepth,
      reverseReadDepth,
      alleleFrequency,
      cigar,
      dbSnp,
      hapMap2,
      hapMap3,
      validated,
      thousandGenomes,
      somatic,
      transcriptEffects,
      attributes
    )

    val variantAnnotations: RDD[VariantAnnotation] = sc.loadParquet(path, projection = Some(projection))
    assert(variantAnnotations.count() === 1)
    assert(variantAnnotations.first.getAncestralAllele === "T")
    assert(variantAnnotations.first.getAlleleCount === 42)
    assert(variantAnnotations.first.getReadDepth === 10)
    assert(variantAnnotations.first.getForwardReadDepth === 4)
    assert(variantAnnotations.first.getReverseReadDepth === 13)
    assert(variantAnnotations.first.getAlleleFrequency === 20.0f)
    assert(variantAnnotations.first.getCigar === "M")
    assert(variantAnnotations.first.getDbSnp === true)
    assert(variantAnnotations.first.getHapMap2 === true)
    assert(variantAnnotations.first.getHapMap3 === true)
    assert(variantAnnotations.first.getValidated === true)
    assert(variantAnnotations.first.getThousandGenomes === true)
    assert(variantAnnotations.first.getSomatic === false)
    assert(variantAnnotations.first.getTranscriptEffects.get(0).getFeatureId === "ENST00000396634.5")
  }
}
