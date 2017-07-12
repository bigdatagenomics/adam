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
package org.bdgenomics.adam.rdd.feature

import org.bdgenomics.adam.models.{
  ReferenceRegion,
  Coverage,
  SequenceDictionary,
  SequenceRecord
}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.Feature

class CoverageRDDSuite extends ADAMFunSuite {

  val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 2000L)))

  def generateCoverage(length: Int): Seq[Coverage] = {
    // generate adjacent regions with coverage
    var j = 0
    val coverage =
      (0 until length).map(i => {
        if ((i % 4) == 0) {
          j = j + 1
        }
        Coverage("chr1", i, i + 1, j.toDouble)
      })
    coverage.toSeq
  }

  sparkTest("correctly saves coverage") {
    def testMetadata(cRdd: CoverageRDD) {
      val sequenceRdd = cRdd.addSequence(SequenceRecord("aSequence", 1000L))
      assert(sequenceRdd.sequences.containsRefName("aSequence"))
    }

    val f1 = Feature.newBuilder().setContigName("chr1").setStart(1).setEnd(10).setScore(3.0).build()
    val f2 = Feature.newBuilder().setContigName("chr1").setStart(15).setEnd(20).setScore(2.0).build()
    val f3 = Feature.newBuilder().setContigName("chr2").setStart(15).setEnd(20).setScore(2.0).build()

    val featureRDD: FeatureRDD = FeatureRDD(sc.parallelize(Seq(f1, f2, f3)))
    val coverageRDD: CoverageRDD = featureRDD.toCoverage
    testMetadata(coverageRDD)

    val outputFile = tmpLocation(".bed")
    coverageRDD.save(outputFile, false, false)

    val coverage = sc.loadCoverage(outputFile)
    testMetadata(coverage)
    assert(coverage.rdd.count == 3)
    assert(coverage.dataset.count == 3)

    // go to dataset and save as parquet
    val outputFile2 = tmpLocation(".adam")
    val dsCov = coverageRDD.transformDataset(ds => ds)
    testMetadata(dsCov)
    dsCov.save(outputFile2, false, false)
    val coverage2 = sc.loadCoverage(outputFile2)
    testMetadata(coverage2)
    assert(coverage2.rdd.count == 3)
    assert(coverage2.dataset.count == 3)

    // load as features, force to dataset, convert to coverage, and count
    val features2Ds = sc.loadFeatures(outputFile2)
      .transformDataset(ds => ds) // no-op, force to dataset
    val coverage2Ds = features2Ds.toCoverage
    assert(coverage2Ds.rdd.count == 3)
    assert(coverage2Ds.dataset.count == 3)

    // translate to features and count
    val features2 = coverage2.toFeatureRDD
    assert(features2.rdd.count == 3)
    assert(features2.dataset.count == 3)

    // go to rdd and save as parquet
    val outputFile3 = tmpLocation(".adam")
    coverageRDD.transform(rdd => rdd).save(outputFile3, false, false)
    val coverage3 = sc.loadCoverage(outputFile3)
    assert(coverage3.rdd.count == 3)
    assert(coverage3.dataset.count == 3)
  }

  sparkTest("can read a bed file to coverage") {
    val inputPath = testFile("sample_coverage.bed")
    val coverage = sc.loadCoverage(inputPath)
    assert(coverage.rdd.count() == 3)
    assert(coverage.dataset.count() == 3)
    val selfUnion = coverage.union(coverage)
    assert(selfUnion.rdd.count === 6)
    val coverageDs = coverage.transformDataset(ds => ds) // no-op, forces to dataset
    val selfUnionDs = coverageDs.union(coverageDs)
    assert(selfUnionDs.rdd.count === 6)
  }

  sparkTest("correctly filters coverage with predicate") {
    val f1 = Feature.newBuilder().setContigName("chr1").setStart(1).setEnd(10).setScore(3.0).build()
    val f2 = Feature.newBuilder().setContigName("chr1").setStart(15).setEnd(20).setScore(2.0).build()
    val f3 = Feature.newBuilder().setContigName("chr2").setStart(15).setEnd(20).setScore(2.0).build()

    val featureRDD: FeatureRDD = FeatureRDD(sc.parallelize(Seq(f1, f2, f3)))
    val coverageRDD: CoverageRDD = featureRDD.toCoverage

    val outputFile = tmpLocation(".adam")
    coverageRDD.save(outputFile, false, false)

    val region = ReferenceRegion("chr1", 1, 9)
    val predicate = region.toPredicate
    val coverage = sc.loadParquetCoverage(outputFile, Some(predicate))
    assert(coverage.rdd.count == 1)
  }

  sparkTest("correctly flatmaps coverage without aggregated bins") {
    val f1 = Feature.newBuilder().setContigName("chr1").setStart(1).setEnd(5).setScore(1.0).build()
    val f2 = Feature.newBuilder().setContigName("chr1").setStart(5).setEnd(7).setScore(3.0).build()
    val f3 = Feature.newBuilder().setContigName("chr1").setStart(7).setEnd(20).setScore(4.0).build()

    val featureRDD: FeatureRDD = FeatureRDD(sc.parallelize(Seq(f1, f2, f3)))
    val coverageRDD: CoverageRDD = featureRDD.toCoverage
    val coverage = coverageRDD.coverage(bpPerBin = 4)

    assert(coverage.rdd.count == 4)
  }

  sparkTest("correctly flatmaps coverage with aggregated bins") {
    val f1 = Feature.newBuilder().setContigName("chr1").setStart(1).setEnd(5).setScore(1.0).build()
    val f2 = Feature.newBuilder().setContigName("chr1").setStart(5).setEnd(7).setScore(3.0).build()
    val f3 = Feature.newBuilder().setContigName("chr1").setStart(7).setEnd(20).setScore(4.0).build()

    val featureRDD: FeatureRDD = FeatureRDD(sc.parallelize(Seq(f1, f2, f3)))
    val coverageRDD: CoverageRDD = featureRDD.toCoverage

    val coverage = coverageRDD
      .aggregatedCoverage(bpPerBin = 4)

    assert(coverage.rdd.count == 5)
    assert(coverage.rdd.filter(_.start == 4).first.count == 2.75)
    assert(coverage.rdd.filter(_.start == 8).first.count == 4.0)
  }

  sparkTest("collapses coverage records in one partition") {
    val cov = generateCoverage(20)
    val coverage = RDDBoundCoverageRDD(sc.parallelize(cov.toSeq).repartition(1), sd, None)
    val collapsed = coverage.collapse

    assert(coverage.rdd.count == 20)
    assert(collapsed.rdd.count == 5)
  }

  sparkTest("approximately collapses coverage records in multiple partitions") {
    val cov = generateCoverage(20)
    val coverage = RDDBoundCoverageRDD(sc.parallelize(cov), sd, None)
    val collapsed = coverage.collapse

    assert(collapsed.rdd.count == 8)
  }
}

