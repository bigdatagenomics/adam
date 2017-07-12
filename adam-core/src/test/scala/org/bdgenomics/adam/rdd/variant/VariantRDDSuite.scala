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
package org.bdgenomics.adam.rdd.variant

import htsjdk.variant.vcf.VCFHeaderLine
import org.bdgenomics.adam.models.{ ReferenceRegion, SequenceRecord }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite

class VariantRDDSuite extends ADAMFunSuite {

  sparkTest("union two variant rdds together") {
    val variant1 = sc.loadVariants(testFile("gvcf_dir/gvcf_multiallelic.g.vcf"))
    val variant2 = sc.loadVariants(testFile("small.vcf"))
    val union = variant1.union(variant2)
    assert(union.rdd.count === (variant1.rdd.count + variant2.rdd.count))
    assert(union.sequences.size === (variant1.sequences.size + variant2.sequences.size))
  }

  sparkTest("round trip to parquet") {
    val variants = sc.loadVariants(testFile("small.vcf"))
    val outputPath = tmpLocation()
    variants.saveAsParquet(outputPath)
    val unfilteredVariants = sc.loadVariants(outputPath)
    assert(unfilteredVariants.rdd.count === 6)

    val predicate = ReferenceRegion.toEnd("1", 752720L).toPredicate
    val filteredVariants = sc.loadParquetVariants(outputPath,
      optPredicate = Some(predicate))
    assert(filteredVariants.rdd.count === 2)
    val starts = filteredVariants.rdd.map(_.getStart).distinct.collect.toSet
    assert(starts.size === 2)
    assert(starts(752720L))
    assert(starts(752790L))
  }

  sparkTest("use broadcast join to pull down variants mapped to targets") {
    val variantsPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val variants = sc.loadVariants(variantsPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = variants.broadcastRegionJoin(targets)

    assert(jRdd.rdd.count === 3L)
  }

  sparkTest("use right outer broadcast join to pull down variants mapped to targets") {
    val variantsPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val variants = sc.loadVariants(variantsPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = variants.rightOuterBroadcastRegionJoin(targets)

    val c = jRdd.rdd.collect
    assert(c.count(_._1.isEmpty) === 3)
    assert(c.count(_._1.isDefined) === 3)
  }

  sparkTest("use shuffle join to pull down variants mapped to targets") {
    val variantsPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val variants = sc.loadVariants(variantsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = variants.shuffleRegionJoin(targets)
    val jRdd0 = variants.shuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    assert(jRdd.rdd.count === 3L)
    assert(jRdd0.rdd.count === 3L)
  }

  sparkTest("use right outer shuffle join to pull down variants mapped to targets") {
    val variantsPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val variants = sc.loadVariants(variantsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = variants.rightOuterShuffleRegionJoin(targets)
    val jRdd0 = variants.rightOuterShuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(_._1.isEmpty) === 3)
    assert(c0.count(_._1.isEmpty) === 3)
    assert(c.count(_._1.isDefined) === 3)
    assert(c0.count(_._1.isDefined) === 3)
  }

  sparkTest("use left outer shuffle join to pull down variants mapped to targets") {
    val variantsPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val variants = sc.loadVariants(variantsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = variants.leftOuterShuffleRegionJoin(targets)
    val jRdd0 = variants.leftOuterShuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(_._2.isEmpty) === 3)
    assert(c0.count(_._2.isEmpty) === 3)
    assert(c.count(_._2.isDefined) === 3)
    assert(c0.count(_._2.isDefined) === 3)
  }

  sparkTest("use full outer shuffle join to pull down variants mapped to targets") {
    val variantsPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val variants = sc.loadVariants(variantsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = variants.fullOuterShuffleRegionJoin(targets)
    val jRdd0 = variants.fullOuterShuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(t => t._1.isEmpty && t._2.isEmpty) === 0)
    assert(c0.count(t => t._1.isEmpty && t._2.isEmpty) === 0)
    assert(c.count(t => t._1.isDefined && t._2.isEmpty) === 3)
    assert(c0.count(t => t._1.isDefined && t._2.isEmpty) === 3)
    assert(c.count(t => t._1.isEmpty && t._2.isDefined) === 3)
    assert(c0.count(t => t._1.isEmpty && t._2.isDefined) === 3)
    assert(c.count(t => t._1.isDefined && t._2.isDefined) === 3)
    assert(c0.count(t => t._1.isDefined && t._2.isDefined) === 3)
  }

  sparkTest("use shuffle join with group by to pull down variants mapped to targets") {
    val variantsPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val variants = sc.loadVariants(variantsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = variants.shuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = variants.shuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.size === 3)
    assert(c0.size === 3)
    assert(c.forall(_._2.size == 1))
    assert(c0.forall(_._2.size == 1))
  }

  sparkTest("use right outer shuffle join with group by to pull down variants mapped to targets") {
    val variantsPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val variants = sc.loadVariants(variantsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = variants.rightOuterShuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = variants.rightOuterShuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 4)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect

    assert(c.count(_._1.isDefined) === 6)
    assert(c0.count(_._1.isDefined) === 6)
    assert(c.filter(_._1.isDefined).count(_._2.size == 1) === 3)
    assert(c0.filter(_._1.isDefined).count(_._2.size == 1) === 3)
    assert(c.filter(_._1.isDefined).count(_._2.isEmpty) === 3)
    assert(c0.filter(_._1.isDefined).count(_._2.isEmpty) === 3)
    assert(c.count(_._1.isEmpty) === 3)
    assert(c0.count(_._1.isEmpty) === 3)
    assert(c.filter(_._1.isEmpty).forall(_._2.size == 1))
    assert(c0.filter(_._1.isEmpty).forall(_._2.size == 1))
  }

  sparkTest("convert back to variant contexts") {
    val variantsPath = testFile("small.vcf")
    val variants = sc.loadVariants(variantsPath)
    val variantContexts = variants.toVariantContextRDD

    assert(variantContexts.sequences.containsRefName("1"))
    assert(variantContexts.samples.isEmpty)

    val vcs = variantContexts.rdd.collect
    assert(vcs.size === 6)

    val vc = vcs.head
    assert(vc.position.referenceName === "1")
    assert(vc.variant.variant.contigName === "1")
    assert(vc.genotypes.isEmpty)
  }

  sparkTest("load parquet to sql, save, re-read from avro") {
    def testMetadata(vRdd: VariantRDD) {
      val sequenceRdd = vRdd.addSequence(SequenceRecord("aSequence", 1000L))
      assert(sequenceRdd.sequences.containsRefName("aSequence"))

      val headerRdd = vRdd.addHeaderLine(new VCFHeaderLine("ABC", "123"))
      assert(headerRdd.headerLines.exists(_.getKey == "ABC"))
    }

    val inputPath = testFile("small.vcf")
    val outputPath = tmpLocation()
    val rrdd = sc.loadVariants(inputPath)
    testMetadata(rrdd)
    val rdd = rrdd.transformDataset(ds => ds) // no-op but force to sql
    testMetadata(rdd)
    assert(rdd.dataset.count === 6)
    assert(rdd.rdd.count === 6)
    rdd.saveAsParquet(outputPath)
    println(outputPath)
    val rdd2 = sc.loadVariants(outputPath)
    testMetadata(rdd2)
    assert(rdd2.rdd.count === 6)
    assert(rdd2.dataset.count === 6)
    val outputPath2 = tmpLocation()
    println(outputPath2)
    rdd.transform(rdd => rdd) // no-op but force to rdd
      .saveAsParquet(outputPath2)
    val rdd3 = sc.loadVariants(outputPath2)
    assert(rdd3.rdd.count === 6)
    assert(rdd3.dataset.count === 6)
  }
}
