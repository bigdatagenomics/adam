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

import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite

class GenotypeRDDSuite extends ADAMFunSuite {

  // these tests will all fail until https://github.com/bigdatagenomics/adam/pull/1291 merges
  ignore("use broadcast join to pull down genotypes mapped to targets") {
    val genotypesPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val genotypes = sc.loadGenotypes(genotypesPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = genotypes.broadcastRegionJoin(targets)

    assert(jRdd.rdd.count === 9L)
  }

  ignore("use right outer broadcast join to pull down genotypes mapped to targets") {
    val genotypesPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val genotypes = sc.loadGenotypes(genotypesPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = genotypes.rightOuterBroadcastRegionJoin(targets)

    val c = jRdd.rdd.collect
    assert(c.count(_._1.isEmpty) === 3)
    assert(c.count(_._1.isDefined) === 9)
  }

  ignore("use shuffle join to pull down genotypes mapped to targets") {
    val genotypesPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val genotypes = sc.loadGenotypes(genotypesPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = genotypes.shuffleRegionJoin(targets)
    val jRdd0 = genotypes.shuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 5)

    assert(jRdd.rdd.count === 3L)
    assert(jRdd0.rdd.count === 3L)
  }

  ignore("use right outer shuffle join to pull down genotypes mapped to targets") {
    val genotypesPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val genotypes = sc.loadGenotypes(genotypesPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = genotypes.rightOuterShuffleRegionJoin(targets)
    val jRdd0 = genotypes.rightOuterShuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(_._1.isEmpty) === 3)
    assert(c0.count(_._1.isEmpty) === 3)
    assert(c.count(_._1.isDefined) === 9)
    assert(c0.count(_._1.isDefined) === 9)
  }

  ignore("use left outer shuffle join to pull down genotypes mapped to targets") {
    val genotypesPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val genotypes = sc.loadGenotypes(genotypesPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = genotypes.leftOuterShuffleRegionJoin(targets)
    val jRdd0 = genotypes.leftOuterShuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(_._2.isEmpty) === 9)
    assert(c0.count(_._2.isEmpty) === 9)
    assert(c.count(_._2.isDefined) === 9)
    assert(c0.count(_._2.isDefined) === 9)
  }

  ignore("use full outer shuffle join to pull down genotypes mapped to targets") {
    val genotypesPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val genotypes = sc.loadGenotypes(genotypesPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = genotypes.fullOuterShuffleRegionJoin(targets)
    val jRdd0 = genotypes.fullOuterShuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(t => t._1.isEmpty && t._2.isEmpty) === 0)
    assert(c0.count(t => t._1.isEmpty && t._2.isEmpty) === 0)
    assert(c.count(t => t._1.isDefined && t._2.isEmpty) === 9)
    assert(c0.count(t => t._1.isDefined && t._2.isEmpty) === 9)
    assert(c.count(t => t._1.isEmpty && t._2.isDefined) === 3)
    assert(c0.count(t => t._1.isEmpty && t._2.isDefined) === 3)
    assert(c.count(t => t._1.isDefined && t._2.isDefined) === 9)
    assert(c0.count(t => t._1.isDefined && t._2.isDefined) === 9)
  }

  ignore("use shuffle join with group by to pull down genotypes mapped to targets") {
    val genotypesPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val genotypes = sc.loadGenotypes(genotypesPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = genotypes.shuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = genotypes.shuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.size === 9)
    assert(c0.size === 9)
    assert(c.forall(_._2.size == 1))
    assert(c0.forall(_._2.size == 1))
  }

  ignore("use right outer shuffle join with group by to pull down genotypes mapped to targets") {
    val genotypesPath = testFile("small.vcf")
    val targetsPath = testFile("small.1.bed")

    val genotypes = sc.loadGenotypes(genotypesPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = genotypes.rightOuterShuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = genotypes.rightOuterShuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 5)

    val c = jRdd0.rdd.collect
    val c0 = jRdd0.rdd.collect

    assert(c.count(_._1.isDefined) === 18)
    assert(c0.count(_._1.isDefined) === 18)
    assert(c.filter(_._1.isDefined).count(_._2.size == 1) === 9)
    assert(c0.filter(_._1.isDefined).count(_._2.size == 1) === 9)
    assert(c.filter(_._1.isDefined).count(_._2.isEmpty) === 9)
    assert(c0.filter(_._1.isDefined).count(_._2.isEmpty) === 9)
    assert(c.count(_._1.isEmpty) === 3)
    assert(c0.count(_._1.isEmpty) === 3)
    assert(c.filter(_._1.isEmpty).forall(_._2.size == 1))
    assert(c0.filter(_._1.isEmpty).forall(_._2.size == 1))
  }
}
