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
package org.bdgenomics.adam.rdd.fragment

import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.{ AlignmentRecordRDD, AnySAMOutFormatter }
import org.bdgenomics.adam.util.ADAMFunSuite

class FragmentRDDSuite extends ADAMFunSuite {

  sparkTest("don't lose any reads when piping interleaved fastq to sam") {
    // write suffixes at end of reads
    sc.hadoopConfiguration.setBoolean(InterleavedFASTQInFormatter.WRITE_SUFFIXES, true)

    val fragmentsPath = testFile("interleaved_fastq_sample1.ifq")
    val ardd = sc.loadFragments(fragmentsPath)
    val records = ardd.rdd.count
    assert(records === 3)

    implicit val tFormatter = InterleavedFASTQInFormatter
    implicit val uFormatter = new AnySAMOutFormatter

    // this script converts interleaved fastq to unaligned sam
    val scriptPath = testFile("fastq_to_usam.py")

    val pipedRdd: AlignmentRecordRDD = ardd.pipe("python $0",
      files = Seq(scriptPath))
    val newRecords = pipedRdd.rdd.count
    assert(2 * records === newRecords)
  }

  sparkTest("use broadcast join to pull down fragments mapped to targets") {
    val fragmentsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val fragments = sc.loadFragments(fragmentsPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = fragments.broadcastRegionJoin(targets)

    assert(jRdd.rdd.count === 5)
  }

  sparkTest("use right outer broadcast join to pull down fragments mapped to targets") {
    val fragmentsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val fragments = sc.loadFragments(fragmentsPath)
    val targets = sc.loadFeatures(targetsPath)

    val jRdd = fragments.rightOuterBroadcastRegionJoin(targets)

    val c = jRdd.rdd.collect
    assert(c.count(_._1.isEmpty) === 1)
    assert(c.count(_._1.isDefined) === 5)
  }

  sparkTest("use shuffle join to pull down fragments mapped to targets") {
    val fragmentsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val fragments = sc.loadFragments(fragmentsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = fragments.shuffleRegionJoin(targets)
    val jRdd0 = fragments.shuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 5)

    assert(jRdd.rdd.count === 5)
    assert(jRdd0.rdd.count === 5)
  }

  sparkTest("use right outer shuffle join to pull down fragments mapped to targets") {
    val fragmentsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val fragments = sc.loadFragments(fragmentsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = fragments.rightOuterShuffleRegionJoin(targets)
    val jRdd0 = fragments.rightOuterShuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(_._1.isEmpty) === 1)
    assert(c0.count(_._1.isEmpty) === 1)
    assert(c.count(_._1.isDefined) === 5)
    assert(c0.count(_._1.isDefined) === 5)
  }

  sparkTest("use left outer shuffle join to pull down fragments mapped to targets") {
    val fragmentsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val fragments = sc.loadFragments(fragmentsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = fragments.leftOuterShuffleRegionJoin(targets)
    val jRdd0 = fragments.leftOuterShuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(_._2.isEmpty) === 15)
    assert(c0.count(_._2.isEmpty) === 15)
    assert(c.count(_._2.isDefined) === 5)
    assert(c0.count(_._2.isDefined) === 5)
  }

  sparkTest("use full outer shuffle join to pull down fragments mapped to targets") {
    val fragmentsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val fragments = sc.loadFragments(fragmentsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = fragments.fullOuterShuffleRegionJoin(targets)
    val jRdd0 = fragments.fullOuterShuffleRegionJoin(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(t => t._1.isEmpty && t._2.isEmpty) === 0)
    assert(c0.count(t => t._1.isEmpty && t._2.isEmpty) === 0)
    assert(c.count(t => t._1.isDefined && t._2.isEmpty) === 15)
    assert(c0.count(t => t._1.isDefined && t._2.isEmpty) === 15)
    assert(c.count(t => t._1.isEmpty && t._2.isDefined) === 1)
    assert(c0.count(t => t._1.isEmpty && t._2.isDefined) === 1)
    assert(c.count(t => t._1.isDefined && t._2.isDefined) === 5)
    assert(c0.count(t => t._1.isDefined && t._2.isDefined) === 5)
  }

  sparkTest("use shuffle join with group by to pull down fragments mapped to targets") {
    val fragmentsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val fragments = sc.loadFragments(fragmentsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = fragments.shuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = fragments.shuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.size === 5)
    assert(c0.size === 5)
    assert(c.forall(_._2.size == 1))
    assert(c0.forall(_._2.size == 1))
  }

  sparkTest("use right outer shuffle join with group by to pull down fragments mapped to targets") {
    val fragmentsPath = testFile("small.1.sam")
    val targetsPath = testFile("small.1.bed")

    val fragments = sc.loadFragments(fragmentsPath)
      .transform(_.repartition(1))
    val targets = sc.loadFeatures(targetsPath)
      .transform(_.repartition(1))

    val jRdd = fragments.rightOuterShuffleRegionJoinAndGroupByLeft(targets)
    val jRdd0 = fragments.rightOuterShuffleRegionJoinAndGroupByLeft(targets, optPartitions = Some(4))

    // we can't guarantee that we get exactly the number of partitions requested,
    // we get close though
    assert(jRdd.rdd.partitions.length === 1)
    assert(jRdd0.rdd.partitions.length === 5)

    val c = jRdd.rdd.collect
    val c0 = jRdd0.rdd.collect
    assert(c.count(_._1.isDefined) === 20)
    assert(c0.count(_._1.isDefined) === 20)
    assert(c.filter(_._1.isDefined).count(_._2.size == 1) === 5)
    assert(c0.filter(_._1.isDefined).count(_._2.size == 1) === 5)
    assert(c.filter(_._1.isDefined).count(_._2.isEmpty) === 15)
    assert(c0.filter(_._1.isDefined).count(_._2.isEmpty) === 15)
    assert(c.count(_._1.isEmpty) === 1)
    assert(c0.count(_._1.isEmpty) === 1)
    assert(c.filter(_._1.isEmpty).forall(_._2.size == 1))
    assert(c0.filter(_._1.isEmpty).forall(_._2.size == 1))
  }
}
