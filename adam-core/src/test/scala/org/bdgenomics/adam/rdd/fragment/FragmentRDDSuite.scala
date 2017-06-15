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

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{
  RecordGroupDictionary,
  ReferenceRegion,
  SequenceDictionary
}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.{
  AlignmentRecordRDD,
  AnySAMOutFormatter,
  QualityScoreBin
}
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ Fragment, Feature, Strand }
import scala.collection.JavaConversions._

class FragmentRDDSuite extends ADAMFunSuite {

  /**
   * Class to test protected and private methods
   *
   * @param rdd An RDD of genomic Fragments.
   */
  private class TestFragments(override val rdd: RDD[Fragment])
      extends FragmentRDD(rdd, SequenceDictionary.empty, RecordGroupDictionary.empty) {

    /**
     * Gets the reference regions for a feature.
     *
     * @param fragment The fragment.
     * @param stranded True to report stranded data for each Feature.
     * @return Since a feature maps directly to a single genomic region, this
     *   method will always return a Seq of exactly one ReferenceRegion.
     */
    override def getReferenceRegions(
      fragment: Fragment,
      stranded: Boolean): Seq[ReferenceRegion] = {

      super.getReferenceRegions(fragment, stranded)
    }
  }

  sparkTest("don't lose any reads when piping interleaved fastq to sam") {
    // write suffixes at end of reads
    sc.hadoopConfiguration.setBoolean(FragmentRDD.WRITE_SUFFIXES, true)

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

  sparkTest("don't lose any reads when piping tab5 to sam") {
    val fragmentsPath = testFile("interleaved_fastq_sample1.ifq")
    val ardd = sc.loadFragments(fragmentsPath)
    val records = ardd.rdd.count
    assert(records === 3)

    implicit val tFormatter = Tab5InFormatter
    implicit val uFormatter = new AnySAMOutFormatter

    // this script converts tab5 to unaligned sam
    val scriptPath = testFile("tab5_to_usam.py")

    val pipedRdd: AlignmentRecordRDD = ardd.pipe("python $0",
      files = Seq(scriptPath))
    val newRecords = pipedRdd.rdd.count
    assert(2 * records === newRecords)
  }

  sparkTest("don't lose any reads when piping tab6 to sam") {
    // write suffixes at end of reads
    sc.hadoopConfiguration.setBoolean(FragmentRDD.WRITE_SUFFIXES, true)

    val fragmentsPath = testFile("interleaved_fastq_sample1.ifq")
    val ardd = sc.loadFragments(fragmentsPath)
    val records = ardd.rdd.count
    assert(records === 3)

    implicit val tFormatter = Tab6InFormatter
    implicit val uFormatter = new AnySAMOutFormatter

    // this script converts tab6 to unaligned sam
    val scriptPath = testFile("tab6_to_usam.py")

    val pipedRdd: AlignmentRecordRDD = ardd.pipe("python $0",
      files = Seq(scriptPath))
    val newRecords = pipedRdd.rdd.count
    assert(2 * records === newRecords)
  }

  sparkTest("getReferenceRegions with stranded set to true gets the stranded region") {
    val features = sc.loadFragments(testFile("small.1.sam"))

    val tempFeatures = new TestFragments(features.rdd)

    assert(!tempFeatures.rdd.collect
      .map(f => tempFeatures.getReferenceRegions(f, true))
      .exists(_.head.strand == Strand.INDEPENDENT))
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
    assert(jRdd0.rdd.partitions.length === 4)

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
    assert(jRdd0.rdd.partitions.length === 4)

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
    assert(jRdd0.rdd.partitions.length === 4)

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
    assert(jRdd0.rdd.partitions.length === 4)

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
    assert(jRdd0.rdd.partitions.length === 4)

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
    assert(jRdd0.rdd.partitions.length === 4)

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

  sparkTest("bin quality scores in fragments") {
    val fragments = sc.loadFragments(testFile("bqsr1.sam"))
    val binnedFragments = fragments.binQualityScores(Seq(QualityScoreBin(0, 20, 10),
      QualityScoreBin(20, 40, 30),
      QualityScoreBin(40, 60, 50)))
    val qualityScoreCounts = binnedFragments.rdd.flatMap(fragment => {
      fragment.getAlignments.toSeq
    }).flatMap(read => {
      read.getQual
    }).map(s => s.toInt - 33)
      .countByValue

    assert(qualityScoreCounts(30) === 92899)
    assert(qualityScoreCounts(10) === 7101)
  }

  sparkTest("union two rdds of fragments together") {
    val reads1 = sc.loadAlignments(testFile("bqsr1.sam")).toFragments
    val reads2 = sc.loadAlignments(testFile("small.sam")).toFragments
    val union = reads1.union(reads2)
    assert(union.rdd.count === (reads1.rdd.count + reads2.rdd.count))
    // all of the contigs small.sam has are in bqsr1.sam
    assert(union.sequences.size === reads1.sequences.size)
    // small.sam has no record groups
    assert(union.recordGroups.size === reads1.recordGroups.size)
  }
}
