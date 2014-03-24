/*
 * Copyright (c) 2013-2014. Regents of the University of California
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

package edu.berkeley.cs.amplab.adam.algorithms.realignmenttarget

import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.models.ReferenceRegion
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import edu.berkeley.cs.amplab.adam.rdd.RealignIndels
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import edu.berkeley.cs.amplab.adam.util.{MdTag, SparkFunSuite}
import org.apache.spark.{TaskContext, Partition}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import parquet.filter.UnboundRecordFilter
import scala.collection.immutable.{NumericRange, TreeSet}

class IndelRealignmentTargetSuite extends SparkFunSuite {

  // Note: this can't be lazy vals because Spark won't find the RDDs after the first test
  def mason_reads: RDD[RichADAMRecord] = {
    val path = ClassLoader.getSystemClassLoader.getResource("small_realignment_targets.sam").getFile
    sc.adamLoad[ADAMRecord, UnboundRecordFilter](path).map(RichADAMRecord(_))
  }

  def artificial_reads: RDD[RichADAMRecord] = {
    val path = ClassLoader.getSystemClassLoader.getResource("artificial.sam").getFile
    sc.adamLoad[ADAMRecord, UnboundRecordFilter](path).map(RichADAMRecord(_))
  }

  def make_read(start : Long, cigar : String, mdtag : String, length : Int, id : Int = 0) : RichADAMRecord = {
    val sequence : String = "A" * length
    RichADAMRecord(ADAMRecord.newBuilder()
      .setReadName("read" + id.toString)
      .setStart(start)
      .setReadMapped(true)
      .setCigar(cigar)
      .setSequence(sequence)
      .setReadNegativeStrand(false)
      .setMapq(60)
      .setQual(sequence) // no typo, we just don't care
      .setReferenceId(1)
      .setMismatchingPositions(mdtag)
      .build())
  }

  sparkTest("checking simple realignment target") {
    val target1 = new IndelRealignmentTarget(Some(ReferenceRegion(1, 1, 10)),
                                             ReferenceRegion(1, 1, 51))
    val target2 = new IndelRealignmentTarget(None,
                                             ReferenceRegion(1, 60, 91))
    assert(target1.readRange.start === 1)
    assert(target1.readRange.end === 51)
    assert(TargetOrdering.overlap(target1, target1) === true)
    assert(TargetOrdering.overlap(target1, target2) === false)
    assert(target2.readRange.start === 60)
    assert(target2.readRange.end === 91)
    assert(!target1.isEmpty)
    assert(target2.isEmpty)
  }

  sparkTest("creating simple target from read with deletion") {
    val read = make_read(3L, "2M3D2M", "2^AAA2", 4)
    val read_rdd : RDD[RichADAMRecord] = sc.makeRDD(Seq(read), 1)
    val targets = RealignmentTargetFinder(read_rdd)
    assert(targets != null)
    assert(targets.size === 1)
    assert(targets.head.variation.get.start === 5)
    assert(targets.head.variation.get.end === 8)
    assert(targets.head.readRange.start === 3)
    assert(targets.head.readRange.end === 10)
  }

  sparkTest("creating simple target from read with insertion") {
    val read = make_read(3L, "2M3I2M", "4", 7)
    val read_rdd : RDD[RichADAMRecord] = sc.makeRDD(Seq(read), 1)
    val targets = RealignmentTargetFinder(read_rdd)
    assert(targets != null)
    assert(targets.size === 1)
    assert(targets.head.variation.get.start === 5)
    assert(targets.head.variation.get.end === 6)
    assert(targets.head.readRange.start === 3)
    assert(targets.head.readRange.end === 7)
  }

  sparkTest("joining simple realignment targets on same chr") {
    val target1 = new IndelRealignmentTarget(Some(ReferenceRegion(1, 10, 16)),
                                             ReferenceRegion(1, 1, 21))
    val target2 = new IndelRealignmentTarget(Some(ReferenceRegion(1, 10, 16)),
                                             ReferenceRegion(1, 6, 26))
    val merged_target = target1.merge(target2)
    assert(merged_target.readRange.start === 1)
    assert(merged_target.readRange.end === 26)
    assert(merged_target.variation.get.start === 10)
    assert(merged_target.variation.get.end === 16)
  }

  sparkTest("joining simple realignment targets on different chr throws exception") {
    val target1 = new IndelRealignmentTarget(Some(ReferenceRegion(1, 10, 16)),
                                             ReferenceRegion(1, 1, 21))
    val target2 = new IndelRealignmentTarget(Some(ReferenceRegion(2, 10, 16)),
                                             ReferenceRegion(2, 6, 26))
    
    intercept[AssertionError] {
      val merged_target = target1.merge(target2)
    }
  }

  sparkTest("creating targets from three intersecting reads, same indel") {
    val read1 = make_read(1L, "4M3D2M", "4^AAA2", 6)
    val read2 = make_read(2L, "3M3D2M", "3^AAA2", 5)
    val read3 = make_read(3L, "2M3D2M", "2^AAA2", 4)
    val read_rdd : RDD[RichADAMRecord] = sc.makeRDD(Seq(read1, read2, read3), 1)
    val targets = RealignmentTargetFinder(read_rdd)
    assert(targets != null)
    assert(targets.size === 1)
    assert(targets.head.variation.get.start === 5)
    assert(targets.head.variation.get.end === 8)
    assert(targets.head.readRange.start === 1)
    assert(targets.head.readRange.end === 10)
  }

  sparkTest("creating targets from three intersecting reads, two different indel") {
    val read1 = make_read(1L, "2M2D4M", "2^AA4", 6, 0)
    val read2 = make_read(1L, "2M2D2M2D2M", "2^AA2^AA2", 6, 1)
    val read3 = make_read(5L, "2M2D4M", "2^AA4", 6, 2)
    val read_rdd : RDD[RichADAMRecord] = sc.makeRDD(Seq(read1, read2, read3), 1)
    val targets = RealignmentTargetFinder(read_rdd)

    assert(targets != null)
    assert(targets.size === 1)
    assert(targets.head.variation.get.start === 3)
    assert(targets.head.variation.get.end === 9)
    assert(targets.head.readRange.start === 1)
    assert(targets.head.readRange.end === 13)
  }

  sparkTest("creating targets from two disjoint reads") {
    val read1 = make_read(1L, "2M2D2M", "2^AA2", 4)
    val read2 = make_read(7L, "2M2D2M", "2^AA2", 4)
    val read_rdd : RDD[RichADAMRecord] = sc.makeRDD(Seq(read1, read2), 1)
    val targets = RealignmentTargetFinder(read_rdd).toArray
    assert(targets != null)
    assert(targets.size === 2)
    assert(targets(0).variation.get.start === 3)
    assert(targets(0).variation.get.end === 5)
    assert(targets(0).readRange.start === 1)
    assert(targets(0).readRange.end === 7)
    assert(targets(1).variation.get.start === 9)
    assert(targets(1).variation.get.end === 11)
    assert(targets(1).readRange.start === 7)
    assert(targets(1).readRange.end === 13)
  }

  sparkTest("creating targets for artificial reads: one-by-one") {
    def check_indel(target : IndelRealignmentTarget, read : ADAMRecord) : Boolean = {
      val indelRange : ReferenceRegion = target.variation.get
      read.getStart.toLong match {
        case 5L => ((indelRange.start == 34) && (indelRange.end == 44))
        case 10L => ((indelRange.start == 54) && (indelRange.end == 64))
        case 15L => ((indelRange.start == 34) && (indelRange.end == 44))
        case 20L => ((indelRange.start == 54) && (indelRange.end == 64))
        case 25L => ((indelRange.start == 34) && (indelRange.end == 44))
        case _ => false
      }
    }

    val reads = artificial_reads.collect()
    reads.foreach(
      read => {
        val read_rdd : RDD[RichADAMRecord] = sc.makeRDD(Seq(read), 1)
        val targets = RealignmentTargetFinder(read_rdd)
        if (read.getStart < 105) {
          assert(targets != null)
          assert(targets.size === 1) // the later read mates do not have indels
          assert(targets.head.readRange.start === read.getStart)
          assert(targets.head.readRange.end === read.end.get)
          assert(check_indel(targets.head, read))
        }
      }
    )
  }

  sparkTest("creating targets for artificial reads: all-at-once (merged)") {
    val targets_collected : Array[IndelRealignmentTarget] = RealignmentTargetFinder(artificial_reads).toArray

    assert(targets_collected.size === 1)
    assert(targets_collected.head.readRange.start === 5)
    assert(targets_collected.head.readRange.end === 95)
    assert(targets_collected.head.variation.get.start === 34)
    assert(targets_collected.head.variation.get.end === 64)
  }

  sparkTest("creating indel targets for mason reads") {
    val targets_collected : Array[IndelRealignmentTarget] = RealignmentTargetFinder(mason_reads).toArray

    // the first read has no indels
    // the second read has a one-base deletion and a one-base insertion
    assert(targets_collected(0).variation.get.start == 702289 && targets_collected(0).variation.get.end == 702324)
    // the third read has a one base deletion
    assert(targets_collected(1).variation.get.start == 807755 && targets_collected(1).variation.get.end == 807756)
    // read 7 has a single 4 bp deletion
    assert(targets_collected(5).variation.get.length === 4)
    assert(targets_collected(5).variation.get.start == 869644 && targets_collected(5).variation.get.end == 869648)
  }
}
