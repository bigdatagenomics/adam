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

import edu.berkeley.cs.amplab.adam.util.{MdTag, SparkFunSuite}
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import org.apache.spark.SparkContext._
import parquet.filter.UnboundRecordFilter
import scala.collection.immutable.{NumericRange, TreeSet}
import edu.berkeley.cs.amplab.adam.rdd.RealignIndels
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import org.apache.spark.{TaskContext, Partition}
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._

class IndelRealignmentTargetSuite extends SparkFunSuite {

  // Note: this can't be lazy vals because Spark won't find the RDDs after the first test
  def mason_reads: RDD[ADAMRecord] = {
    val path = ClassLoader.getSystemClassLoader.getResource("small_realignment_targets.sam").getFile
    sc.adamLoad[ADAMRecord, UnboundRecordFilter](path)
  }

  def artificial_reads: RDD[ADAMRecord] = {
    val path = ClassLoader.getSystemClassLoader.getResource("artificial.sam").getFile
    sc.adamLoad[ADAMRecord, UnboundRecordFilter](path)
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
      .setMismatchingPositions(mdtag)
      .build())
  }

  sparkTest("checking simple ranges") {
    val range1 : IndelRange = new IndelRange(new NumericRange.Inclusive[Long](1, 4, 1),
      new NumericRange.Inclusive[Long](1, 10, 1)
    )
    val range2 : IndelRange = new IndelRange(new NumericRange.Inclusive[Long](1, 4, 1),
      new NumericRange.Inclusive[Long](40, 50, 1)
    )
    val range3 : SNPRange = new SNPRange(5, new NumericRange.Inclusive[Long](40, 50, 1))

    assert(range1 != range2)
    assert(range1.compareRange(range2) === 0)
    assert(range1.compare(range2) === -1)
    assert(range1.compareReadRange(range2) === -1)
    assert(range2.compareReadRange(range3) === 0)
    assert(range1.merge(range2).getReadRange().start === 1)
    assert(range1.merge(range2).getReadRange().end === 50)
  }

  sparkTest("checking simple realignment target") {
    val range1 : IndelRange = new IndelRange(new NumericRange.Inclusive[Long](1, 4, 1),
      new NumericRange.Inclusive[Long](1, 10, 1)
    )
    val range2 : IndelRange = new IndelRange(new NumericRange.Inclusive[Long](1, 4, 1),
      new NumericRange.Inclusive[Long](40, 50, 1)
    )
    val range3 : IndelRange = new IndelRange(new NumericRange.Inclusive[Long](6, 14, 1),
      new NumericRange.Inclusive[Long](80, 90, 1)
    )
    val range4 : IndelRange = new IndelRange(new NumericRange.Inclusive[Long](2, 4, 1),
      new NumericRange.Inclusive[Long](60, 70, 1)
    )

    val indelRanges1 = (range1 :: range2 :: List()).toSet
    val target1 = new IndelRealignmentTarget(indelRanges1, Set.empty[SNPRange])
    val indelRanges2 = (range3 :: range4 :: List()).toSet
    val target2 = new IndelRealignmentTarget(indelRanges2, Set.empty[SNPRange])
    assert(target1.readRange.start === 1)
    assert(target1.readRange.end === 50)
    assert(TargetOrdering.overlap(target1, target1) === true)
    assert(TargetOrdering.overlap(target1, target2) === false)
    assert(target2.getReadRange().start === 60)
    assert(target2.getReadRange().end === 90)
  }

  sparkTest("creating simple target from read with deletion") {
    val read = make_read(3L, "2M3D2M", "2^AAA2", 4)
    val read_rdd : RDD[RichADAMRecord] = sc.makeRDD(Seq(read), 1)
    val targets = RealignmentTargetFinder(read_rdd)
    assert(targets != null)
    assert(targets.size === 1)
    assert(targets.head.getIndelSet().head.indelRange.start === 5)
    assert(targets.head.getIndelSet().head.indelRange.end === 7)
    assert(targets.head.getIndelSet().head.readRange.start === 3)
    assert(targets.head.getIndelSet().head.readRange.end === 9)
  }

  sparkTest("creating simple target from read with insertion") {
    val read = make_read(3L, "2M3I2M", "4", 7)
    val read_rdd : RDD[RichADAMRecord] = sc.makeRDD(Seq(read), 1)
    val targets = RealignmentTargetFinder(read_rdd)
    assert(targets != null)
    assert(targets.size === 1)
    assert(targets.head.getIndelSet().head.indelRange.start === 5)
    assert(targets.head.getIndelSet().head.indelRange.end === 5)
    assert(targets.head.getIndelSet().head.readRange.start === 3)
    assert(targets.head.getIndelSet().head.readRange.end === 6)
  }

  sparkTest("joining simple realignment targets") {
    val range1 : IndelRange = new IndelRange(new NumericRange.Inclusive[Long](10, 15, 1),
      new NumericRange.Inclusive[Long](1, 20, 1)
    )
    val range2 : IndelRange = new IndelRange(new NumericRange.Inclusive[Long](10, 15, 1),
      new NumericRange.Inclusive[Long](6, 25, 1)
    )
    val target1 = new IndelRealignmentTarget((range1 :: List()).toSet, Set.empty[SNPRange])
    val target2 = new IndelRealignmentTarget((range2 :: List()).toSet, Set.empty[SNPRange])
    val merged_target = target1.merge(target2)
    assert(merged_target.getReadRange().start === 1)
    assert(merged_target.getReadRange().end === 25)
    assert(merged_target.getIndelSet().toArray.apply(0).indelRange.start === 10)
    assert(merged_target.getIndelSet().toArray.apply(0).indelRange.end === 15)
    assert(merged_target.getIndelSet().toArray.apply(0).readRange.start === 1)
    assert(merged_target.getIndelSet().toArray.apply(0).readRange.end === 25)
  }

  sparkTest("creating targets from three intersecting reads, same indel") {
    val read1 = make_read(1L, "4M3D2M", "4^AAA2", 6)
    val read2 = make_read(2L, "3M3D2M", "3^AAA2", 5)
    val read3 = make_read(3L, "2M3D2M", "2^AAA2", 4)
    val read_rdd : RDD[RichADAMRecord] = sc.makeRDD(Seq(read1, read2, read3), 1)
    val targets = RealignmentTargetFinder(read_rdd)
    assert(targets != null)
    assert(targets.size === 1)
    assert(targets.head.getIndelSet().head.indelRange.start === 5)
    assert(targets.head.getIndelSet().head.indelRange.end === 7)
    assert(targets.head.getIndelSet().head.readRange.start === 1)
    assert(targets.head.getIndelSet().head.readRange.end === 9)
  }

  sparkTest("creating targets from three intersecting reads, two different indel") {
    val read1 = make_read(1L, "2M2D4M", "2^AA4", 6, 0)
    val read2 = make_read(1L, "2M2D2M2D2M", "2^AA2^AA2", 6, 1)
    val read3 = make_read(5L, "2M2D4M", "2^AA4", 6, 2)
    val read_rdd : RDD[RichADAMRecord] = sc.makeRDD(Seq(read1, read2, read3), 1)
    val targets = RealignmentTargetFinder(read_rdd)
    assert(targets != null)
    assert(targets.size === 1)
    val indels = targets.head.indelSet.toArray
    assert(indels(0).indelRange.start === 3)
    assert(indels(0).indelRange.end === 4)
    assert(indels(0).readRange.start === 1)
    assert(indels(0).readRange.end === 10)
    assert(indels(1).indelRange.start === 7)
    assert(indels(1).indelRange.end === 8)
    assert(indels(1).readRange.start === 1)
    assert(indels(1).readRange.end === 12)
    assert(targets.head.getReadRange().start === 1)
    assert(targets.head.getReadRange().end === 12)
  }

  sparkTest("creating targets from two disjoint reads") {
    val read1 = make_read(1L, "2M2D2M", "2^AA2", 4)
    val read2 = make_read(7L, "2M2D2M", "2^AA2", 4)
    val read_rdd : RDD[RichADAMRecord] = sc.makeRDD(Seq(read1, read2), 1)
    val targets = RealignmentTargetFinder(read_rdd).toArray
    assert(targets != null)
    assert(targets.size === 2)
    assert(targets(0).getIndelSet().head.indelRange.start === 3)
    assert(targets(0).getIndelSet().head.indelRange.end === 4)
    assert(targets(0).getIndelSet().head.readRange.start === 1)
    assert(targets(0).getIndelSet().head.readRange.end === 6)
    assert(targets(1).getIndelSet().head.indelRange.start === 9)
    assert(targets(1).getIndelSet().head.indelRange.end === 10)
    assert(targets(1).getIndelSet().head.readRange.start === 7)
    assert(targets(1).getIndelSet().head.readRange.end === 12)
  }

  sparkTest("creating targets for artificial reads: one-by-one") {
    def check_indel(target : IndelRealignmentTarget, read : RichADAMRecord) : Boolean = {
      val indelRange : NumericRange[Long] = target.indelSet.head.getIndelRange()
      read.record.getStart.toLong match {
        case 5L => ((indelRange.start == 34) && (indelRange.end == 43))
        case 10L => ((indelRange.start == 54) && (indelRange.end == 63))
        case 15L => ((indelRange.start == 34) && (indelRange.end == 43))
        case 20L => ((indelRange.start == 54) && (indelRange.end == 63))
        case 25L => ((indelRange.start == 34) && (indelRange.end == 43))
        case _ => false
      }
    }

    val reads = artificial_reads.map(RichADAMRecord(_)).collect()
    reads.foreach(
      read => {
        val read_rdd : RDD[RichADAMRecord] = sc.makeRDD(Seq(read), 1)
        val targets = RealignmentTargetFinder(read_rdd)
        if (read.getStart < 105) {
          assert(targets != null)
          assert(targets.size === 1) // the later read mates do not have indels
          assert(targets.head.getIndelSet().head.readRange.start === read.getStart)
          assert(targets.head.getIndelSet().head.readRange.end === read.end.get - 1)
          assert(check_indel(targets.head, read))
        }
      }
    )
  }

  sparkTest("creating indel targets for mason reads") {
    object IndelRangeOrdering extends Ordering[IndelRange] {
      def compare(x: IndelRange, y: IndelRange): Int = x.getIndelRange().start compare y.getIndelRange().start
    }

    val targets_collected : Array[IndelRealignmentTarget] = RealignmentTargetFinder(mason_reads.map(RichADAMRecord(_))).toArray
    assert(targets_collected.size > 0)

    val only_indels =  targets_collected.filter(_.getIndelSet() != Set.empty)
    // the first read has no indels
    // the second read has a one-base deletion and a one-base insertion
    assert(only_indels(0).getIndelSet().size === 2)
    val tmp1 = new TreeSet()(IndelRangeOrdering).union(only_indels(0).getIndelSet())
    assert(tmp1.toIndexedSeq(0).getIndelRange().start == 702289  && tmp1.toIndexedSeq(0).getIndelRange().end == 702289)
    assert(tmp1.toIndexedSeq(1).getIndelRange().start == 702323 && tmp1.toIndexedSeq(1).getIndelRange().end == 702323)
    // the third read has a one base deletion
    assert(only_indels(1).getIndelSet().size === 1)
    assert(only_indels(1).getIndelSet().head.getIndelRange().start == 807755 && only_indels(1).getIndelSet().head.getIndelRange().end == 807755)
    // read 7 has a single 4 bp deletion
    assert(only_indels(5).getIndelSet().size === 1)
    assert(only_indels(5).getIndelSet().head.getIndelRange().length === 4)
    assert(only_indels(5).getIndelSet().head.getIndelRange().start == 869644 && only_indels(5).getIndelSet().head.getIndelRange().end == 869647)
  }
}
