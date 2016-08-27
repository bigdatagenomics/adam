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
package org.bdgenomics.adam.rdd.read.realignment

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.algorithms.consensus.Consensus
import org.bdgenomics.adam.models.ReferencePosition
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rich.RichAlignmentRecord
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.{ AlignmentRecord, Contig }

class RealignIndelsSuite extends ADAMFunSuite {

  def artificialReadsRdd: AlignmentRecordRDD = {
    val path = testFile("artificial.sam")
    sc.loadAlignments(path)
  }

  def artificialReads: RDD[AlignmentRecord] = {
    artificialReadsRdd.rdd
  }

  def artificialRealignedReads: RDD[AlignmentRecord] = {
    artificialReadsRdd
      .realignIndels()
      .sortReadsByReferencePosition()
      .rdd
  }

  def gatkArtificialRealignedReads: RDD[AlignmentRecord] = {
    val path = testFile("artificial.realigned.sam")
    sc.loadAlignments(path).rdd
  }

  sparkTest("checking mapping to targets for artificial reads") {
    val targets = RealignmentTargetFinder(artificialReads.map(RichAlignmentRecord(_)))
    assert(targets.size === 1)
    val rr = artificialReads.map(RichAlignmentRecord(_))
    val readsMappedToTarget = RealignIndels.mapTargets(rr, targets).map(kv => {
      val (t, r) = kv

      (t, r.map(r => r.record))
    }).collect()

    assert(readsMappedToTarget.size === 2)

    readsMappedToTarget.forall {
      case (target: Option[IndelRealignmentTarget], reads: Seq[AlignmentRecord]) => reads.forall {
        read =>
          {
            if (read.getStart <= 25) {
              val result = target.get.readRange.start <= read.getStart.toLong
              result && (target.get.readRange.end >= read.getEnd)
            } else {
              target.isEmpty
            }
          }
      }
      case _ => false
    }
  }

  sparkTest("checking alternative consensus for artificial reads") {
    var consensus = List[Consensus]()

    // similar to realignTargetGroup() in RealignIndels
    artificialReads.collect().toList.foreach(r => {
      if (r.mdTag.get.hasMismatches) {
        consensus = Consensus.generateAlternateConsensus(r.getSequence, ReferencePosition("0", r.getStart), r.samtoolsCigar) match {
          case Some(o) => o :: consensus
          case None    => consensus
        }
      }
    })
    consensus = consensus.distinct
    assert(consensus.length > 0)
    // Note: it seems that consensus ranges are non-inclusive
    assert(consensus(0).index.start === 34)
    assert(consensus(0).index.end === 45)
    assert(consensus(0).consensus === "")
    assert(consensus(1).index.start === 54)
    assert(consensus(1).index.end === 65)
    assert(consensus(1).consensus === "")
    // TODO: add check with insertions, how about SNPs
  }

  sparkTest("checking extraction of reference from reads") {
    def checkReference(readReference: (String, Long, Long)) {
      // the first three lines of artificial.fasta
      val refStr = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGGGGGGGGGGAAAAAAAAAAGGGGGGGGGGAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
      val startIndex = Math.min(readReference._2.toInt, 120)
      val stopIndex = Math.min(readReference._3.toInt, 180)
      assert(readReference._1 === refStr.substring(startIndex, stopIndex))
    }

    val targets = RealignmentTargetFinder(artificialReads.map(RichAlignmentRecord(_)))
    val rr = artificialReads.map(RichAlignmentRecord(_))
    val readsMappedToTarget: Array[(IndelRealignmentTarget, Iterable[AlignmentRecord])] = RealignIndels.mapTargets(rr, targets)
      .filter(_._1.isDefined)
      .map(kv => {
        val (t, r) = kv

        (t.get, r.map(r => r.record))
      }).collect()

    val readReference = readsMappedToTarget.map {
      case (target, reads) =>
        if (!target.isEmpty) {
          val referenceFromReads: (String, Long, Long) = RealignIndels.getReferenceFromReads(reads.map(r => new RichAlignmentRecord(r)).toSeq)
          assert(referenceFromReads._2 == -1 || referenceFromReads._1.length > 0)
          checkReference(referenceFromReads)
        }
      case _ => throw new AssertionError("Mapping should contain target and reads")
    }
    assert(readReference != null)
  }

  sparkTest("checking realigned reads for artificial input") {
    val artificialRealignedReads_collected = artificialRealignedReads.collect()
    val gatkArtificialRealignedReads_collected = gatkArtificialRealignedReads.collect()

    assert(artificialRealignedReads_collected.size === gatkArtificialRealignedReads_collected.size)

    val artificialRead4 = artificialRealignedReads_collected.filter(_.getReadName == "read4")
    val gatkRead4 = gatkArtificialRealignedReads_collected.filter(_.getReadName == "read4")
    val result = artificialRead4.zip(gatkRead4)

    assert(result.forall(
      pair => pair._1.getReadName == pair._2.getReadName))
    assert(result.forall(
      pair => pair._1.getStart == pair._2.getStart))
    assert(result.forall(
      pair => pair._1.getCigar == pair._2.getCigar))
    assert(result.forall(
      pair => pair._1.getMapq == pair._2.getMapq))
  }

  sparkTest("test mismatch quality scoring") {
    val ri = new RealignIndels()
    val read = "AAAAAAAA"
    val ref = "AAGGGGAA"
    val qScores = Seq(40, 40, 40, 40, 40, 40, 40, 40)

    assert(ri.sumMismatchQualityIgnoreCigar(read, ref, qScores) === 160)
  }

  sparkTest("test mismatch quality scoring for no mismatches") {
    val ri = new RealignIndels()
    val read = "AAAAAAAA"
    val qScores = Seq(40, 40, 40, 40, 40, 40, 40, 40)

    assert(ri.sumMismatchQualityIgnoreCigar(read, read, qScores) === 0)
  }

  sparkTest("test mismatch quality scoring after unpacking read") {
    val ri = new RealignIndels()
    val read = artificialReads.first()

    assert(ri.sumMismatchQuality(read) === 800)
  }

  test("we shouldn't try to realign a region with no target") {
    val ctg = Contig.newBuilder()
      .setContigName("chr1")
      .build()
    val reads = Seq(AlignmentRecord.newBuilder()
      .setContigName(ctg.getContigName)
      .setStart(1L)
      .setEnd(4L)
      .setSequence("AAA")
      .setQual("...")
      .setCigar("3M")
      .setReadMapped(true)
      .setMismatchingPositions("3")
      .build(), AlignmentRecord.newBuilder()
      .setContigName(ctg.getContigName)
      .setStart(9L)
      .setEnd(12L)
      .setSequence("AAA")
      .setQual("...")
      .setCigar("3M")
      .setReadMapped(true)
      .setMismatchingPositions("3")
      .build()).map(RichAlignmentRecord(_))
      .toIterable
    val ri = new RealignIndels()

    // this should be a NOP
    assert(ri.realignTargetGroup(None.asInstanceOf[Option[IndelRealignmentTarget]],
      reads).size === 2)
  }

  sparkTest("we shouldn't try to realign reads with no indel evidence") {
    val ctg = Contig.newBuilder()
      .setContigName("chr1")
      .build()
    val reads = sc.parallelize(Seq(AlignmentRecord.newBuilder()
      .setContigName(ctg.getContigName)
      .setStart(1L)
      .setEnd(4L)
      .setSequence("AAA")
      .setQual("...")
      .setCigar("3M")
      .setReadMapped(true)
      .setMismatchingPositions("3")
      .build(), AlignmentRecord.newBuilder()
      .setContigName(ctg.getContigName)
      .setStart(10L)
      .setEnd(13L)
      .setSequence("AAA")
      .setQual("...")
      .setCigar("3M")
      .setReadMapped(true)
      .setMismatchingPositions("3")
      .build(), AlignmentRecord.newBuilder()
      .setContigName(ctg.getContigName)
      .setStart(4L)
      .setEnd(7L)
      .setSequence("AAA")
      .setQual("...")
      .setCigar("3M")
      .setReadMapped(true)
      .setMismatchingPositions("3")
      .build(), AlignmentRecord.newBuilder()
      .setContigName(ctg.getContigName)
      .setStart(7L)
      .setEnd(10L)
      .setSequence("AAA")
      .setQual("...")
      .setCigar("3M")
      .setReadMapped(true)
      .setMismatchingPositions("3")
      .build()))

    // this should be a NOP
    assert(RealignIndels(reads).count === 4)
  }

  sparkTest("test OP and OC tags") {
    artificialRealignedReads.collect().foreach(realn => {
      val readName = realn.getReadName()
      val op = realn.getOldPosition()
      val oc = realn.getOldCigar()

      Option(op).filter(_ >= 0).foreach(oPos => {
        val s = artificialReads.collect().filter(x => (x.getReadName() == readName))
        assert(s.filter(x => (x.getStart() == oPos)).length > 0)
        assert(s.filter(x => (x.getCigar() == oc)).length > 0)
      })
    })
  }
}
