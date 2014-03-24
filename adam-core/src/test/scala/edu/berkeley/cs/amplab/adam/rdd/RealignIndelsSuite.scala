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

package edu.berkeley.cs.amplab.adam.rdd

import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import edu.berkeley.cs.amplab.adam.rdd.AdamContext._
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import parquet.filter.UnboundRecordFilter
import edu.berkeley.cs.amplab.adam.algorithms.realignmenttarget.RealignmentTargetFinder
import edu.berkeley.cs.amplab.adam.algorithms.realignmenttarget.IndelRealignmentTarget
import edu.berkeley.cs.amplab.adam.models.Consensus
import edu.berkeley.cs.amplab.adam.rich.RichADAMRecord
import org.scalatest.exceptions.TestFailedException

class RealignIndelsSuite extends SparkFunSuite {

  def mason_reads: RDD[ADAMRecord] = {
    val path = ClassLoader.getSystemClassLoader.getResource("small_realignment_targets.sam").getFile
    sc.adamLoad[ADAMRecord, UnboundRecordFilter](path)
  }

  def artificial_reads: RDD[ADAMRecord] = {
    val path = ClassLoader.getSystemClassLoader.getResource("artificial.sam").getFile
    sc.adamLoad[ADAMRecord, UnboundRecordFilter](path)
  }

  def artificial_realigned_reads: RDD[ADAMRecord] = {
    artificial_reads
      .adamRealignIndels()
      .adamSortReadsByReferencePosition()
  }

  def gatk_artificial_realigned_reads: RDD[ADAMRecord] = {
    val path = ClassLoader.getSystemClassLoader.getResource("artificial.realigned.sam").getFile
    sc.adamLoad[ADAMRecord, UnboundRecordFilter](path)
  }

  sparkTest("checking mapping to targets for artificial reads") {
    val targets = RealignmentTargetFinder(artificial_reads.map(RichADAMRecord(_)))
    assert(targets.size === 1)
    val rr = artificial_reads.map(RichADAMRecord(_))
    val readsMappedToTarget = RealignIndels.mapTargets(rr, targets).map(kv => {
        val (t, r) = kv

        (t, r.map(r => r.record))
      }).collect()
      
    assert(readsMappedToTarget.size === 2)

    readsMappedToTarget.forall {
      case (target : Option[IndelRealignmentTarget], reads : Seq[ADAMRecord]) => reads.forall {
        read => {
          if(read.getStart <= 25) {
            var result : Boolean = (target.get.readRange.start.toLong <= read.getStart.toLong)
            result && (target.get.readRange.end >= read.end.get)
          } else {
            target.isEmpty
          }
      }
      }
    }
  }

  sparkTest("checking alternative consensus for artificial reads") {
    var consensus = List[Consensus]()

    // similar to realignTargetGroup() in RealignIndels
    artificial_reads.collect().toList.foreach(r => {
      if (r.mdTag.get.hasMismatches) {
        consensus = Consensus.generateAlternateConsensus(r.getSequence, r.getStart, r.samtoolsCigar) match {
          case Some(o) => o :: consensus
          case None => consensus
        }
      }
    }
    )
    consensus = consensus.distinct
    assert(consensus.length > 0)
    // Note: it seems that consensus ranges are non-inclusive
    assert(consensus.get(0).index.start === 34)
    assert(consensus.get(0).index.end === 44)
    assert(consensus.get(0).consensus === "")
    assert(consensus.get(1).index.start === 54)
    assert(consensus.get(1).index.end === 64)
    assert(consensus.get(1).consensus === "")
    // TODO: add check with insertions, how about SNPs
  }

  sparkTest("checking extraction of reference from reads") {
    def checkReference(readReference : Tuple3[String, Long, Long]) {
      // the first three lines of artificial.fasta
      val refStr = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGGGGGGGGGGAAAAAAAAAAGGGGGGGGGGAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
      val startIndex = Math.min(readReference._2.toInt, 120)
      val stopIndex = Math.min(readReference._3.toInt, 180)
      assert(readReference._1 === refStr.substring(startIndex, stopIndex))
    }
    
    val targets = RealignmentTargetFinder(artificial_reads.map(RichADAMRecord(_)))
    val rr = artificial_reads.map(RichADAMRecord(_))
    val readsMappedToTarget : Array[Tuple2[IndelRealignmentTarget, Seq[ADAMRecord]]] = RealignIndels.mapTargets(rr, targets)
      .filter(_._1.isDefined)
      .map(kv => {
        val (t, r) = kv

        (t.get, r.map(r => r.record))
      }).collect()

    val readReference = readsMappedToTarget.map {
      case (target, reads) => {
        if (!target.isEmpty) {
          val referenceFromReads : (String, Long, Long) = RealignIndels.getReferenceFromReads(reads.map(r => new RichADAMRecord(r)))
          assert(referenceFromReads._2 == -1 || referenceFromReads._1.length > 0)
          checkReference(referenceFromReads)
        }
      }
      case _ => assert(false)
    }
    assert(readReference != null)
  }

  sparkTest("checking search for consensus list for artitifical reads") {
    val (realignedReads, readsToClean, consensus) = (new RealignIndels()).findConsensus(artificial_reads.map(new RichADAMRecord(_))
                                                                                                        .collect()
                                                                                                        .toSeq)

    assert(consensus.length === 2)
  }

  sparkTest("checking realigned reads for artificial input") {
    val artificial_realigned_reads_collected = artificial_realigned_reads.collect()
    val gatk_artificial_realigned_reads_collected = gatk_artificial_realigned_reads.collect()

    assert(artificial_realigned_reads_collected.size === gatk_artificial_realigned_reads_collected.size)

    Console.println("checking relative ordering of realigned reads")
    val artificial_read4 = artificial_realigned_reads_collected.filter(_.getReadName == "read4")
    val gatk_read4 = gatk_artificial_realigned_reads_collected.filter(_.getReadName == "read4")
    val result = artificial_read4.zip(gatk_read4)

    assert(result.forall(
      pair => pair._1.getReadName == pair._2.getReadName
    ))
    assert(result.forall(
      pair => pair._1.getStart == pair._2.getStart
    ))
    assert(result.forall(
      pair => pair._1.getCigar == pair._2.getCigar
    ))
    assert(result.forall(
      pair => pair._1.getMapq == pair._2.getMapq
    ))
  }

  sparkTest("test mismatch quality scoring") {
    val ri = new RealignIndels()
    val read = "AAAAAAAA"
    val ref =  "AAGGGGAA"
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
    val read = artificial_reads.first
    
    assert(ri.sumMismatchQuality(read) === 800)
  }

}
