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
package org.bdgenomics.adam.ds.read.realignment

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.algorithms.consensus.{
  Consensus,
  ConsensusGenerator
}
import org.bdgenomics.adam.converters.DefaultHeaderLines
import org.bdgenomics.adam.models.{
  ReadGroupDictionary,
  ReferencePosition,
  ReferenceRegion,
  SequenceDictionary,
  SequenceRecord
}
import org.bdgenomics.adam.ds.ADAMContext._
import org.bdgenomics.adam.ds.read.AlignmentDataset
import org.bdgenomics.adam.ds.variant.VariantDataset
import org.bdgenomics.adam.rich.RichAlignment
import org.bdgenomics.adam.util.{ ADAMFunSuite, ReferenceFile }
import org.bdgenomics.formats.avro.{ Alignment, Variant, Reference }

class RealignIndelsSuite extends ADAMFunSuite {

  def artificialReadsRdd: AlignmentDataset = {
    val path = testFile("artificial.sam")
    sc.loadAlignments(path)
  }

  def artificialReads: RDD[Alignment] = {
    artificialReadsRdd.rdd
  }

  def artificialRealignedReads(cg: ConsensusGenerator = ConsensusGenerator.fromReads,
                               maxCoverage: Int = 3000,
                               optRefFile: Option[ReferenceFile] = None): RDD[Alignment] = {
    artificialReadsRdd
      .realignIndels(consensusModel = cg,
        maxReadsPerTarget = maxCoverage,
        optReferenceFile = optRefFile)
      .sortByReferencePosition()
      .rdd
  }

  def gatkArtificialRealignedReads: RDD[Alignment] = {
    val path = testFile("artificial.realigned.sam")
    sc.loadAlignments(path).rdd
  }

  def makeRead(start: Long, end: Long): RichAlignment = {
    RichAlignment(Alignment.newBuilder()
      .setReferenceName("ctg")
      .setStart(start)
      .setEnd(end)
      .setReadMapped(true)
      .build())
  }

  test("map reads to targets") {
    val targets = Array(
      new IndelRealignmentTarget(None, ReferenceRegion("ctg", 1L, 4L)),
      new IndelRealignmentTarget(None, ReferenceRegion("ctg", 10L, 44L)),
      new IndelRealignmentTarget(None, ReferenceRegion("ctg", 100L, 400L)))

    assert(RealignIndels.mapToTarget(makeRead(0L, 1L), targets, 0, 3) < 0)
    assert(RealignIndels.mapToTarget(makeRead(0L, 2L), targets, 0, 3) === 0)
    assert(RealignIndels.mapToTarget(makeRead(1L, 2L), targets, 0, 3) === 0)
    assert(RealignIndels.mapToTarget(makeRead(3L, 6L), targets, 0, 3) === 0)
    assert(RealignIndels.mapToTarget(makeRead(6L, 8L), targets, 0, 3) < 0)
    assert(RealignIndels.mapToTarget(makeRead(8L, 12L), targets, 0, 3) === 1)
    assert(RealignIndels.mapToTarget(makeRead(10L, 12L), targets, 0, 3) === 1)
    assert(RealignIndels.mapToTarget(makeRead(14L, 36L), targets, 0, 3) === 1)
    assert(RealignIndels.mapToTarget(makeRead(35L, 50L), targets, 0, 3) === 1)
    assert(RealignIndels.mapToTarget(makeRead(45L, 50L), targets, 0, 3) < 0)
    assert(RealignIndels.mapToTarget(makeRead(90L, 100L), targets, 0, 3) < 0)
    assert(RealignIndels.mapToTarget(makeRead(90L, 101L), targets, 0, 3) === 2)
    assert(RealignIndels.mapToTarget(makeRead(200L, 300L), targets, 0, 3) === 2)
    assert(RealignIndels.mapToTarget(makeRead(200L, 600L), targets, 0, 3) === 2)
    assert(RealignIndels.mapToTarget(makeRead(700L, 1000L), targets, 0, 3) < 0)
  }

  sparkTest("checking mapping to targets for artificial reads") {
    val targets = RealignmentTargetFinder(artificialReads.map(RichAlignment(_))).toArray
    assert(targets.size === 1)
    val rr = artificialReads.map(RichAlignment(_))
    val readsMappedToTarget = RealignIndels.mapTargets(rr, targets).map(kv => {
      val (t, r) = kv

      (t, r.map(r => r.record))
    }).collect()

    assert(readsMappedToTarget.count(_._1.isDefined) === 1)

    assert(readsMappedToTarget.forall {
      case (target: Option[(Int, IndelRealignmentTarget)], reads: Seq[Alignment]) => reads.forall {
        read =>
          {
            if (read.getStart <= 25) {
              val result = target.get._2.readRange.start <= read.getStart.toLong
              result && (target.get._2.readRange.end >= read.getEnd)
            } else {
              target.isEmpty
            }
          }
      }
      case _ => false
    })
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
      val startIndex = readReference._2.toInt
      val stopIndex = readReference._3.toInt
      assert(readReference._1.length === stopIndex - startIndex)
      assert(readReference._1 === refStr.substring(startIndex, stopIndex))
    }

    val targets = RealignmentTargetFinder(artificialReads.map(RichAlignment(_))).toArray
    val rr = artificialReads.map(RichAlignment(_))
    val readsMappedToTarget: Array[((Int, IndelRealignmentTarget), Iterable[Alignment])] = RealignIndels.mapTargets(rr, targets)
      .filter(_._1.isDefined)
      .map(kv => {
        val (t, r) = kv

        (t.get, r.map(r => r.record))
      }).collect()

    val readReference = readsMappedToTarget.map {
      case ((_, target), reads) =>
        if (!target.isEmpty) {
          val referenceFromReads: (String, Long, Long) = RealignIndels.getReferenceFromReads(reads.map(r => new RichAlignment(r)).toSeq)
          assert(referenceFromReads._2 == -1 || referenceFromReads._1.length > 0)
          checkReference(referenceFromReads)
        }
      case _ => throw new AssertionError("Mapping should contain target and reads")
    }
    assert(readReference != null)
  }

  sparkTest("checking realigned reads for artificial input") {
    val artificialRealignedReadsCollected = artificialRealignedReads()
      .collect()
    val gatkArtificialRealignedReadsCollected = gatkArtificialRealignedReads
      .collect()

    assert(artificialRealignedReadsCollected.size === gatkArtificialRealignedReadsCollected.size)

    val artificialRead4 = artificialRealignedReadsCollected.filter(_.getReadName == "read4")
    val gatkRead4 = gatkArtificialRealignedReadsCollected.filter(_.getReadName == "read4")
    val result = artificialRead4.zip(gatkRead4)

    result.foreach(pair => assert(pair._1.getReadName === pair._2.getReadName))
    result.foreach(pair => assert(pair._1.getStart === pair._2.getStart))
    result.foreach(pair => assert(pair._1.getCigar === pair._2.getCigar))
    result.foreach(pair => assert(pair._1.getMappingQuality === pair._2.getMappingQuality))
  }

  sparkTest("checking realigned reads for artificial input with reference file") {
    val artificialRealignedReadsCollected = artificialRealignedReads(optRefFile = Some(sc.loadReferenceFile(testFile("artificial.fa"), 1000)))
      .collect()
    val gatkArtificialRealignedReadsCollected = gatkArtificialRealignedReads
      .collect()

    assert(artificialRealignedReadsCollected.size === gatkArtificialRealignedReadsCollected.size)

    val artificialRead4 = artificialRealignedReadsCollected.filter(_.getReadName == "read4")
    val gatkRead4 = gatkArtificialRealignedReadsCollected.filter(_.getReadName == "read4")
    val result = artificialRead4.zip(gatkRead4)

    result.foreach(pair => assert(pair._1.getReadName === pair._2.getReadName))
    result.foreach(pair => assert(pair._1.getStart === pair._2.getStart))
    result.foreach(pair => assert(pair._1.getCigar === pair._2.getCigar))
    result.foreach(pair => assert(pair._1.getMappingQuality === pair._2.getMappingQuality))
  }

  sparkTest("checking realigned reads for artificial input using knowns") {
    val indel = Variant.newBuilder()
      .setReferenceName("artificial")
      .setStart(33)
      .setEnd(44)
      .setReferenceAllele("AGGGGGGGGGG")
      .setAlternateAllele("A")
      .build
    val variantRdd = VariantDataset(sc.parallelize(Seq(indel)),
      artificialReadsRdd.references, DefaultHeaderLines.allHeaderLines)
    val knowns = ConsensusGenerator.fromKnownIndels(variantRdd)
    val artificialRealignedReadsCollected = artificialRealignedReads(cg = knowns)
      .collect()
    val gatkArtificialRealignedReadsCollected = gatkArtificialRealignedReads
      .collect()

    assert(artificialRealignedReadsCollected.size === gatkArtificialRealignedReadsCollected.size)

    val artificialRead4 = artificialRealignedReadsCollected.filter(_.getReadName == "read4")
    val gatkRead4 = gatkArtificialRealignedReadsCollected.filter(_.getReadName == "read4")
    val result = artificialRead4.zip(gatkRead4)

    result.foreach(pair => assert(pair._1.getReadName === pair._2.getReadName))
    result.foreach(pair => assert(pair._1.getStart === pair._2.getStart))
    result.foreach(pair => assert(pair._1.getCigar === pair._2.getCigar))
    result.foreach(pair => assert(pair._1.getMappingQuality === pair._2.getMappingQuality))
  }

  sparkTest("checking realigned reads for artificial input using knowns and reads") {
    val indel = Variant.newBuilder()
      .setReferenceName("artificial")
      .setStart(33L)
      .setEnd(44L)
      .setReferenceAllele("AGGGGGGGGGG")
      .setAlternateAllele("A")
      .build
    val variantRdd = VariantDataset(sc.parallelize(Seq(indel)),
      artificialReadsRdd.references, DefaultHeaderLines.allHeaderLines)
    val knowns = ConsensusGenerator.fromKnownIndels(variantRdd)
    val union = ConsensusGenerator.union(knowns, ConsensusGenerator.fromReads)
    val artificialRealignedReadsCollected = artificialRealignedReads(cg = union)
      .collect()
    val gatkArtificialRealignedReadsCollected = gatkArtificialRealignedReads
      .collect()

    assert(artificialRealignedReadsCollected.size === gatkArtificialRealignedReadsCollected.size)

    val artificialRead4 = artificialRealignedReadsCollected.filter(_.getReadName == "read4")
    val gatkRead4 = gatkArtificialRealignedReadsCollected.filter(_.getReadName == "read4")
    val result = artificialRead4.zip(gatkRead4)

    result.foreach(pair => assert(pair._1.getReadName === pair._2.getReadName))
    result.foreach(pair => assert(pair._1.getStart === pair._2.getStart))
    result.foreach(pair => assert(pair._1.getCigar === pair._2.getCigar))
    result.foreach(pair => assert(pair._1.getMappingQuality === pair._2.getMappingQuality))
  }

  sparkTest("skip realigning reads if target is highly covered") {
    val artificialRealignedReadsCollected = artificialRealignedReads(maxCoverage = 0)
      .collect()
    val reads = artificialReads
    val result = artificialRealignedReadsCollected.zip(reads.collect())

    result.foreach(pair => assert(pair._1.getReadName === pair._2.getReadName))
    result.foreach(pair => assert(pair._1.getStart === pair._2.getStart))
    result.foreach(pair => assert(pair._1.getCigar === pair._2.getCigar))
    result.foreach(pair => assert(pair._1.getMappingQuality === pair._2.getMappingQuality))
  }

  sparkTest("skip realignment if target is an insufficient LOD improvement") {
    val path = testFile("NA12878.1_922305.G_GC_hom.sam")
    val reads = sc.loadAlignments(path)
    val realignedReads = reads.realignIndels()
    val result = reads.rdd.filter(!_.getSupplementaryAlignment).keyBy(r => (r.getReadName, r.getReadInFragment))
      .join(realignedReads.rdd.filter(!_.getSupplementaryAlignment).keyBy(r => (r.getReadName, r.getReadInFragment)))
      .map(_._2)
      .collect()

    result.foreach(pair => assert(pair._1 === pair._2))
  }

  sparkTest("realign reads to an insertion") {
    val path = testFile("NA12878.1_922305.G_GC_hom.sam")
    val reads = sc.loadAlignments(path)
    val realignedReads = reads.realignIndels(lodThreshold = 0.0, unclipReads = true)
    val result = reads.rdd.filter(!_.getSupplementaryAlignment).keyBy(r => (r.getReadName, r.getReadInFragment))
      .join(realignedReads.rdd.filter(!_.getSupplementaryAlignment).keyBy(r => (r.getReadName, r.getReadInFragment)))
      .map(_._2)
      .collect()

    val movedReads = result.filter(pair => pair._1 != pair._2)
    assert(movedReads.size === 41)
    val read = movedReads.map(_._2)
      .filter(_.getReadName === "H06HDADXX130110:1:1114:19044:27806")
      .head
    assert(read.getStart === 922057)
    assert(read.getCigar === "248M1I1M")
    assert(read.getMismatchingPositions === "249")
  }

  test("test mismatch quality scoring") {
    val ri = new RealignIndels(sc)
    val read = "AAAAAAAA"
    val ref = "AAGGGGAA"
    val qScores = Seq(40, 40, 40, 40, 40, 40, 40, 40)

    assert(ri.sumMismatchQualityIgnoreCigar(read, ref, qScores, Int.MaxValue, 0) === 160)
  }

  test("test mismatch quality scoring for no mismatches") {
    val ri = new RealignIndels(sc)
    val read = "AAAAAAAA"
    val qScores = Seq(40, 40, 40, 40, 40, 40, 40, 40)

    assert(ri.sumMismatchQualityIgnoreCigar(read, read, qScores, Int.MaxValue, 0) === 0)
  }

  test("test mismatch quality scoring for offset") {
    val ri = new RealignIndels(sc)
    val read = "AAAAAAAA"
    val ref = "G%s".format(read)
    val qScores = Seq(40, 40, 40, 40, 40, 40, 40, 40)

    assert(ri.sumMismatchQualityIgnoreCigar(read, ref, qScores, Int.MaxValue, 1) === 0)
  }

  test("test mismatch quality scoring with early exit") {
    val ri = new RealignIndels(sc)
    val read = "AAAAAAAA"
    val ref = "AAGGGGAA"
    val qScores = Seq(40, 40, 40, 40, 40, 40, 40, 40)

    assert(ri.sumMismatchQualityIgnoreCigar(read, ref, qScores, 120, 0) === Int.MaxValue)
  }

  sparkTest("test mismatch quality scoring after unpacking read") {
    val ri = new RealignIndels(sc)
    val read = artificialReads.first()

    assert(ri.sumMismatchQuality(read) === 400)
  }

  test("we shouldn't try to realign a region with no target") {
    val reference = Reference.newBuilder()
      .setName("chr1")
      .build()
    val reads = Seq(Alignment.newBuilder()
      .setReferenceName(reference.getName)
      .setStart(1L)
      .setEnd(4L)
      .setSequence("AAA")
      .setQualityScores("...")
      .setCigar("3M")
      .setReadMapped(true)
      .setMismatchingPositions("3")
      .build(), Alignment.newBuilder()
      .setReferenceName(reference.getName)
      .setStart(9L)
      .setEnd(12L)
      .setSequence("AAA")
      .setQualityScores("...")
      .setCigar("3M")
      .setReadMapped(true)
      .setMismatchingPositions("3")
      .build()).map(RichAlignment(_))
      .toIterable
    val ri = new RealignIndels(sc)

    // this should be a NOP
    assert(ri.realignTargetGroup((None.asInstanceOf[Option[(Int, IndelRealignmentTarget)]],
      reads)).size === 2)
  }

  sparkTest("we shouldn't try to realign reads with no indel evidence") {
    val reference = Reference.newBuilder()
      .setName("chr1")
      .build()
    val reads = sc.parallelize(Seq(Alignment.newBuilder()
      .setReferenceName(reference.getName)
      .setStart(1L)
      .setEnd(4L)
      .setSequence("AAA")
      .setQualityScores("...")
      .setCigar("3M")
      .setReadMapped(true)
      .setMismatchingPositions("3")
      .build(), Alignment.newBuilder()
      .setReferenceName(reference.getName)
      .setStart(10L)
      .setEnd(13L)
      .setSequence("AAA")
      .setQualityScores("...")
      .setCigar("3M")
      .setReadMapped(true)
      .setMismatchingPositions("3")
      .build(), Alignment.newBuilder()
      .setReferenceName(reference.getName)
      .setStart(4L)
      .setEnd(7L)
      .setSequence("AAA")
      .setQualityScores("...")
      .setCigar("3M")
      .setReadMapped(true)
      .setMismatchingPositions("3")
      .build(), Alignment.newBuilder()
      .setReferenceName(reference.getName)
      .setStart(7L)
      .setEnd(10L)
      .setSequence("AAA")
      .setQualityScores("...")
      .setCigar("3M")
      .setReadMapped(true)
      .setMismatchingPositions("3")
      .build()))

    // this should be a NOP
    assert(RealignIndels(reads).count === 4)
  }

  sparkTest("test OP and OC tags") {
    artificialRealignedReads()
      .collect()
      .foreach(realn => {
        val readName = realn.getReadName()
        val op = realn.getOriginalStart()
        val oc = realn.getOriginalCigar()

        Option(op).filter(_ >= 0).foreach(oPos => {
          val s = artificialReads.collect().filter(x => (x.getReadName() == readName))
          assert(s.filter(x => (x.getStart() === oPos)).length > 0)
          assert(s.filter(x => (x.getCigar() === oc)).length > 0)
        })
      })
  }

  sparkTest("realign a read with an insertion that goes off the end of the read") {
    // ref: TTACCA___CCACA
    // ins:   ACCAGTTC
    // ext: TTACCA   GT
    // ovl:  TACCA   GTTC
    // ovs:   AGTT   CCAC
    // st:      TT   CCACA
    // sc:     agA   GGTC
    // ec:       A   GGTCt
    val insRead = Alignment.newBuilder
      .setReferenceName("1")
      .setStart(10L)
      .setEnd(15L)
      .setSequence("ACCAGTTC")
      .setQualityScores("........")
      .setCigar("4M3I1M")
      .setMismatchingPositions("5")
      .setReadMapped(true)
      .setMappingQuality(40)
      .build
    val extRead = Alignment.newBuilder
      .setReferenceName("1")
      .setStart(8L)
      .setEnd(16L)
      .setSequence("TTACCAGT")
      .setQualityScores("........")
      .setCigar("8M")
      .setMismatchingPositions("6C0C0")
      .setReadMapped(true)
      .setMappingQuality(40)
      .build
    val ovlRead = Alignment.newBuilder
      .setReferenceName("1")
      .setStart(9L)
      .setEnd(18L)
      .setSequence("TACCAGTTC")
      .setQualityScores("........")
      .setCigar("9M")
      .setMismatchingPositions("5C0C0A1")
      .setReadMapped(true)
      .setMappingQuality(41)
      .build
    val ovsRead = Alignment.newBuilder
      .setReferenceName("1")
      .setStart(10L)
      .setEnd(18L)
      .setSequence("AGTTCCAC")
      .setQualityScores("........")
      .setCigar("8M")
      .setMismatchingPositions("1C0C0A4")
      .setReadMapped(true)
      .setMappingQuality(42)
      .build
    val stRead = Alignment.newBuilder
      .setReferenceName("1")
      .setStart(12L)
      .setEnd(19L)
      .setSequence("TTCCACA")
      .setQualityScores(".......")
      .setCigar("7M")
      .setMismatchingPositions("0C0A5")
      .setReadMapped(true)
      .setMappingQuality(43)
      .build
    val scRead = Alignment.newBuilder
      .setReadName("sc")
      .setReferenceName("1")
      .setStart(13L)
      .setEnd(18L)
      .setSequence("AGAGGTC")
      .setQualityScores(".......")
      .setCigar("2S5M")
      .setMismatchingPositions("1C0C0A1")
      .setReadMapped(true)
      .setMappingQuality(44)
      .build
    val ecRead = Alignment.newBuilder
      .setReferenceName("1")
      .setStart(13L)
      .setEnd(18L)
      .setSequence("AGGTCA")
      .setQualityScores("......")
      .setCigar("5M1S1H")
      .setMismatchingPositions("1C0C0A1")
      .setBasesTrimmedFromEnd(1)
      .setReadMapped(true)
      .setMappingQuality(45)
      .build

    val rdd = AlignmentDataset(sc.parallelize(Seq(insRead,
      extRead,
      ovlRead,
      ovsRead,
      stRead,
      scRead,
      ecRead)),
      new SequenceDictionary(Vector(SequenceRecord("1", 20L))),
      ReadGroupDictionary.empty,
      Seq.empty)
    val realignedReads = rdd.realignIndels(lodThreshold = 0.0)
      .rdd
      .collect
    assert(realignedReads.count(_.getMappingQuality >= 50) === 7)
    val realignedExtRead = realignedReads.filter(_.getMappingQuality == 50).head
    assert(realignedExtRead.getStart === 8L)
    assert(realignedExtRead.getEnd === 14L)
    assert(realignedExtRead.getCigar === "6M2S")
    assert(realignedExtRead.getMismatchingPositions === "6")
    val realignedOvlRead = realignedReads.filter(_.getMappingQuality == 51).head
    assert(realignedOvlRead.getStart === 9L)
    assert(realignedOvlRead.getEnd === 15L)
    assert(realignedOvlRead.getCigar === "5M3I1M")
    assert(realignedOvlRead.getMismatchingPositions === "6")
    val realignedOvsRead = realignedReads.filter(_.getMappingQuality == 52).head
    assert(realignedOvsRead.getStart === 13L)
    assert(realignedOvsRead.getEnd === 18L)
    assert(realignedOvsRead.getCigar === "1M3I4M")
    assert(realignedOvsRead.getMismatchingPositions === "5")
    val realignedStRead = realignedReads.filter(_.getMappingQuality == 53).head
    assert(realignedStRead.getStart === 14L)
    assert(realignedStRead.getEnd === 19L)
    assert(realignedStRead.getCigar === "2S5M")
    assert(realignedStRead.getMismatchingPositions === "5")
    val realignedScRead = realignedReads.filter(_.getMappingQuality == 54).head
    assert(realignedScRead.getStart === 13L)
    assert(realignedScRead.getEnd === 15L)
    assert(realignedScRead.getCigar === "2S1M3I1M")
    assert(realignedScRead.getMismatchingPositions === "2")
    val realignedEcRead = realignedReads.filter(_.getMappingQuality == 55).head
    assert(realignedEcRead.getStart === 13L)
    assert(realignedEcRead.getEnd === 15L)
    assert(realignedEcRead.getCigar === "1M3I1M1S1H")
    assert(realignedEcRead.getMismatchingPositions === "2")
    assert(realignedEcRead.getBasesTrimmedFromEnd === 1)
  }

  sparkTest("if realigning a target doesn't improve the LOD, don't drop reads") {
    val reads = sc.loadAlignments(testFile("NA12878.1_854950_855150.sam"))
    val realignedReads = reads.realignIndels()
    assert(reads.rdd.count === realignedReads.rdd.count)
  }

  test("extract seq/qual from a read with no clipped bases") {
    val read = new RichAlignment(Alignment.newBuilder
      .setSequence("ACTCG")
      .setQualityScores(Seq(20, 30, 40, 50, 60).map(i => (i + 33).toChar).mkString)
      .setCigar("5M")
      .build)

    val (seq, qual) = new RealignIndels(sc).extractSequenceAndQuality(read)

    assert(seq === "ACTCG")
    assert(qual.length === 5)
    assert(qual(0) === 20)
    assert(qual(1) === 30)
    assert(qual(2) === 40)
    assert(qual(3) === 50)
    assert(qual(4) === 60)
  }

  test("extract seq/qual from a read with clipped bases at start") {
    val read = new RichAlignment(Alignment.newBuilder
      .setSequence("ACTCG")
      .setQualityScores(Seq(20, 30, 40, 50, 60).map(i => (i + 33).toChar).mkString)
      .setCigar("2H1S4M")
      .build)

    val (seq, qual) = new RealignIndels(sc).extractSequenceAndQuality(read)

    assert(seq === "CTCG")
    assert(qual.length === 4)
    assert(qual(0) === 30)
    assert(qual(1) === 40)
    assert(qual(2) === 50)
    assert(qual(3) === 60)
  }

  test("extract seq/qual from a read with clipped bases at end") {
    val read = new RichAlignment(Alignment.newBuilder
      .setSequence("ACTCG")
      .setQualityScores(Seq(20, 30, 40, 50, 60).map(i => (i + 33).toChar).mkString)
      .setCigar("3M2S")
      .build)

    val (seq, qual) = new RealignIndels(sc).extractSequenceAndQuality(read)

    assert(seq === "ACT")
    assert(qual.length === 3)
    assert(qual(0) === 20)
    assert(qual(1) === 30)
    assert(qual(2) === 40)
  }

  test("if unclip is selected, don't drop base when extracting from a read with clipped bases") {
    val read = new RichAlignment(Alignment.newBuilder
      .setSequence("ACTCG")
      .setQualityScores(Seq(20, 30, 40, 50, 60).map(i => (i + 33).toChar).mkString)
      .setCigar("1S2M2S")
      .build)

    val (seq, qual) = new RealignIndels(sc,
      unclipReads = true).extractSequenceAndQuality(read)

    assert(seq === "ACTCG")
    assert(qual.length === 5)
    assert(qual(0) === 20)
    assert(qual(1) === 30)
    assert(qual(2) === 40)
    assert(qual(3) === 50)
    assert(qual(4) === 60)
  }

  test("get cigar and coordinates for read that spans indel, no clipped bases") {
    // start: 10
    // ref:   ACACGT__TCCA
    // read:     CGTTTTC
    val (start, end, cigar) = new RealignIndels(sc).cigarAndCoordinates(
      7,
      0, 0, 0, 0,
      10L,
      3,
      Consensus("TT", ReferenceRegion("ctg", 15L, 16L)))

    assert(start === 13L)
    assert(end === 18L)
    assert(cigar.toString === "3M2I2M")
  }

  test("get cigar and coordinates for read that spans deletion, clipped bases at start") {
    // start: 10
    // ref:   ACACGTACTCCACG
    // read:    cCGT__TCCA
    //
    //             ||
    // cons:  ACACGTTCCA
    // read:     CGTTCCA
    // csi:        5
    // cei:         6
    // rsi:      3
    // rei:             10
    val (start, end, cigar) = new RealignIndels(sc).cigarAndCoordinates(
      8,
      1, 0, 0, 0,
      10L,
      3,
      Consensus("", ReferenceRegion("ctg", 16L, 19L)))

    assert(start === 13L)
    assert(end === 22L)
    assert(cigar.toString === "1S3M2D4M")
  }

  test("get cigar and coordinates for read that falls wholly before insertion") {
    // start: 10
    // ref:   ACACGTACTCCACG__AC
    // read:      GTACTCgc
    val (start, end, cigar) = new RealignIndels(sc).cigarAndCoordinates(
      8,
      0, 2, 0, 0,
      10L,
      4,
      Consensus("AA", ReferenceRegion("ctg", 23L, 24L)))
    assert(start === 14L)
    assert(end === 20L)
    assert(cigar.toString === "6M2S")
  }

  test("get cigar and coordinates for read that falls wholly after insertion") {
    // start: 10
    // ref:   ACA__CGTACTCCACGAC
    // read:        GTACTCgc
    val (start, end, cigar) = new RealignIndels(sc).cigarAndCoordinates(
      8,
      0, 2, 0, 0,
      10L,
      6,
      Consensus("AA", ReferenceRegion("ctg", 12L, 13L)))
    assert(start === 14L)
    assert(end === 20L)
    assert(cigar.toString === "6M2S")
  }

  test("get cigar and coordinates for read that falls wholly after deletion") {
    // start: 10
    // ref:   ACACGTACTCCACG
    // con:    _  
    // read:     aGTACTCa
    // 1 bp deletion at 11
    val (start, end, cigar) = new RealignIndels(sc).cigarAndCoordinates(
      8,
      1, 1, 0, 1,
      10L,
      4,
      Consensus("", ReferenceRegion("ctg", 11L, 12L)))
    assert(start === 14L)
    assert(end === 20L)
    assert(cigar.toString === "1S6M1S1H")
  }

  test("get cigar and coordinates for read that partially spans insertion, no clipped bases") {
    // start: 10
    // ref:   ACACGTACTC__CACG
    // read:     aGTACTCA
    // insert:          AT
    val (start, end, cigar) = new RealignIndels(sc).cigarAndCoordinates(
      8,
      1, 0, 0, 0,
      10L,
      4,
      Consensus("AT", ReferenceRegion("ctg", 19L, 20L)))
    assert(start === 14L)
    assert(end === 20L)
    assert(cigar.toString === "1S6M1S")
  }

  test("get cigar and coordinates for read that partially spans insertion, clipped bases at end") {
    // start: 10
    // ref:   ACACGTACTC__CACG
    // read:      GTACTCAg
    // insert:          AT
    val (start, end, cigar) = new RealignIndels(sc).cigarAndCoordinates(
      8,
      0, 1, 1, 0,
      10L,
      4,
      Consensus("AT", ReferenceRegion("ctg", 19L, 20L)))
    assert(start === 14L)
    assert(end === 20L)
    assert(cigar.toString === "1H6M2S")
  }

  test("get cigar and coordinates for read that partially spans insertion, clipped bases both ends") {
    // start: 10
    // ref:   ACACGTACTC__CACG
    // read:            tTCACGgc
    // insert:          AT
    val (start, end, cigar) = new RealignIndels(sc).cigarAndCoordinates(
      8,
      1, 2, 0, 0,
      10L,
      11,
      Consensus("AT", ReferenceRegion("ctg", 19L, 20L)))

    assert(start === 20L)
    assert(end === 24L)
    assert(cigar.toString === "2S4M2S")
  }
}
