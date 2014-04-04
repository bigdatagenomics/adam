/*
 * Copyright (c) 2013. Regents of the University of California
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
package org.bdgenomics.adam.rdd

import org.bdgenomics.adam.util.SparkFunSuite
import org.bdgenomics.adam.avro.ADAMRecord
import org.bdgenomics.adam.rdd.ADAMContext._
import java.util.UUID

class MarkDuplicatesSuite extends SparkFunSuite {

  def createUnmappedRead() = {
    ADAMRecord.newBuilder().setReadMapped(false).build()
  }

  def createMappedRead(referenceId: Int, position: Long,
    readName: String = UUID.randomUUID().toString, avgPhredScore: Int = 20,
    numClippedBases: Int = 0, isPrimaryAlignment: Boolean = true,
    isNegativeStrand: Boolean = false) = {
    assert(avgPhredScore >= 10 && avgPhredScore <= 50)
    val qual = (for (i <- 0 until 100) yield (avgPhredScore + 33).toChar).toString()
    val cigar = if (numClippedBases > 0) "%dS%dM".format(numClippedBases, 100 - numClippedBases) else "100M"
    ADAMRecord.newBuilder()
      .setReferenceId(referenceId)
      .setStart(position)
      .setQual(qual)
      .setCigar(cigar)
      .setReadMapped(true)
      .setPrimaryAlignment(isPrimaryAlignment)
      .setReadName(readName)
      .setRecordGroupName("machine foo")
      .setRecordGroupId(0)
      .setRecordGroupLibrary("library bar")
      .setReferenceName("reference%d".format(referenceId))
      .setDuplicateRead(false)
      .setReadNegativeStrand(isNegativeStrand)
      .build()
  }

  def createPair(firstReferenceId: Int, firstPosition: Long,
    secondReferenceId: Int, secondPosition: Long,
    readName: String = UUID.randomUUID().toString,
    avgPhredScore: Int = 20): Seq[ADAMRecord] = {
    val firstOfPair = createMappedRead(firstReferenceId, firstPosition,
      readName = readName, avgPhredScore = avgPhredScore)
    firstOfPair.setFirstOfPair(true)
    firstOfPair.setMateMapped(true)
    firstOfPair.setMateReferenceId(secondReferenceId)
    firstOfPair.setMateAlignmentStart(secondPosition)
    firstOfPair.setReadPaired(true)
    val secondOfPair = createMappedRead(secondReferenceId, secondPosition,
      readName = readName, avgPhredScore = avgPhredScore, isNegativeStrand = true)
    secondOfPair.setSecondOfPair(true)
    secondOfPair.setMateMapped(true)
    secondOfPair.setMateReferenceId(firstReferenceId)
    secondOfPair.setMateAlignmentStart(firstPosition)
    secondOfPair.setReadPaired(true)
    Seq(firstOfPair, secondOfPair)
  }

  private def markDuplicates(reads: ADAMRecord*) = {
    sc.parallelize(reads).adamMarkDuplicates().collect()
  }

  sparkTest("single read") {
    val read = createMappedRead(0, 100)
    val marked = markDuplicates(read)
    // Can't have duplicates with a single read, should return the read unchanged.
    assert(marked(0) == read)
  }

  sparkTest("reads at different positions") {
    val read1 = createMappedRead(0, 42)
    val read2 = createMappedRead(0, 43)
    val marked = markDuplicates(read1, read2)
    // Reads shouldn't be modified
    assert(marked.contains(read1) && marked.contains(read2))
  }

  sparkTest("reads at the same position") {
    val poorReads = for (i <- 0 until 10) yield {
      createMappedRead(1, 42, avgPhredScore = 20, readName = "poor%d".format(i))
    }
    val bestRead = createMappedRead(1, 42, avgPhredScore = 30, readName = "best")
    val marked = markDuplicates(List(bestRead) ++ poorReads: _*)
    val (dups, nonDup) = marked.partition(p => p.getDuplicateRead)
    assert(nonDup.size == 1 && nonDup(0) == bestRead)
    assert(dups.forall(p => p.getReadName.startsWith("poor")))
  }

  sparkTest("reads at the same position with clipping") {
    val poorClippedReads = for (i <- 0 until 5) yield {
      createMappedRead(1, 44, numClippedBases = 2, avgPhredScore = 20, readName = "poorClipped%d".format(i))
    }
    val poorUnclippedReads = for (i <- 0 until 5) yield {
      createMappedRead(1, 42, avgPhredScore = 20, readName = "poorUnclipped%d".format(i))
    }
    val bestRead = createMappedRead(1, 42, avgPhredScore = 30, readName = "best")
    val marked = markDuplicates(List(bestRead) ++ poorClippedReads ++ poorUnclippedReads: _*)
    val (dups, nonDup) = marked.partition(p => p.getDuplicateRead)
    assert(nonDup.size == 1 && nonDup(0) == bestRead)
    assert(dups.forall(p => p.getReadName.startsWith("poor")))
  }

  sparkTest("reads on reverse strand") {
    val poorReads = for (i <- 0 until 7) yield {
      createMappedRead(10, 42, isNegativeStrand = true, avgPhredScore = 20, readName = "poor%d".format(i))
    }
    val bestRead = createMappedRead(10, 42, isNegativeStrand = true, avgPhredScore = 30, readName = "best")
    val marked = markDuplicates(List(bestRead) ++ poorReads: _*)
    val (dups, nonDup) = marked.partition(p => p.getDuplicateRead)
    assert(nonDup.size == 1 && nonDup(0) == bestRead)
    assert(dups.forall(p => p.getReadName.startsWith("poor")))
  }

  sparkTest("unmapped reads") {
    val unmappedReads = for (i <- 0 until 10) yield createUnmappedRead()
    val marked = markDuplicates(unmappedReads: _*)
    assert(marked.size == unmappedReads.size)
    // Unmapped reads should never be marked duplicates
    assert(marked.forall(p => !p.getDuplicateRead))
  }

  sparkTest("read pairs") {
    val poorPairs = for (
      i <- 0 until 10;
      read <- createPair(0, 10, 0, 210, avgPhredScore = 20, readName = "poor%d".format(i))
    ) yield read
    val bestPair = createPair(0, 10, 0, 210, avgPhredScore = 30, readName = "best")
    val marked = markDuplicates(bestPair ++ poorPairs: _*)
    val (dups, nonDups) = marked.partition(_.getDuplicateRead)
    assert(nonDups.size == 2 && nonDups.forall(p => p.getReadName.toString == "best"))
    assert(dups.forall(p => p.getReadName.startsWith("poor")))
  }

  sparkTest("read pairs with fragments") {
    val fragments = for (i <- 0 until 10) yield {
      createMappedRead(2, 33, avgPhredScore = 40, readName = "fragment%d".format(i))
    }
    // Even though the phred score is lower, pairs always score higher than fragments
    val pairs = createPair(2, 33, 2, 200, avgPhredScore = 20, readName = "pair")
    val marked = markDuplicates(fragments ++ pairs: _*)
    val (dups, nonDups) = marked.partition(_.getDuplicateRead)
    assert(nonDups.size == 2 && nonDups.forall(p => p.getReadName.toString == "pair"))
    assert(dups.size == 10 && dups.forall(p => p.getReadName.startsWith("fragment")))
  }

  test("quality scores") {
    // The ascii value 53 is equal to a phred score of 20
    val qual = 53.toChar.toString * 100
    val record = ADAMRecord.newBuilder().setQual(qual).build()
    assert(MarkDuplicates.score(record) == 2000)
  }

}
