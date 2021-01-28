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
package org.bdgenomics.adam.cli

import org.bdgenomics.adam.ds.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite
import org.bdgenomics.formats.avro.Alignment
import org.bdgenomics.utils.cli.Args4j

class ViewSuite extends ADAMFunSuite {

  val inputSamPath = testFile("flag-values.sam")

  var reads: Array[Alignment] = null
  var readsCount = 0

  sparkBefore("initialize 'reads' Array from flag-values.sam") {

    val transform =
      new TransformAlignments(
        Args4j[TransformAlignmentsArgs](
          Array(
            inputSamPath,
            "unused_output_path"
          )
        )
      )

    val Alignments = sc.loadBam(inputSamPath)

    reads = transform.apply(Alignments).rdd.collect()
    readsCount = reads.length
  }

  def runView(matchAllBits: Int = -1,
              mismatchAllBits: Int = -1,
              matchSomeBits: Int = -1,
              mismatchSomeBits: Int = -1)(expected: Int): Unit =
    runView(
      if (matchAllBits >= 0) Some(matchAllBits) else None,
      if (mismatchAllBits >= 0) Some(mismatchAllBits) else None,
      if (matchSomeBits >= 0) Some(matchSomeBits) else None,
      if (mismatchSomeBits >= 0) Some(mismatchSomeBits) else None,
      expected
    )

  def runView(matchAllBitsOpt: Option[Int],
              mismatchAllBitsOpt: Option[Int],
              matchSomeBitsOpt: Option[Int],
              mismatchSomeBitsOpt: Option[Int],
              expected: Int): Unit = {

    val args: Array[String] =
      (
        matchAllBitsOpt.toList.flatMap("-f %d".format(_).split(" ")) ++
        mismatchAllBitsOpt.toList.flatMap("-F %d".format(_).split(" ")) ++
        matchSomeBitsOpt.toList.flatMap("-g %d".format(_).split(" ")) ++
        mismatchSomeBitsOpt.toList.flatMap("-G %d".format(_).split(" ")) :+
        "unused_input_path"
      ).toArray

    assert(
      new View(
        Args4j[ViewArgs](
          args
        )
      ).applyFilters(sc.parallelize(reads))
        .count() == expected
    )
  }

  sparkTest("-f 0 -F 0 is a no-op") {
    runView(0, 0)(readsCount)
  }

  sparkTest("no -f or -F args is a no-op") {
    runView()(readsCount)
  }

  sparkTest("-f 4: only unmapped reads") {
    /**
     * Of the 4096 (2^12) possible values of the 12 flag-field bits:
     *
     *   - half (2048) have the 0x4 (unmapped read) flag set, which we are filtering around in this test case.
     *   - only 1/8 of those (256) have the "reverse strand" (0x10), "secondary alignment" (0x100), and "supplementary
     *     alignment" (0x800) flags all unset (HTSJDK doesn't allow them to be set if 0x4 is set, because that wouldn't
     *     make sense).
     *   - half (128) have the "paired" flag (0x1) set and half (128) don't:
     *     1. of the 128 that do, 3/4ths (96) of them have at least one of {"first in template" (0x40), "second in
     *        template" (0x80)} set, and are therefore valid.
     *     2. of those that don't, 1/32nd (4) of them (those with none of {"proper pair" (0x2), "mate unmapped" (0x8),
     *        "mate reversed" (0x20), "first in template" (0x40), "last in template" (0x80)} set) are valid.
     *     - 96 and 4 from 1. and 2. above make for 100 total.
     */
    runView(4)(100)
  }

  sparkTest("-F 4: only mapped reads") {
    // 500 here is the rest of the 700-read input file that was not counted in the 200 above.
    runView(mismatchAllBits = 4)(600)
  }

  /**
   *   - 1/4 (1024) have 0x4 set and 0x8 *not* set.
   *   - 1/8 of those (128) have none of {0x10, 0x100, 0x800}, which is necessary on unmapped reads
   *     1. of 64 "paired" reads, 3/4ths (48) have 0x40 or 0x80
   *     2. of 64 "unpaired" reads, 0x8 has already been excluded, but so must be {0x2, 0x20, 0x40, 0x80}, leaving only
   *        1/16th the reads, or 4.
   *   - total: 52 (48 reads from 1., 4 from 2.).
   */
  sparkTest("-f 4 -F 8: unmapped reads with mapped mates") {
    runView(4, 8)(52)
  }

  // 48 reads is the complement of the last case (52) from among the 100 from the "unmapped" case.
  sparkTest("-f 12: unmapped reads with unmapped mates") {
    runView(12)(48)
  }

  /**
   *   - 2048 have "proper pair" set
   *     - 1/4 of these are no good because they don't have 0x40 or 0x80 set (one of which is required if 0x1 is set),
   *       so only 1536 remain.
   *       - 1/2 of those, or 768, have 0x4 set
   *         - only 1/8 (96) of these are good, because 0x10, 0x100, and 0x800 can't be set with 0x4
   *       - another 384 have 0x8 and not 0x4.
   *         - leaving out 1/4 (96) that have 0x800 and not 0x100 set, we have 288.
   *       - total: 384
   *   - 2048 possible reads don't have "proper pair" set.
   *     - none of them can have 0x2, 0x8, 0x20, 0x40, 0x80 set, so really only 1/32 of 2048, or 64, are possible.
   *     - 32 of those have 0x4 set.
   *     - none of {0x10, 0x100, 0x800} can be set, so only 1/8 of 32, or 4, remain.
   *
   *   - 384 + 4 = 388
   */
  sparkTest("-g 12: reads that are unmapped or whose mate is unmapped") {
    runView(matchSomeBits = 12)(388)
  }

  // Complement of the last test case.
  sparkTest("-F 12: mapped reads with mapped mates") {
    runView(mismatchAllBits = 12)(312)
  }

  /**
   *   - 2048 have "proper pair" set
   *     - 3/4ths (1536) have 0x40 or 0x80 set.
   *       - 1/2 (768) have 0x4 set.
   *         - 1/8 (96) don't have {0x10, 0x100, 0x800}
   *       - 1/2 (768) don't have 0x4 set.
   *         - 3/4 (576) satisfy 0x800 => 0x100
   *         - 1/2 (288) have 0x20 set.
   *     - total: 96 + 288 = 384.
   *   - 2048 don't have "proper pair" set
   *     - 1/32nd (64) don't have {0x2, 0x8, 0x20, 0x40, 0x80}.
   *     - 1/2 (32) have 0x4 set.
   *       - 1/8 (4) don't have {0x10, 0x100, 0x800}
   *   - 384 + 4 = 388
   *
   * Or:
   *
   *   - 2048 have 0x4 set.
   *     - 1/8th (256) have none of {0x10, 0x100, 0x800}
   *       - half (128) have 0x1.
   *         - 3/4ths (96) have 0x40 or 0x80.
   *       - half (128) don't have 0x1.
   *         - 1/32nd (4) don't have {0x2, 0x8, 0x20, 0x40, 0x80}
   *   - 2048 don't have 0x4 set.
   *     - 3/4ths (1536) satisfy 0x800 => 0x100
   *       - 1/2 (768) have 0x1.
   *         - 3/4ths (576) have 0x40 or 0x80.
   *         - 1/2 (288) have 0x20.
   *       - 1/2 (768) don't have 0x1.
   *         - none have 0x20, so none are valid.
   *   - 96 + 4 + 288 = 388.
   */
  sparkTest("-g 36: unmapped reads or reads with mate on negative strand") {
    runView(matchSomeBits = 36)(388)
  }

  sparkTest("-F 36: unmapped reads or reads with mate on negative strand") {
    // Complement to the last test case (700 - 388 == 312)
    runView(mismatchAllBits = 36)(312)
  }
}
