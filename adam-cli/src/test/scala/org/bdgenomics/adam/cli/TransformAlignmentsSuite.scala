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

import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite

class TransformAlignmentsSuite extends ADAMFunSuite {
  sparkTest("unordered sam to unordered sam") {
    val inputPath = copyResource("unordered.sam")
    val actualPath = tmpFile("unordered.sam")
    val expectedPath = inputPath
    TransformAlignments(Array("-single", "-disable_pg", inputPath, actualPath)).run(sc)
    checkFiles(expectedPath, actualPath)
  }

  sparkTest("unordered sam to ordered sam") {
    val inputPath = copyResource("unordered.sam")
    val actualPath = tmpFile("ordered.sam")
    val expectedPath = copyResource("ordered.sam")
    TransformAlignments(Array("-single", "-disable_pg", "-sort_reads", "-sort_lexicographically", inputPath, actualPath)).run(sc)
    checkFiles(expectedPath, actualPath)
  }

  sparkTest("unordered sam, to adam, to sam") {
    val inputPath = copyResource("unordered.sam")
    val intermediateAdamPath = tmpFile("unordered.adam")
    val actualPath = tmpFile("unordered.sam")
    val expectedPath = inputPath
    TransformAlignments(Array("-disable_pg", inputPath, intermediateAdamPath)).run(sc)
    TransformAlignments(Array("-single", "-disable_pg", intermediateAdamPath, actualPath)).run(sc)
    checkFiles(expectedPath, actualPath)
  }

  sparkTest("unordered sam, to adam, to ordered sam") {
    val inputPath = copyResource("unordered.sam")
    val intermediateAdamPath = tmpFile("unordered.adam")
    val actualPath = tmpFile("ordered.sam")
    val expectedPath = copyResource("ordered.sam")
    TransformAlignments(Array("-disable_pg", inputPath, intermediateAdamPath)).run(sc)
    TransformAlignments(Array("-single", "-disable_pg", "-sort_reads", "-sort_lexicographically", intermediateAdamPath, actualPath)).run(sc)
    checkFiles(expectedPath, actualPath)
  }

  sparkTest("put quality scores into bins") {
    val inputPath = copyResource("bqsr1.sam")
    val finalPath = tmpFile("binned.adam")
    TransformAlignments(Array(inputPath, finalPath, "-bin_quality_scores", "0,20,10;20,40,30;40,60,50")).run(sc)
    val qualityScoreCounts = sc.loadAlignments(finalPath)
      .rdd
      .flatMap(_.getQuality)
      .map(s => s.toInt - 33)
      .countByValue

    assert(qualityScoreCounts(30) === 92899)
    assert(qualityScoreCounts(10) === 7101)
  }

  sparkTest("run region predicate") {
    // alas, copy resource does not work here...
    val inputPath = testFile("sorted.bam")
    val outputPath1 = tmpFile("predicate.1.adam")
    val outputPath2 = tmpFile("predicate.2.adam")
    val outputPath3 = tmpFile("predicate.3.adam")
    TransformAlignments(Array(inputPath, outputPath1,
      "-region_predicate", "1:0-200,chr2:0-1000",
      "-force_load_bam")).run(sc)
    TransformAlignments(Array(outputPath1, outputPath2,
      "-region_predicate", "chr2:0-1000",
      "-force_load_parquet")).run(sc)
    TransformAlignments(Array(outputPath2, outputPath3,
      "-region_predicate", "chr2:0-400")).run(sc)

    assert(sc.loadAlignments(outputPath1).rdd.count === 3)
    assert(sc.loadAlignments(outputPath2).rdd.count === 2)
    assert(sc.loadAlignments(outputPath3).rdd.count === 1)
  }
}
