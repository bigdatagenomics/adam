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

class TransformFragmentsSuite extends ADAMFunSuite {

  sparkTest("load queryname sorted sam and save as parquet") {
    val input = copyResource("sample1.queryname.sam")
    val output = tmpLocation("fragments.adam")
    TransformFragments(Array(input, output)).run(sc)
    assert(sc.loadFragments(output).rdd.count === 3)
  }

  sparkTest("cannot sort if not saving as sam") {
    val loc = tmpLocation()
    intercept[IllegalArgumentException] {
      TransformFragments(Array(loc, loc, "-sort_reads")).run(sc)
    }
    intercept[IllegalArgumentException] {
      TransformFragments(Array(loc, loc, "-sort_lexicographically")).run(sc)
    }
  }

  sparkTest("load reads as sam and save them sorted") {
    val input = copyResource("unsorted.sam")
    val output = tmpLocation(".sam")
    TransformFragments(Array(input, output,
      "-load_as_reads", "-save_as_reads",
      "-single",
      "-sort_reads")).run(sc)
    val actualSorted = copyResource("sorted.sam")
    checkFiles(actualSorted, output)
  }

  sparkTest("bin quality scores on reads") {
    val inputPath = copyResource("bqsr1.sam")
    val finalPath = tmpFile("binned.adam")
    TransformFragments(Array(inputPath, finalPath,
      "-save_as_reads",
      "-bin_quality_scores", "0,20,10;20,40,30;40,60,50")).run(sc)
    val qualityScoreCounts = sc.loadAlignments(finalPath)
      .rdd
      .flatMap(_.getQuality)
      .map(s => s.toInt - 33)
      .countByValue

    assert(qualityScoreCounts(30) === 92899)
    assert(qualityScoreCounts(10) === 7101)
  }
}
