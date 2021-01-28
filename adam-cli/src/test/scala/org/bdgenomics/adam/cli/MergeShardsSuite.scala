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

import org.seqdoop.hadoop_bam.CRAMInputFormat
import org.bdgenomics.adam.ds.ADAMContext._
import org.bdgenomics.adam.util.ADAMFunSuite

class MergeShardsSuite extends ADAMFunSuite {
  sparkTest("merge shards from unordered sam to unordered sam") {
    val inputPath = copyResource("unordered.sam")
    val actualPath = tmpFile("unordered.sam")
    val expectedPath = inputPath
    TransformAlignments(Array("-single", "-defer_merging", "-disable_pg",
      inputPath, actualPath)).run(sc)
    MergeShards(Array(actualPath + "_tail", actualPath,
      "-header_path", actualPath + "_head")).run(sc)
    checkFiles(expectedPath, actualPath)
  }

  sparkTest("unordered sam to ordered sam") {
    val inputPath = copyResource("unordered.sam")
    val actualPath = tmpFile("ordered.sam")
    val expectedPath = copyResource("ordered.sam")
    TransformAlignments(Array("-single",
      "-disable_pg",
      "-sort_by_reference_position",
      "-defer_merging",
      inputPath, actualPath)).run(sc)
    MergeShards(Array(actualPath + "_tail", actualPath,
      "-header_path", actualPath + "_head")).run(sc)
    checkFiles(expectedPath, actualPath)
  }

  sparkTest("merge sharded bam") {
    val inputPath = copyResource("unordered.sam")
    val actualPath = tmpFile("unordered.bam")
    TransformAlignments(Array("-single",
      "-defer_merging",
      inputPath, actualPath)).run(sc)
    MergeShards(Array(actualPath + "_tail", actualPath,
      "-header_path", actualPath + "_head",
      "-write_empty_GZIP_at_eof")).run(sc)
    assert(sc.loadAlignments(actualPath).rdd.count === 8L)
  }

  sparkTest("merge sharded cram") {
    val inputPath = copyResource("artificial.cram")
    val referencePath = resourceUrl("artificial.fa").toString
    sc.hadoopConfiguration.set(CRAMInputFormat.REFERENCE_SOURCE_PATH_PROPERTY,
      referencePath)

    val actualPath = tmpFile("artificial.cram")
    TransformAlignments(Array("-single",
      "-sort_by_reference_position",
      "-defer_merging",
      inputPath, actualPath)).run(sc)
    MergeShards(Array(actualPath + "_tail", actualPath,
      "-header_path", actualPath + "_head",
      "-write_cram_eof")).run(sc)
    assert(sc.loadAlignments(actualPath).rdd.count === 10L)
  }
}
