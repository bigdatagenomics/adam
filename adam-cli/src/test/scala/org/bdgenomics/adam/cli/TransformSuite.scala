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

import java.nio.file.Files

import org.bdgenomics.adam.util.ADAMFunSuite

class TransformSuite extends ADAMFunSuite {
  sparkTest("unordered sam to unordered sam") {
    val inputPath = resourcePath("unordered.sam")
    val actualPath = tmpFile("unordered.sam")
    val expectedPath = inputPath
    Transform(Array("-force_load_bam", "-single", inputPath, actualPath)).run(sc)
    checkFiles(expectedPath, actualPath)
  }

  sparkTest("unordered sam to ordered sam") {
    val inputPath = resourcePath("unordered.sam")
    val actualPath = tmpFile("ordered.sam")
    val expectedPath = resourcePath("ordered.sam")
    Transform(Array("-force_load_bam", "-single", "-sort_reads", inputPath, actualPath)).run(sc)
    checkFiles(expectedPath, actualPath)
  }

  ignore("unordered sam, to adam, to sam") {
    val inputPath = resourcePath("unordered.sam")
    val intermediateAdamPath = tmpFile("unordered.adam")
    val actualPath = tmpFile("unordered.sam")
    val expectedPath = inputPath
    Transform(Array("-force_load_bam", inputPath, intermediateAdamPath)).run(sc)
    Transform(Array("-single", intermediateAdamPath, actualPath)).run(sc)
    checkFiles(expectedPath, actualPath)
  }

  ignore("unordered sam, to adam, to ordered sam") {
    val inputPath = resourcePath("unordered.sam")
    val intermediateAdamPath = tmpFile("unordered.adam")
    val actualPath = tmpFile("ordered.sam")
    val expectedPath = resourcePath("ordered.sam")
    Transform(Array("-force_load_bam", inputPath, intermediateAdamPath)).run(sc)
    Transform(Array("-single", "-sort_reads", intermediateAdamPath, actualPath)).run(sc)
    checkFiles(expectedPath, actualPath)
  }
}
