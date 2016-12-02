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

import org.bdgenomics.adam.util.ADAMFunSuite

class Reads2FragmentsSuite extends ADAMFunSuite {
  sparkTest("unordered sam to ordered sam round trip") {
    val inputPath = copyResource("unordered.sam")
    val fragmentsPath = tmpFile("fragments.adam")
    val readsPath = tmpFile("reads.adam")
    val actualPath = tmpFile("fragments.sam")
    val expectedPath = copyResource("ordered.sam")
    Reads2Fragments(Array(inputPath, fragmentsPath)).run(sc)
    Fragments2Reads(Array(fragmentsPath, readsPath)).run(sc)
    Transform(Array("-single", "-sort_reads", "-sort_lexicographically",
      readsPath, actualPath)).run(sc)
    checkFiles(expectedPath, actualPath)
  }
}
