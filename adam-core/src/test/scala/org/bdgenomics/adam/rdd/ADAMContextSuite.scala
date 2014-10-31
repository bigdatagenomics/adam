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
package org.bdgenomics.adam.rdd

import java.io.File
import java.util.UUID

import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.predicates.HighQualityReadPredicate
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.PhredUtils._
import org.bdgenomics.adam.util.SparkFunSuite
import org.bdgenomics.formats.avro.AlignmentRecord

class ADAMContextSuite extends SparkFunSuite {

  sparkTest("sc.adamLoad should not fail on unmapped reads") {
    val readsFilepath = ClassLoader.getSystemClassLoader.getResource("unmapped.sam").getFile

    // Convert the reads12.sam file into a parquet file
    val bamReads: RDD[AlignmentRecord] = sc.adamLoad(readsFilepath)
    assert(bamReads.count === 200)
  }

  sparkTest("can read a small .SAM file") {
    val path = ClassLoader.getSystemClassLoader.getResource("small.sam").getFile
    val reads: RDD[AlignmentRecord] = sc.adamLoad(path)
    assert(reads.count() === 20)
  }

  sparkTest("can filter a .SAM file based on quality") {
    val path = ClassLoader.getSystemClassLoader.getResource("small.sam").getFile
    val reads: RDD[AlignmentRecord] = sc.adamLoad(path, predicate = Some(classOf[HighQualityReadPredicate]))
    assert(reads.count() === 18)
  }

  test("Can convert to phred") {
    assert(successProbabilityToPhred(0.9) === 10)
    assert(successProbabilityToPhred(0.99999) === 50)
  }

  test("Can convert from phred") {
    // result is floating point, so apply tolerance
    assert(phredToSuccessProbability(10) > 0.89 && phredToSuccessProbability(10) < 0.91)
    assert(phredToSuccessProbability(50) > 0.99998 && phredToSuccessProbability(50) < 0.999999)
  }

  sparkTest("findFiles correctly finds a nested set of directories") {

    /**
     * Create the following directory structure, in the temp file location:
     *
     * .
     * |__ parent-dir/
     *     |__ subDir1/
     *     |   |__ match1/
     *     |   |__ match2/
     *     |__ subDir2/
     *     |   |__ match3/
     *     |   |__ nomatch4/
     *     |__ match5/
     *     |__ nomatch6/
     */

    val tempDir = File.createTempFile("ADAMContextSuite", "").getParentFile

    def createDir(dir: File, name: String): File = {
      val dirFile = new File(dir, name)
      dirFile.mkdir()
      dirFile
    }

    val parentName: String = "parent-" + UUID.randomUUID().toString
    val parentDir: File = createDir(tempDir, parentName)
    val subDir1: File = createDir(parentDir, "subDir1")
    val subDir2: File = createDir(parentDir, "subDir2")
    val match1: File = createDir(subDir1, "match1")
    val match2: File = createDir(subDir1, "match2")
    val match3: File = createDir(subDir2, "match3")
    val nomatch4: File = createDir(subDir2, "nomatch4")
    val match5: File = createDir(parentDir, "match5")
    val nomatch6: File = createDir(parentDir, "nomatch6")

    /**
     * Now, run findFiles() on the parentDir, and make sure we find match{1, 2, 3, 5} and _do not_
     * find nomatch{4, 6}
     */

    val paths = sc.findFiles(new Path(parentDir.getAbsolutePath), "^match.*")

    assert(paths.size === 4)

    val pathNames = paths.map(_.getName)
    assert(pathNames.contains("match1"))
    assert(pathNames.contains("match2"))
    assert(pathNames.contains("match3"))
    assert(pathNames.contains("match5"))
  }
}

