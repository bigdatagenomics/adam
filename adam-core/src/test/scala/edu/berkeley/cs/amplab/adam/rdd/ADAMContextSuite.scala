/**
 * Copyright 2013 Genome Bridge LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.cs.amplab.adam.rdd

import parquet.filter.UnboundRecordFilter
import org.apache.spark.rdd.RDD
import edu.berkeley.cs.amplab.adam.avro.ADAMRecord
import edu.berkeley.cs.amplab.adam.util.SparkFunSuite
import edu.berkeley.cs.amplab.adam.rdd.ADAMContext._
import edu.berkeley.cs.amplab.adam.util.PhredUtils._
import java.io.File
import org.apache.hadoop.fs.Path
import java.util.UUID

class ADAMContextSuite extends SparkFunSuite {

  sparkTest("sc.adamLoad should not fail on unmapped reads") {
    val readsFilepath = ClassLoader.getSystemClassLoader.getResource("unmapped.sam").getFile

    // Convert the reads12.sam file into a parquet file
    val bamReads: RDD[ADAMRecord] = sc.adamLoad(readsFilepath)
    assert(bamReads.count === 200)
  }

  sparkTest("can read a small .SAM file") {
    val path = ClassLoader.getSystemClassLoader.getResource("small.sam").getFile
    val reads: RDD[ADAMRecord] = sc.adamLoad[ADAMRecord, UnboundRecordFilter](path)
    assert(reads.count() === 20)
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

  sparkTest("loadADAMFromPaths can load simple RDDs that have just been saved") {
    val a0 = ADAMRecord.newBuilder()
      .setRecordGroupName("group0")
      .setReadName("read0")
      .setReferenceId(0)
      .setReferenceName("abc")
      .setReferenceUrl("http://abc")
      .setReferenceLength(1000000)
      .setStart(100)
      .setPrimaryAlignment(true)
      .setReadPaired(false)
      .setReadMapped(true)
      .build()
    val a1 = ADAMRecord.newBuilder(a0)
      .setReadName("read1")
      .setStart(200)
      .build()

    val saved = sc.parallelize(Seq(a0, a1))
    val loc = tempLocation()
    val path = new Path(loc)

    saved.adamSave(loc)
    try {
      val loaded = sc.loadADAMFromPaths(Seq(path))

      assert(loaded.count() === saved.count())
    } catch {
      case (e: Exception) => {
        println(e)
        throw e
      }
    }
  }

  sparkTest("findFiles correctly finds a nested set of directories") {

    /**
     * Create the following directory structure, in the temp file location:
     *
     * .
     * ├── parent-dir/
     *     ├── subDir1/
     *     |   ├── match1/
     *     |   └── match2/
     *     └── subDir2/
     *     |   ├── match3/
     *     |   └── nomatch4/
     *     ├── match5/
     *     └── nomatch6/
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

  /*
  Little helper function -- because apparently createTempFile creates an actual file, not
  just a name?  Whereas, this returns the name of something that could be mkdir'ed, in the
  same location as createTempFile() uses, so therefore the returned path from this method
  should be suitable for adamSave().
   */
  def tempLocation(suffix: String = "adam"): String = {
    val tempFile = File.createTempFile("ADAMContextSuite", "")
    val tempDir = tempFile.getParentFile
    new File(tempDir, tempFile.getName + suffix).getAbsolutePath
  }

}

