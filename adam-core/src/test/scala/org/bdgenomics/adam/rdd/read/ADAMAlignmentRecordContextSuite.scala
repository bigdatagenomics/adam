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
package org.bdgenomics.adam.rdd.read

import java.io.File
import org.apache.hadoop.fs.Path
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.ADAMAlignmentRecordContext._
import org.bdgenomics.adam.util.SparkFunSuite
import org.bdgenomics.formats.avro.{ AlignmentRecord, Contig }

class ADAMAlignmentRecordContextSuite extends SparkFunSuite {

  sparkTest("loadADAMFromPaths can load simple RDDs that have just been saved") {
    val contig = Contig.newBuilder
      .setContigName("abc")
      .setContigLength(1000000)
      .setReferenceURL("http://abc")
      .build

    val a0 = AlignmentRecord.newBuilder()
      .setRecordGroupName("group0")
      .setReadName("read0")
      .setContig(contig)
      .setStart(100)
      .setPrimaryAlignment(true)
      .setReadPaired(false)
      .setReadMapped(true)
      .build()
    val a1 = AlignmentRecord.newBuilder(a0)
      .setReadName("read1")
      .setStart(200)
      .build()

    val saved = sc.parallelize(Seq(a0, a1))
    val loc = tempLocation()
    val path = new Path(loc)

    saved.adamSave(loc)
    try {
      val loaded = new ADAMAlignmentRecordContext(sc).loadADAMFromPaths(Seq(path))

      assert(loaded.count() === saved.count())
    } catch {
      case (e: Exception) =>
        println(e)
        throw e
    }
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

