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

  (1 to 4) foreach { testNumber =>
    val inputName = "interleaved_fastq_sample%d.fq".format(testNumber)
    val path = ClassLoader.getSystemClassLoader.getResource(inputName).getFile

    sparkTest("import records from interleaved FASTQ: %d".format(testNumber)) {

      val reads = ADAMAlignmentRecordContext.adamInterleavedFastqLoad(sc, path)
      if (testNumber == 1) {
        assert(reads.count === 6)
        assert(reads.filter(_.getReadPaired).count === 6)
        assert(reads.filter(_.getFirstOfPair).count === 3)
        assert(reads.filter(_.getSecondOfPair).count === 3)
      } else {
        assert(reads.count === 4)
        assert(reads.filter(_.getReadPaired).count === 4)
        assert(reads.filter(_.getFirstOfPair).count === 2)
        assert(reads.filter(_.getSecondOfPair).count === 2)
      }

      assert(reads.collect.forall(_.getSequence.toString.length === 250))
      assert(reads.collect.forall(_.getQual.toString.length === 250))
    }
  }

  (1 to 4) foreach { testNumber =>
    val inputName = "interleaved_fastq_sample%d.fq".format(testNumber)
    val path = ClassLoader.getSystemClassLoader.getResource(inputName).getFile

    sparkTest("import records from single ended FASTQ: %d".format(testNumber)) {

      val reads = ADAMAlignmentRecordContext.adamUnpairedFastqLoad(sc, path)
      if (testNumber == 1) {
        assert(reads.count === 6)
        assert(reads.filter(_.getReadPaired).count === 0)
      } else if (testNumber == 4) {
        assert(reads.count === 4)
        assert(reads.filter(_.getReadPaired).count === 0)
      } else {
        assert(reads.count === 5)
        assert(reads.filter(_.getReadPaired).count === 0)
      }

      assert(reads.collect.forall(_.getSequence.toString.length === 250))
      assert(reads.collect.forall(_.getQual.toString.length === 250))
    }
  }

  sparkTest("read properly paired fastq") {
    val path1 = ClassLoader.getSystemClassLoader.getResource("proper_pairs_1.fq").getFile
    val path2 = ClassLoader.getSystemClassLoader.getResource("proper_pairs_2.fq").getFile
    val reads = new ADAMAlignmentRecordContext(sc).adamFastqLoad(path1, path2)

    assert(reads.count === 6)
    assert(reads.filter(_.getReadPaired).count === 6)
    assert(reads.filter(_.getProperPair).count === 6)
    assert(reads.filter(_.getFirstOfPair).count === 3)
    assert(reads.filter(_.getSecondOfPair).count === 3)
  }

  sparkTest("read properly paired fastq and force pair fixing") {
    val path1 = ClassLoader.getSystemClassLoader.getResource("proper_pairs_1.fq").getFile
    val path2 = ClassLoader.getSystemClassLoader.getResource("proper_pairs_2.fq").getFile
    val reads = new ADAMAlignmentRecordContext(sc).adamFastqLoad(path1, path2, true)

    assert(reads.count === 6)
    assert(reads.filter(_.getReadPaired).count === 6)
    assert(reads.filter(_.getProperPair).count === 6)
    assert(reads.filter(_.getFirstOfPair).count === 3)
    assert(reads.filter(_.getSecondOfPair).count === 3)
  }

  sparkTest("read improperly paired fastq by noting size mismatch") {
    val path1 = ClassLoader.getSystemClassLoader.getResource("improper_pairs_1.fq").getFile
    val path2 = ClassLoader.getSystemClassLoader.getResource("improper_pairs_2.fq").getFile
    val reads = new ADAMAlignmentRecordContext(sc).adamFastqLoad(path1, path2)

    assert(reads.count === 4)
    assert(reads.filter(_.getReadPaired).count === 4)
    assert(reads.filter(_.getProperPair).count === 4)
    assert(reads.filter(_.getFirstOfPair).count === 2)
    assert(reads.filter(_.getSecondOfPair).count === 2)
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

